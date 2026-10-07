// The embedded runtime keeps kairo's invariants with a database (ADR 0051).
// Needs go (to build kairo.wasm from this repository).

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, test } from 'node:test';

import { Embedded } from './embedded.ts';
import { PostgresStore, SQLiteStore } from './store.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-embedded-'));
const wasm = join(dir, 'kairo.wasm');
const hasGo = spawnSync('go', ['version']).status === 0;

before(() => {
	if (!hasGo) return;
	const r = spawnSync('go', ['build', '-buildmode=c-shared', '-o', wasm, './cmd/kairo-wasm'], {
		cwd: repo,
		stdio: 'inherit',
		env: { ...process.env, GOOS: 'wasip1', GOARCH: 'wasm' },
	});
	assert.equal(r.status, 0, 'building kairo.wasm');
});

after(() => rmSync(dir, { recursive: true, force: true }));

async function events(path: string, run: string): Promise<Array<{ kind: string; act?: number }>> {
	const sqlite = 'node:sqlite';
	const { DatabaseSync } = (await import(sqlite)) as any;
	const db = new DatabaseSync(path, { readOnly: true });
	try {
		return db.prepare('SELECT body FROM kairo_event WHERE run = ? ORDER BY seq').all(run).map((r: any) => JSON.parse(r.body));
	} finally {
		db.close();
	}
}

test('a real step runs only once its intent is committed; ignored events are not recorded', { skip: !hasGo }, async () => {
	const path = join(dir, 'inv.db');
	const rt = await Embedded.open({ store: new SQLiteStore(path), wasm });
	let seen: Array<{ kind: string; act?: number }> = [];
	let act = 0;
	rt.registerActions([{ action: 'send', effect: 'real' }], async (task) => {
		// What another process would find in the database now.
		act = task.act;
		seen = await events(path, 'r1');
		return { output: { sent: true } };
	});
	rt.registerPlan({ name: 'p', root: { kind: 'seq', nodes: [{ kind: 'step', id: 's', action: 'send' }, { kind: 'wait', id: 'w', signal: 'go' }] } });
	await rt.run('p', null, { runId: 'r1' });
	assert.equal((await rt.run('p', null, { runId: 'r1' })).existing, true, 'a run id is an idempotency key');
	await rt.signal('r1', 'go', 1);
	const r = await rt.wait('r1');
	assert.equal(r.status, 'completed');
	assert.ok(seen.some((e) => e.kind === 'intent' && e.act === act), `the intent was committed before the step ran: ${JSON.stringify(seen)}`);

	const n = (await events(path, 'r1')).length;
	await rt.signal('r1', 'go', 2); // the run is over: ignored
	assert.equal((await events(path, 'r1')).length, n, 'an ignored event is not recorded');
	await rt.close();
});

// A process that stops with steps running leaves their leases to expire.
// Then another process takes them up: the unprotected step runs again, the
// real one is not run twice and its run stops for review. A process that
// comes back under the same name takes up its steps at once.
test('steps of a process that stopped are taken up', { skip: !hasGo }, async () => {
	const path = join(dir, 'lease.db');
	const plans = (rt: Embedded) => {
		rt.registerPlan({ name: 'u', root: { kind: 'step', id: 'think', action: 'think' } });
		rt.registerPlan({ name: 'r', root: { kind: 'step', id: 'send', action: 'send' } });
	};
	const specs = [{ action: 'think', effect: 'unprotected' as const }, { action: 'send', effect: 'real' as const }];

	// A: both steps start, and A stops before either ends.
	const a = await Embedded.open({ store: new SQLiteStore(path), wasm, leaseMs: 200 });
	let started = 0;
	a.registerActions(specs, async (_task, ctx) => {
		started++;
		await new Promise<void>((r) => ctx.signal.addEventListener('abort', () => r()));
		return { output: 'never recorded' };
	});
	plans(a);
	await a.run('u', null, { runId: 'u1' });
	await a.run('r', null, { runId: 'r1' });
	for (let i = 0; i < 200 && started < 2; i++) await new Promise((r) => setTimeout(r, 10));
	assert.equal(started, 2);
	await a.close();

	// B, once the leases expired.
	const b = await Embedded.open({ store: new SQLiteStore(path), wasm, leaseMs: 200 });
	const ran: string[] = [];
	b.registerActions(specs, async (task) => {
		ran.push(task.action);
		return { output: `${task.action} done` };
	});
	plans(b);
	await b.tick(); // not yet expired: nothing to take up
	assert.deepEqual(ran, []);
	await new Promise((r) => setTimeout(r, 250));
	await b.tick();
	assert.equal((await b.wait('u1')).status, 'completed', 'the unprotected step ran again');
	const r1 = await b.wait('r1');
	assert.equal(r1.status, 'blocked', 'the real step stops for review');
	assert.deepEqual(ran, ['think'], 'the real step did not run twice');
	await b.close();

	// C stops with a step running; D comes back as C at once.
	const c = await Embedded.open({ store: new SQLiteStore(path), wasm, leaseMs: 60_000, owner: 'host-1' });
	let cStarted = false;
	c.registerActions(specs, async (_task, ctx) => {
		cStarted = true;
		await new Promise<void>((r) => ctx.signal.addEventListener('abort', () => r()));
		return { output: null };
	});
	plans(c);
	await c.run('u', null, { runId: 'u2' });
	for (let i = 0; i < 200 && !cStarted; i++) await new Promise((r) => setTimeout(r, 10));
	await c.close();
	const d = await Embedded.open({ store: new SQLiteStore(path), wasm, leaseMs: 60_000, owner: 'host-1' });
	d.registerActions(specs, async () => ({ output: 'again' }));
	plans(d);
	await d.tick();
	const u2 = await d.wait('u2');
	assert.equal(u2.status, 'completed');
	assert.equal(u2.output, 'again');
	await d.close();
});

// A process that is alive renews its leases: a step longer than the lease
// is not taken up by another process meanwhile.
test('a running step is not taken up while its process lives', { skip: !hasGo }, async () => {
	const path = join(dir, 'renew.db');
	const specs = [{ action: 'think', effect: 'unprotected' as const }];
	const plan = { name: 'u', root: { kind: 'step', id: 'think', action: 'think' } };
	const a = await Embedded.open({ store: new SQLiteStore(path), wasm, leaseMs: 300 });
	a.registerActions(specs, async () => {
		await new Promise((r) => setTimeout(r, 900));
		return { output: 'a' };
	});
	a.registerPlan(plan);
	const b = await Embedded.open({ store: new SQLiteStore(path), wasm, leaseMs: 300 });
	const ranInB: string[] = [];
	b.registerActions(specs, async (t) => {
		ranInB.push(t.action);
		return { output: 'b' };
	});
	b.registerPlan(plan);
	await a.run('u', null, { runId: 'long1' });
	for (let i = 0; i < 6; i++) {
		await new Promise((r) => setTimeout(r, 150));
		await b.tick();
	}
	const r = await a.wait('long1');
	assert.equal(r.output, 'a');
	assert.deepEqual(ranInB, [], 'b took up a step that a was running');
	await a.close();
	await b.close();
});

// With PostgreSQL (KAIRO_SDK_PG_DSN, and KAIRO_SDK_PG_MODULE where "pg" is
// installed): a run that settles in one process wakes a wait in another.
const dsn = process.env.KAIRO_SDK_PG_DSN;
const pgDir = process.env.KAIRO_SDK_PG_MODULE;
test('a run settled in another process wakes the wait here', { skip: !hasGo || !dsn || !pgDir }, async () => {
	const pg = createRequire(join(pgDir!, 'x.js'))('pg');
	const pool = new pg.Pool({ connectionString: dsn, max: 8 });
	const prefix = `ke${process.pid}_`;
	try {
		const plan = { name: 'p', root: { kind: 'wait', id: 'w', signal: 'go' } };
		const a = await Embedded.open({ store: new PostgresStore(pool, { prefix }), wasm });
		const b = await Embedded.open({ store: new PostgresStore(pool, { prefix }), wasm });
		for (const rt of [a, b]) {
			rt.registerActions([], async () => ({ output: null }));
			rt.registerPlan(plan);
		}
		await a.run('p', null, { runId: 'x1' });
		const waiting = a.wait('x1');
		await b.signal('x1', 'go', 'from b');
		const r = await waiting;
		assert.equal(r.status, 'completed');
		assert.deepEqual(r.output, { timed_out: false, payload: 'from b' });
		await a.close();
		await b.close();
	} finally {
		for (const t of ['run', 'event', 'timer', 'lease']) await pool.query(`DROP TABLE IF EXISTS ${prefix}${t}`);
		await pool.end();
	}
});
