// The embedded runtime's gaps filled (ADR 0059): drive leases, signals
// before their waits, limits, wait timeouts, listing and submitting; the
// same behaviour as sdk/go/kairotest. SQLite; needs go (to build
// kairo.wasm from this repository).

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, describe, test } from 'node:test';

import { EmbeddedBackend } from './backend.ts';
import { PostgresStore, SQLiteStore, type Store } from './store.ts';
import { CancelledError, Kairo, StoppedError, TimedOutError, type KairoOptions } from './workflow.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-driving-'));
const hasGo = spawnSync('go', ['version']).status === 0;
// Built by scripts/check.sh (KAIRO_WASM), or here.
const wasm = process.env.KAIRO_WASM || join(dir, 'kairo.wasm');

before(() => {
	if (!hasGo || process.env.KAIRO_WASM) return;
	const r = spawnSync('go', ['build', '-buildmode=c-shared', '-o', wasm, './cmd/kairo-wasm'], {
		cwd: repo,
		stdio: 'inherit',
		env: { ...process.env, GOOS: 'wasip1', GOARCH: 'wasm' },
	});
	assert.equal(r.status, 0, 'building kairo.wasm');
});

after(() => rmSync(dir, { recursive: true, force: true }));

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitUntil(cond: () => boolean | Promise<boolean>): Promise<void> {
	for (let i = 0; i < 400; i++) {
		if (await cond()) return;
		await sleep(25);
	}
	throw new Error('condition not reached');
}

/** A process on the database at path (or on store). */
async function open(path: string, opts: KairoOptions & { leaseMs?: number; store?: Store } = {}): Promise<Kairo> {
	const { leaseMs, store, ...rest } = opts;
	return new Kairo({ backend: await EmbeddedBackend.open({ store: store ?? new SQLiteStore(path), wasm, leaseMs }), ...rest });
}

/** Rows of the database at path, as another process would read them. */
async function query(path: string, sql: string, ...args: unknown[]): Promise<any[]> {
	const sqlite = 'node:sqlite';
	const { DatabaseSync } = (await import(sqlite)) as any;
	const db = new DatabaseSync(path, { readOnly: true });
	try {
		return db.prepare(sql).all(...args);
	} finally {
		db.close();
	}
}

async function children(path: string, id: string): Promise<string[]> {
	return (await query(path, 'SELECT id FROM kairo_run WHERE parent = ? ORDER BY created_at, id', id)).map((r) => r.id);
}

/**
 * A store that stops answering, as the database does for a process that
 * stopped without closing: its leases are not renewed any more.
 */
function dying(store: Store): [Store, { dead: boolean }] {
	const state = { dead: false };
	const cut = new Set(['withRun', 'renewLeases', 'expiredLeases', 'dueTimers', 'endDrive']);
	const proxy = new Proxy(store, {
		get(target, prop) {
			const v = (target as any)[prop];
			if (typeof v !== 'function') return v;
			if (cut.has(prop as string))
				return (...args: unknown[]) => (state.dead ? Promise.reject(new Error('the process stopped')) : v.apply(target, args));
			return v.bind(target);
		},
	});
	return [proxy, state];
}

describe('driving workflows (ADR 0059)', { skip: !hasGo }, () => {
	// A caller that stops waiting does not cancel the workflow: its calls
	// stay, and it goes on when it is run again.
	test('stopping waiting is not cancelling', async () => {
		const path = join(dir, 'stop.db');
		const k = await open(path);
		k.workflow('inner', async (ctx) => ctx.waitFor<string>('go'));
		k.workflow('outer', async (ctx) => ctx.workflow<string>('inner', null));
		await k.start();
		const stop = new AbortController();
		const first = k.run('outer', null, { id: 'stop-1', signal: stop.signal });
		let inner = '';
		await waitUntil(async () => {
			inner = (await children(path, 'stop-1'))[0] ?? '';
			return inner !== '' && (await children(path, inner)).length > 0;
		});
		stop.abort();
		await assert.rejects(first, StoppedError);
		for (const id of ['stop-1', inner]) assert.ok(!['completed', 'failed', 'cancelled'].includes((await k.backend.get(id)).status), id);
		const again = k.run('outer', null, { id: 'stop-1' });
		await k.signal(inner, 'go', 'on');
		assert.equal(await again, 'on');
		await k.close();
	});

	test('cancelling a workflow cancels the workflows it made, and their calls', async () => {
		const path = join(dir, 'tree.db');
		const k = await open(path);
		let started = false;
		let stopped = false;
		k.defineAction('slow', {
			effect: 'unprotected',
			handler: async (_i, ctx) => {
				started = true;
				await new Promise<void>((r) => ctx.signal.addEventListener('abort', () => r()));
				stopped = true;
				return null;
			},
		});
		k.workflow('inner', async (ctx) => ctx.call('slow', null));
		k.workflow('outer', async (ctx) => ctx.workflow('inner', null));
		await k.start();
		const res = k.run('outer', null, { id: 'tree-1' });
		await waitUntil(() => started);
		await k.cancel('tree-1');
		await assert.rejects(res, CancelledError);
		await waitUntil(() => stopped);
		const kids = await children(path, 'tree-1');
		assert.equal(kids.length, 1);
		await waitUntil(async () => (await k.backend.get(kids[0]!)).status === 'cancelled');
		await k.close();
	});

	test('a workflow that throws fails; the process goes on', async () => {
		const k = await open(join(dir, 'bad.db'));
		let runs = 0;
		k.workflow('bad', ((): never => {
			runs++;
			throw new TypeError('boom'); // at once, not in a promise
		}) as any);
		await k.start();
		await assert.rejects(k.run('bad', null, { id: 'bad-1' }), /boom/);
		// Recorded as failed: run again, it fails at once, without running.
		await assert.rejects(k.run('bad', null, { id: 'bad-1' }), /boom/);
		assert.equal(runs, 1);
		await k.close();
	});

	// A resident process that stops without closing: another one takes its
	// workflow up when its drive lease expires, and the real call it made
	// is not made again.
	test('a stopped driver is taken up', async () => {
		const path = join(dir, 'taken.db');
		let writes = 0;
		const start = async (store: Store, hang: boolean) => {
			const k = await open(path, { store, leaseMs: 200 });
			k.defineAction('write', { effect: 'real', handler: async (p: string) => (writes++, `wrote ${p}`) });
			k.workflow('w', async (ctx, p: string) => {
				const w = await ctx.call<string>('write', p);
				if (hang) await new Promise(() => {}); // as a process that stops here
				return w + '!';
			});
			await k.start();
			return k;
		};
		const [store, state] = dying(new SQLiteStore(path));
		const k1 = await start(store, true);
		await k1.submit('w', 'f', { id: 'taken-1' });
		await waitUntil(async () => (await query(path, "SELECT 1 FROM kairo_run WHERE parent = 'taken-1' AND status = 'completed'")).length > 0);
		state.dead = true; // stops: its lease is not renewed

		const k2 = await start(new SQLiteStore(path), false);
		assert.equal(await k2.result('taken-1', { signal: AbortSignal.timeout(10_000) }), 'wrote f!');
		assert.equal(writes, 1, 'the real call was not made again');
		await k2.close();
		await k1.close();
	});

	// While a process drives a workflow, another one that runs it does not
	// drive it too: it waits for the result.
	test('a live driver is not doubled', async () => {
		const path = join(dir, 'one.db');
		const runs: Record<string, number> = { k1: 0, k2: 0 };
		const start = async (name: string) => {
			const k = await open(path, { leaseMs: 200 });
			k.workflow('w', async (ctx) => {
				runs[name]!++;
				return ctx.waitFor<string>('go');
			});
			await k.start();
			return k;
		};
		const k1 = await start('k1');
		const k2 = await start('k2');
		await k1.submit('w', null, { id: 'one-1' });
		await waitUntil(() => runs.k1 === 1);
		const res = k2.run('w', null, { id: 'one-1' });
		await sleep(300); // a lease period and more: k1 renews it
		await k2.signal('one-1', 'go', 'went');
		assert.equal(await res, 'went');
		assert.equal(runs.k2, 0, 'k2 drove it');
		await k1.close();
		await k2.close();
	});

	test('signals sent before their waits are received there, in order', async () => {
		const k = await open(join(dir, 'ahead.db'));
		let open_!: () => void;
		const gate = new Promise<void>((r) => (open_ = r));
		k.defineAction('gate', { effect: 'unprotected', handler: async () => (await gate, null) });
		k.workflow('w', async (ctx) => {
			await ctx.call('gate', null);
			const a = await ctx.waitFor<string>('s');
			const b = await ctx.waitFor<string>('s', { timeout: 60_000 });
			return [a, b];
		});
		await k.start();
		await assert.rejects(k.signal('nope', 's', 'x'), /no workflow/);
		await k.submit('w', null, { id: 'ahead-1' });
		for (const p of ['first', 'second']) await k.signal('ahead-1', 's', p);
		open_();
		assert.deepEqual(await k.result('ahead-1'), ['first', 'second']);
		await k.close();
	});

	test('a wait times out', async () => {
		const k = await open(join(dir, 'late.db'));
		k.workflow('w', async (ctx) => {
			try {
				return await ctx.waitFor<string>('approve', { timeout: 100 });
			} catch (e) {
				if (e instanceof TimedOutError) return 'timed out';
				throw e;
			}
		});
		await k.start();
		assert.equal(await k.run('w', null, { id: 'late-1' }), 'timed out');
		await k.close();
	});

	test("steps over an action's limit wait for a slot; starts keep to its rate", async () => {
		const k = await open(join(dir, 'limits.db'), { concurrency: 3 });
		let running = 0;
		let most = 0;
		const starts: number[] = [];
		k.defineAction('slow', {
			effect: 'unprotected',
			limit: 2,
			handler: async (n: number) => {
				most = Math.max(most, ++running);
				await sleep(20);
				running--;
				return n;
			},
		});
		k.defineAction('paced', {
			effect: 'unprotected',
			rate: 600, // one each 100ms
			handler: async (n: number) => (starts.push(performance.now()), n),
		});
		k.workflow('w', async (ctx) => {
			const slow = [0, 1, 2, 3, 4, 5].map((i) => () => ctx.call<number>('slow', i));
			const paced = [0, 1, 2].map((i) => () => ctx.call<number>('paced', i));
			const out = await ctx.parallel([...slow, ...paced]);
			return out.slice(0, 6).reduce((a, b) => a + b, 0);
		});
		await k.start();
		assert.equal(await k.run('w', null, { id: 'limits-1' }), 15);
		assert.ok(most <= 2, `${most} steps of slow at once`);
		starts.sort((a, b) => a - b);
		for (let i = 1; i < starts.length; i++) assert.ok(starts[i]! - starts[i - 1]! >= 90, `paced started ${starts[i]! - starts[i - 1]!}ms apart`);
		await k.close();
	});

	test('submit, result and list', async () => {
		const k = await open(join(dir, 'list.db'));
		k.workflow('double', async (_ctx, n: number) => 2 * n);
		k.workflow('other', async () => null);
		k.workflow('parent', async (ctx, n: number) => ctx.workflow<number>('double', n));
		await k.start();
		const ids = ['list-1', 'list-2', 'list-3'];
		for (const [i, id] of ids.entries()) assert.equal(await k.submit('double', i, { id, meta: { user: id } }), id);
		await k.run('other', null, { id: 'other-1' });
		assert.equal(await k.run('parent', 5, { id: 'parent-1' }), 10);
		for (const [i, id] of ids.entries()) assert.equal(await k.result(id), 2 * i);
		const all = await k.list({ workflow: 'double' });
		assert.equal(all.length, 3, 'the child workflow of parent-1 is not a root');
		for (const [i, r] of all.entries()) {
			assert.equal(r.run_id, ids[i]);
			assert.equal(r.workflow, 'double');
			assert.deepEqual(r.meta, { user: ids[i] });
			assert.equal(r.status, 'completed');
			assert.ok(r.createdAt! > 0 && r.updatedAt! >= r.createdAt!);
		}
		const page = await k.list({ workflow: 'double', after: all[0]!.run_id, limit: 1 });
		assert.deepEqual(page.map((r) => r.run_id), ['list-2']);
		assert.equal((await k.list()).length, 5);
		assert.equal((await k.list({ status: 'completed', since: new Date(all[0]!.createdAt!) })).length, 5);
		assert.equal((await k.list({ until: all[0]!.createdAt! })).length, 0);
		await k.close();
	});
});

// The same on PostgreSQL (KAIRO_SDK_PG_DSN, and KAIRO_SDK_PG_MODULE where
// "pg" is installed): the drive lease's claim and hand-off, signals sent
// ahead, and listing, in its SQL.
const dsn = process.env.KAIRO_SDK_PG_DSN;
const pgDir = process.env.KAIRO_SDK_PG_MODULE;
describe('driving workflows on PostgreSQL (ADR 0059)', { skip: !hasGo || !dsn || !pgDir }, () => {
	let pool: any;
	const prefix = `kd${process.pid}_`;
	before(() => {
		const pg = createRequire(join(pgDir!, 'x.js'))('pg');
		pool = new pg.Pool({ connectionString: dsn, max: 8 });
	});
	after(async () => {
		for (const t of ['run', 'event', 'timer', 'lease']) await pool.query(`DROP TABLE IF EXISTS ${prefix}${t}`);
		await pool.end();
	});
	const pgStore = () => new PostgresStore(pool, { prefix });

	test('a stopped driver is taken up', async () => {
		let writes = 0;
		const start = async (store: Store, hang: boolean) => {
			const k = await open('', { store, leaseMs: 200 });
			k.defineAction('write', { effect: 'real', handler: async (p: string) => (writes++, `wrote ${p}`) });
			k.workflow('w', async (ctx, p: string) => {
				const w = await ctx.call<string>('write', p);
				if (hang) await new Promise(() => {});
				return w + '!';
			});
			await k.start();
			return k;
		};
		const [store, state] = dying(pgStore());
		const k1 = await start(store, true);
		await k1.submit('w', 'f', { id: 'taken-pg' });
		await waitUntil(() => writes === 1);
		await sleep(100); // the write's outcome recorded
		state.dead = true;
		const k2 = await start(pgStore(), false);
		assert.equal(await k2.result('taken-pg', { signal: AbortSignal.timeout(10_000) }), 'wrote f!');
		assert.equal(writes, 1);
		await k2.close();
		await k1.close();
	});

	test('signals ahead, submit, result and list', async () => {
		const k = await open('', { store: pgStore() });
		let open_ = false;
		k.defineAction('gate', { effect: 'unprotected', handler: async () => (await waitUntil(() => open_), null) });
		k.workflow('ahead', async (ctx) => {
			await ctx.call('gate', null);
			return [await ctx.waitFor<string>('s'), await ctx.waitFor<string>('s', { timeout: 60_000 })];
		});
		await k.start();
		try {
			for (const id of ['pg-1', 'pg-2']) await k.submit('ahead', null, { id, meta: { user: id } });
			for (const id of ['pg-1', 'pg-2']) for (const p of ['first', 'second']) await k.signal(id, 's', `${id} ${p}`);
			open_ = true;
			for (const id of ['pg-1', 'pg-2']) assert.deepEqual(await k.result(id), [`${id} first`, `${id} second`]);
			const all = await k.list({ workflow: 'ahead' });
			assert.deepEqual(all.map((r) => [r.run_id, r.meta]), [['pg-1', { user: 'pg-1' }], ['pg-2', { user: 'pg-2' }]]);
			assert.deepEqual((await k.list({ workflow: 'ahead', after: 'pg-1' })).map((r) => r.run_id), ['pg-2']);
		} finally {
			open_ = true;
			await k.close();
		}
	});
});
