// Finished runs removed by trees, after the time they are kept (ADR 0054).
// The clock of the runtime is moved on; the database is looked at directly.
// Needs go.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, describe, test } from 'node:test';

import { EmbeddedBackend } from './backend.ts';
import { Embedded, keepFinished } from './embedded.ts';
import { ParentUnavailableError, SQLiteStore } from './store.ts';
import { Kairo } from './workflow.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-sdk-retention-'));
// Built by scripts/check.sh (KAIRO_WASM), or here.
const wasm = process.env.KAIRO_WASM || join(dir, 'kairo.wasm');
const hasGo = spawnSync('go', ['version']).status === 0;
const HOUR = 3600 * 1000;

before(() => {
	if (!hasGo) return;
	if (!process.env.KAIRO_WASM) {
		const w = spawnSync('go', ['build', '-buildmode=c-shared', '-o', wasm, './cmd/kairo-wasm'], {
			cwd: repo,
			stdio: 'inherit',
			env: { ...process.env, GOOS: 'wasip1', GOARCH: 'wasm' },
		});
		assert.equal(w.status, 0, 'building kairo.wasm');
	}
});

after(() => rmSync(dir, { recursive: true, force: true }));

/** Rows of each table for runs whose id starts with prefix. */
async function rows(path: string, prefix: string): Promise<{ run: number; event: number; timer: number; lease: number }> {
	const sqlite = 'node:sqlite';
	const { DatabaseSync } = (await import(sqlite)) as any;
	const db = new DatabaseSync(path, { readOnly: true });
	try {
		const n = (t: string, col: string) => Number(db.prepare(`SELECT COUNT(*) AS n FROM kairo_${t} WHERE ${col} LIKE ?`).get(prefix + '%').n);
		return { run: n('run', 'id'), event: n('event', 'run'), timer: n('timer', 'run'), lease: n('lease', 'run') };
	} finally {
		db.close();
	}
}

describe('removing finished runs (ADR 0054)', { skip: !hasGo }, () => {
	test('an external child is replayed by re-submission, not an expired drive lease (ADR 0065)', async () => {
		const path = join(dir, 'external-recovery.db');
		let replays = 0;
		const start = async (owner: string, mode: 'wait' | 'suspend') => {
			const backend = await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm, owner, leaseMs: 50 });
			const k = new Kairo({ backend, mode });
			k.workflow('hold', async (ctx) => ctx.waitFor<string>('parent-go'));
			k.workflow('external-replay', async (ctx) => {
				replays++;
				return ctx.waitFor<string>('child-go');
			});
			await k.start();
			return k;
		};
		let k = await start('external-recovery-1', 'wait');
		await k.submit('hold', null, { id: 'recovery-parent' });
		await k.submit('external-replay', null, { id: 'recovery-child', parent: 'recovery-parent' });
		for (let i = 0; replays !== 1 && i < 100; i++) await new Promise((resolve) => setTimeout(resolve, 5));
		assert.equal(replays, 1);
		await k.close();

		await new Promise((resolve) => setTimeout(resolve, 5));
		k = await start('external-recovery-2', 'suspend');
		await k.tick();
		await new Promise((resolve) => setTimeout(resolve, 20));
		assert.equal(replays, 1, 'the expired drive lease did not replay the external child');
		await k.submit('external-replay', null, { id: 'recovery-child' });
		assert.equal(replays, 2, 're-submission is the new reach');
		assert.equal((await k.backend.get('recovery-child')).parent, 'recovery-parent');
		await k.cancel('recovery-parent');
		await k.close();
	});

	test('an externally started workflow joins an unfinished parent tree (ADR 0065)', async () => {
		const path = join(dir, 'external-parent.db');
		let skew = 0;
		const backend = await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm, now: () => Date.now() + skew, keepFinished: HOUR });
		const k = new Kairo({ backend, mode: 'suspend' });
		k.workflow('hold', async (ctx) => ctx.waitFor<string>('go'));
		k.workflow('quick', async (_ctx, input: string) => input);
		k.workflow('child-wait', async (ctx) => ctx.waitFor<string>('child-go'));
		await k.start();
		await k.submit('hold', null, { id: 'external-parent' });
		await k.submit('quick', 'kept', { id: 'external-child', parent: 'external-parent' });
		assert.equal(await k.result('external-child'), 'kept');
		assert.equal((await backend.get('external-child')).parent, 'external-parent');
		await k.submit('quick', 'ignored', { id: 'external-child' });
		assert.ok(!(await k.list()).some((r) => r.run_id === 'external-child'));

		const sqlite = 'node:sqlite';
		const { DatabaseSync } = (await import(sqlite)) as any;
		const db = new DatabaseSync(path, { readOnly: true });
		const wait = db.prepare("SELECT id FROM kairo_run WHERE parent = ? AND plan = 'kairo.wait/go'").get('external-parent').id as string;
		db.close();
		for (const [id, parent] of [['bad-missing', 'no-parent'], ['bad-call', wait], ['bad-self', 'bad-self']] as const) {
			await assert.rejects(k.submit('quick', 'x', { id, parent }), ParentUnavailableError);
			await assert.rejects(backend.get(id), /no run/);
		}
		await k.submit('quick', 'done', { id: 'finished-parent' });
		await assert.rejects(k.submit('quick', 'x', { id: 'bad-finished', parent: 'finished-parent' }), ParentUnavailableError);
		await k.submit('hold', null, { id: 'other-parent' });
		await assert.rejects(k.submit('quick', 'x', { id: 'external-child', parent: 'other-parent' }), ParentUnavailableError);
		for (let i = 0; i < 5; i++) {
			const parent = `settlement-parent-${i}`;
			const child = `settlement-child-${i}`;
			await k.submit('hold', null, { id: parent });
			const raced = await Promise.allSettled([
				k.signal(parent, 'go', 'done'),
				k.submit('quick', 'x', { id: child, parent }),
			]);
			for (const result of raced) if (result.status === 'rejected') assert.ok(result.reason instanceof ParentUnavailableError, `${result.reason}`);
			assert.equal(await k.run('hold', null, { id: parent }), 'done');
			const row = await backend.get(child).catch(() => undefined);
			assert.ok(!row || ['completed', 'failed', 'cancelled'].includes(row.status), `unfinished child: ${JSON.stringify(row)}`);
		}

		skew = 2 * HOUR;
		await k.tick();
		assert.equal((await backend.get('external-child')).parent, 'external-parent', 'the finished child stays with its unfinished parent');
		await k.signal('external-parent', 'go', 'done');
		assert.equal(await k.run('hold', null, { id: 'external-parent' }), 'done');
		skew = 4 * HOUR;
		await k.tick();
		await assert.rejects(backend.get('external-parent'), /no run/);
		await assert.rejects(backend.get('external-child'), /no run/);

		await k.submit('hold', null, { id: 'cancel-external-parent' });
		await k.submit('child-wait', null, { id: 'cancel-external-child', parent: 'cancel-external-parent' });
		await k.cancel('cancel-external-parent');
		assert.equal((await backend.get('cancel-external-child')).status, 'cancelled');
		await k.close();
	});

	test('a finished workflow goes with its calls once kept long enough; a removed id starts anew', async () => {
		const path = join(dir, 'tree.db');
		let skew = 0;
		const runs = { llm: 0 };
		async function kairo(): Promise<Kairo> {
			const backend = await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm, now: () => Date.now() + skew, keepFinished: HOUR });
			const k = new Kairo({ backend });
			k.defineAction('llm', { effect: 'unprotected', handler: async (q: string) => (runs.llm++, q.toUpperCase()) });
			k.workflow('w', async (ctx) => (await ctx.call('llm', 'a')) + (await ctx.workflow('child', 'b')));
			k.workflow('child', async (ctx, q: string) => ctx.call('llm', q));
			await k.start();
			return k;
		}
		const k = await kairo();
		assert.equal(await k.run('w', null, { id: 'tree-1' }), 'AB');
		const before = await rows(path, 'tree-1');
		assert.equal(before.run, 4, 'the workflow, its call, the child workflow and its call');
		assert.ok(before.event > 0);

		skew = HOUR / 2;
		await k.tick();
		assert.deepEqual(await rows(path, 'tree-1'), before, 'kept for the time given');

		skew = HOUR + 1000;
		await k.tick();
		assert.deepEqual(await rows(path, 'tree-1'), { run: 0, event: 0, timer: 0, lease: 0 }, 'removed as one tree');

		// The id, removed, is a new run: its calls run again.
		assert.equal(await k.run('w', null, { id: 'tree-1' }), 'AB');
		assert.equal(runs.llm, 4);
		await k.close();
	});

	test("a running workflow's finished calls are kept however long it waits", async () => {
		const path = join(dir, 'running.db');
		let skew = 0;
		const runs = { write: 0 };
		async function kairo(): Promise<Kairo> {
			const backend = await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm, now: () => Date.now() + skew, keepFinished: HOUR });
			const k = new Kairo({ backend, mode: 'suspend' });
			k.defineAction('write', { effect: 'real', handler: async (v: string) => (runs.write++, `wrote ${v}`) });
			k.workflow('w', async (ctx) => {
				const w = await ctx.call('write', 'x');
				const ok = await ctx.waitFor<{ by: string }>('approve');
				return `${w} for ${ok.by}`;
			});
			await k.start();
			return k;
		}
		let k = await kairo();
		await assert.rejects(k.run('w', null, { id: 'long-1' }));
		await k.close();

		// Days later: the workflow still waits; its finished call is kept.
		skew = 72 * HOUR;
		k = await kairo();
		await k.tick();
		assert.equal((await rows(path, 'long-1')).run, 3, 'the workflow, its call, its wait');
		await k.signal('long-1', 'approve', { by: 'alice' });
		assert.equal(await k.run('w', null, { id: 'long-1' }), 'wrote x for alice');
		assert.equal(runs.write, 1, 'the real call did not run again');

		skew = 72 * HOUR + HOUR + 1000;
		await k.tick();
		assert.equal((await rows(path, 'long-1')).run, 0);
		await k.close();
	});

	test('a finished root goes with what under it has not finished, timers and leases too', async () => {
		const path = join(dir, 'unfinished.db');
		let skew = 0;
		const store = new SQLiteStore(path);
		const rt = await Embedded.open({ store, wasm, now: () => Date.now() + skew, keepFinished: HOUR });
		rt.registerActions([{ action: 'slow', effect: 'unprotected', timeout: '100h' }], async () => ({ pending: { owner: 'remote:x', leaseMs: 100 * HOUR } }));
		rt.registerPlan({ name: 'wait', root: { kind: 'wait', id: 'w', signal: 'go' } });
		rt.registerPlan({ name: 'step', root: { kind: 'step', id: 's', action: 'slow' } });
		await rt.run('wait', null, { runId: 'u-1' });
		await rt.run('wait', null, { runId: 'u-1/waits', parent: 'u-1' }); // waits for ever
		await rt.run('step', null, { runId: 'u-1/waits/step', parent: 'u-1/waits' }); // a step elsewhere: a timer, a lease
		await rt.idle();
		await rt.signal('u-1', 'go', null);
		assert.equal((await rt.get('u-1')).status, 'completed');
		assert.deepEqual(await rows(path, 'u-1'), { run: 3, event: (await rows(path, 'u-1')).event, timer: 1, lease: 1 });

		skew = HOUR + 1000;
		await rt.tick();
		assert.deepEqual(await rows(path, 'u-1'), { run: 0, event: 0, timer: 0, lease: 0 });
		// An outcome that comes for a removed run does nothing.
		await rt.complete('u-1/waits/step', 1, 1, { output: 'late' });
		assert.equal((await rows(path, 'u-1')).run, 0);
		await rt.close();
	});

	test('runs with no parent go alone; at most the limit per removal; older tables get the column', async () => {
		const path = join(dir, 'alone.db');
		// A table from before ADR 0054 (no parent column).
		const sqlite = 'node:sqlite';
		const { DatabaseSync } = (await import(sqlite)) as any;
		const old = new DatabaseSync(path);
		old.exec(`CREATE TABLE kairo_run (id TEXT PRIMARY KEY, plan TEXT NOT NULL, hash TEXT NOT NULL, state BLOB NOT NULL,
			input TEXT, status TEXT NOT NULL, output TEXT, error TEXT, seq INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`);
		old.close();

		let skew = 0;
		const store = new SQLiteStore(path);
		const rt = await Embedded.open({ store, wasm, now: () => Date.now() + skew, keepFinished: Infinity });
		rt.registerPlan({ name: 'wait', root: { kind: 'wait', id: 'w', signal: 'go' } });
		for (const id of ['a-1', 'a-2', 'a-3']) {
			await rt.run('wait', null, { runId: id });
			await rt.signal(id, 'go', null);
		}
		await rt.run('wait', null, { runId: 'a-running' });
		skew = HOUR;
		await rt.tick();
		assert.equal((await rows(path, 'a-')).run, 4, 'Infinity keeps them');

		const cutoff = Date.now() + skew;
		assert.equal(await store.removeFinished(cutoff, 2), 2);
		assert.equal(await store.removeFinished(cutoff, 2), 1);
		assert.equal(await store.removeFinished(cutoff, 2), 0);
		assert.equal((await rows(path, 'a-')).run, 1, 'the running one stays');
		await rt.close();
	});

	test('how long finished runs are kept: the option, KAIRO_KEEP_FINISHED, or 24 hours', () => {
		assert.equal(keepFinished(undefined, undefined), 24 * HOUR);
		assert.equal(keepFinished(undefined, '7d'), 7 * 24 * HOUR);
		assert.equal(keepFinished(undefined, '30m'), 30 * 60 * 1000);
		assert.equal(keepFinished(undefined, 'forever'), Infinity);
		assert.equal(keepFinished(5000, '7d'), 5000, 'the option first');
		assert.throws(() => keepFinished(undefined, 'a week'), /KAIRO_KEEP_FINISHED/);
	});
});
