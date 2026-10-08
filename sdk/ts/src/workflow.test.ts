// Workflows written as code (ADR 0049), one suite against each backend:
// kairod (built from this repository) and the runtime embedded here with
// SQLite (ADR 0051, its WASM core built from this repository). Needs go.

import assert from 'node:assert/strict';
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, describe, test } from 'node:test';

import { EmbeddedBackend, HttpBackend, type Backend } from './backend.ts';
import { PostgresStore, SQLiteStore } from './store.ts';
import { CancelledError, Kairo, Suspended } from './workflow.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-sdk-'));
const hasGo = spawnSync('go', ['version']).status === 0;
// Built by scripts/check.sh (KAIRO_WASM), or here.
const wasm = process.env.KAIRO_WASM || join(dir, 'kairo.wasm');
const port = 18420 + (process.pid % 1000);
const url = `http://127.0.0.1:${port}`;
const sock = join(dir, 'worker.sock');
let kairod: ChildProcess | undefined;

before(async () => {
	if (!hasGo) return;
	if (!process.env.KAIRO_WASM) {
		const w = spawnSync('go', ['build', '-buildmode=c-shared', '-o', wasm, './cmd/kairo-wasm'], {
			cwd: repo,
			stdio: 'inherit',
			env: { ...process.env, GOOS: 'wasip1', GOARCH: 'wasm' },
		});
		assert.equal(w.status, 0, 'building kairo.wasm');
	}
	const bin = join(dir, 'kairod');
	assert.equal(spawnSync('go', ['build', '-o', bin, './cmd/kairod'], { cwd: repo, stdio: 'inherit' }).status, 0, 'building kairod');
	kairod = spawn(bin, ['-data', join(dir, 'data'), '-http', `127.0.0.1:${port}`, '-socket', sock, '-nosync'], { stdio: 'ignore' });
	for (let i = 0; i < 200; i++) {
		try {
			if ((await fetch(url + '/v1/stats')).ok) return;
		} catch {}
		await new Promise((r) => setTimeout(r, 50));
	}
	throw new Error('kairod did not start');
});

after(() => {
	kairod?.kill('SIGTERM');
	rmSync(dir, { recursive: true, force: true });
});

async function waitUntil(cond: () => boolean | Promise<boolean>): Promise<void> {
	for (let i = 0; i < 400; i++) {
		if (await cond()) return;
		await new Promise((r) => setTimeout(r, 25));
	}
	throw new Error('timed out');
}

/** The suite, against backends made by backend() (a new one = a new process). */
function suite(name: string, backend: () => Promise<Backend>) {
	describe(name, { skip: !hasGo }, () => {
		const runs = { write: 0, llm: 0, slow: 0 };
		let hang = true;
		let slowAborted = false;

		async function kairo(): Promise<Kairo> {
			const k = new Kairo({ backend: await backend(), concurrency: 8 });
			k.defineAction('write', {
				effect: 'real',
				handler: async (input: { path: string }) => {
					runs.write++;
					return { wrote: input.path };
				},
			});
			k.defineAction('llm', {
				effect: 'unprotected',
				handler: async (input: { q: string }) => {
					runs.llm++;
					return { answer: input.q.toUpperCase() };
				},
			});
			k.defineAction('slow', {
				handler: async (_input, ctx) => {
					runs.slow++;
					await new Promise<void>((r) => ctx.signal.addEventListener('abort', () => r()));
					slowAborted = true;
					return null;
				},
			});
			k.workflow('edit', async (ctx, input: { files: string[] }) => {
				const answers = await Promise.all(input.files.map((f) => ctx.call('llm', { q: f })));
				const w = await ctx.call('write', { path: input.files[0] });
				if (hang) await new Promise(() => {}); // as a process that stopped here
				const again = await ctx.call('llm', { q: 'done' });
				return { answers, w, again };
			});
			k.workflow('timed', async (ctx) => {
				const t0 = await ctx.now();
				await ctx.sleep(300);
				const t1 = await ctx.now();
				const approval = await ctx.waitFor<{ by: string }>('approve');
				return { slept: t1 - t0, by: approval.by };
			});
			k.workflow('long', async (ctx) => ctx.call('slow', null));
			k.workflow('parent', async (ctx, n: number) => ctx.workflow('child', n));
			k.workflow('child', async (ctx, n: number) => (await ctx.call('llm', { q: `c${n}` })).answer);
			await k.start();
			return k;
		}

		test('a workflow resumed elsewhere runs no finished call again', async () => {
			hang = true;
			const k1 = await kairo();
			void k1.run('edit', { files: ['a', 'b', 'c'] }, { id: 'wf-edit-1' }).catch(() => {});
			await waitUntil(() => runs.write === 1);
			assert.equal(runs.llm, 3);
			await k1.close(); // the first process "stops"

			hang = false;
			const k2 = await kairo();
			const out = await k2.run('edit', { files: ['a', 'b', 'c'] }, { id: 'wf-edit-1' });
			assert.deepEqual(out, {
				answers: [{ answer: 'A' }, { answer: 'B' }, { answer: 'C' }],
				w: { wrote: 'a' },
				again: { answer: 'DONE' },
			});
			assert.equal(runs.write, 1, 'the real action ran once');
			assert.equal(runs.llm, 4);
			assert.deepEqual(await k2.run('edit', { files: ['a', 'b', 'c'] }, { id: 'wf-edit-1' }), out);
			assert.equal(runs.llm, 4);
			assert.equal(await k2.run('parent', 7, { id: 'wf-parent-1' }), 'C7');
			await k2.close();
		});

		test('sleeps in kairo and waits for a signal', async () => {
			const k = await kairo();
			const done = k.run('timed', null, { id: 'wf-timed-1' });
			await waitUntil(async () => {
				try {
					await k.signal('wf-timed-1', 'approve', { by: 'alice' });
					return true;
				} catch {
					return false;
				}
			});
			const out = await done;
			assert.ok(out.slept >= 300, `slept ${out.slept}ms`);
			assert.equal(out.by, 'alice');
			await k.close();
		});

		test('a cancelled workflow stops its calls', async () => {
			const k = await kairo();
			const done = k.run('long', null, { id: 'wf-long-1' });
			await waitUntil(() => runs.slow === 1);
			await k.cancel('wf-long-1');
			await assert.rejects(done, CancelledError);
			await waitUntil(() => slowAborted);
			await k.close();
		});
	});
}

suite('kairod', async () => new HttpBackend({ url, worker: sock, concurrency: 8 }));

// One database file for the suite: a new backend on it is a new process.
const db = join(dir, 'embedded.db');
suite('embedded (SQLite)', async () => EmbeddedBackend.open({ store: new SQLiteStore(db), wasm }));

// PostgreSQL, when given: KAIRO_SDK_PG_DSN, and KAIRO_SDK_PG_MODULE, a
// directory where node-postgres ("pg") is installed (the SDK does not
// depend on it).
const dsn = process.env.KAIRO_SDK_PG_DSN;
const pgDir = process.env.KAIRO_SDK_PG_MODULE;
if (dsn && pgDir) {
	const pg = createRequire(join(pgDir, 'x.js'))('pg');
	const pool = new pg.Pool({ connectionString: dsn, max: 16 });
	const prefix = `kt${process.pid}_`;
	after(async () => {
		for (const t of ['run', 'event', 'timer']) await pool.query(`DROP TABLE IF EXISTS ${prefix}${t}`);
		await pool.end();
	});
	suite('embedded (PostgreSQL)', async () => EmbeddedBackend.open({ store: new PostgresStore(pool, { prefix }), wasm }));
}

// Serverless (suspend mode, ADR 0051): each "invocation" is a new process
// that goes as far as it can and returns. A scheduler's tick and a signal
// drive the workflow on; no call runs twice.
describe('serverless (suspend)', { skip: !hasGo }, () => {
	test('a workflow goes on across invocations', async () => {
		const path = join(dir, 'serverless.db');
		const runs = { llm: 0, write: 0 };
		async function invocation(): Promise<Kairo> {
			const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm }), mode: 'suspend' });
			k.defineAction('llm', { effect: 'unprotected', handler: async (i: { q: string }) => (runs.llm++, i.q.toUpperCase()) });
			k.defineAction('write', { effect: 'real', handler: async (i: { v: string }) => (runs.write++, `wrote ${i.v}`) });
			k.workflow('job', async (ctx, input: { q: string }) => {
				const a = await ctx.call('llm', { q: input.q });
				await ctx.sleep(300);
				const w = await ctx.call('write', { v: a });
				const ok = await ctx.waitFor<{ by: string }>('approve');
				return { a, w, by: ok.by };
			});
			await k.start();
			return k;
		}

		// 1: starts, runs llm, and suspends at the sleep.
		let k = await invocation();
		await assert.rejects(k.run('job', { q: 'hi' }, { id: 'sl-1' }), Suspended);
		assert.deepEqual(runs, { llm: 1, write: 0 });
		await k.close();

		// 2: the scheduler's tick after the sleep: write runs, then it waits for the signal.
		await new Promise((r) => setTimeout(r, 350));
		k = await invocation();
		await k.tick();
		assert.deepEqual(runs, { llm: 1, write: 1 });
		await k.close();

		// 3: the signal: the workflow ends.
		k = await invocation();
		await k.signal('sl-1', 'approve', { by: 'alice' });
		assert.deepEqual(await k.run('job', { q: 'hi' }, { id: 'sl-1' }), { a: 'HI', w: 'wrote HI', by: 'alice' });
		assert.deepEqual(runs, { llm: 1, write: 1 }, 'no call ran twice');
		await k.close();
	});
});

// kairod restarts while a workflow waits (ADR 0050): its finished calls
// were submitted with keep_output, so resuming it after the restart finds
// their results, and the real call does not run again.
describe('kairod restarts', { skip: !hasGo }, () => {
	test('a workflow resumes after kairod restarts, its real call run once', { timeout: 60_000 }, async () => {
		const t0 = Date.now();
		const mark = (what: string) => console.log(`restart test: ${what} at ${Date.now() - t0}ms`);
		const data = join(dir, 'restart-data');
		const port2 = port + 1;
		const url2 = `http://127.0.0.1:${port2}`;
		const sock2 = join(dir, 'restart.sock');
		async function startKairod(): Promise<ChildProcess> {
			const p = spawn(join(dir, 'kairod'), ['-data', data, '-http', `127.0.0.1:${port2}`, '-socket', sock2, '-nosync'], { stdio: 'ignore' });
			for (let i = 0; i < 200; i++) {
				try {
					if ((await fetch(url2 + '/v1/stats')).ok) return p;
				} catch {}
				await new Promise((r) => setTimeout(r, 50));
			}
			throw new Error('kairod did not start');
		}
		async function stop(p: ChildProcess): Promise<void> {
			const exited = new Promise((r) => p.once('exit', r));
			p.kill('SIGTERM');
			await exited;
		}
		let writes = 0;
		async function kairo(): Promise<Kairo> {
			const k = new Kairo({ backend: new HttpBackend({ url: url2, worker: sock2, concurrency: 4 }) });
			k.defineAction('write', { effect: 'real', handler: async (v: string) => (writes++, `wrote ${v}`) });
			k.workflow('approve-then', async (ctx) => {
				const w = await ctx.call<string>('write', 'x');
				const ok = await ctx.waitFor<{ by: string }>('approve');
				return `${w} for ${ok.by}`;
			});
			await k.start();
			return k;
		}

		// Whatever happens, what this test started stops with it (a kairod
		// or a worker left running would keep the test process alive).
		const kairods: ChildProcess[] = [];
		const clients: Kairo[] = [];
		try {
			kairods.push(await startKairod());
			let k = await kairo();
			clients.push(k);
			mark('started');
			const first = k.run('approve-then', null, { id: 'restart-1' }).catch(() => {});
			await waitUntil(() => writes === 1);
			mark('wrote');
			await k.close();
			await first;
			mark('closed');
			await stop(kairods[0]!);
			mark('kairod stopped');

			kairods.push(await startKairod());
			k = await kairo();
			clients.push(k);
			mark('restarted');
			// Resumed: the finished call returns its kept result, then it waits.
			const resumed = k.run('approve-then', null, { id: 'restart-1' });
			for (let i = 0; ; i++) {
				try {
					await k.signal('restart-1', 'approve', { by: 'alice' });
					break;
				} catch (e) {
					if (i > 200) throw e;
					await new Promise((r) => setTimeout(r, 25));
				}
			}
			mark('signalled');
			assert.equal(await resumed, 'wrote x for alice');
			assert.equal(writes, 1, 'the real call did not run again');
		} finally {
			for (const c of clients) await c.close().catch(() => {});
			for (const p of kairods) if (p.exitCode === null && p.signalCode === null) await stop(p);
		}
	});
});
