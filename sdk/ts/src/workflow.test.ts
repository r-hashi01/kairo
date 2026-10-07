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
import { CancelledError, Kairo } from './workflow.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-sdk-'));
const hasGo = spawnSync('go', ['version']).status === 0;
const wasm = join(dir, 'kairo.wasm');
const port = 18420 + (process.pid % 1000);
const url = `http://127.0.0.1:${port}`;
const sock = join(dir, 'worker.sock');
let kairod: ChildProcess | undefined;

before(async () => {
	if (!hasGo) return;
	const w = spawnSync('go', ['build', '-buildmode=c-shared', '-o', wasm, './cmd/kairo-wasm'], {
		cwd: repo,
		stdio: 'inherit',
		env: { ...process.env, GOOS: 'wasip1', GOARCH: 'wasm' },
	});
	assert.equal(w.status, 0, 'building kairo.wasm');
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
