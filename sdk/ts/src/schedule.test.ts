// Waking the runtime that does not stay up (ADR 0053): a scheduler's /tick,
// and wake(at) for one-off wake-ups instead of polling. Needs go.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, describe, test } from 'node:test';

import { EmbeddedBackend } from './backend.ts';
import { nodeHandler } from './http.ts';
import { SQLiteStore } from './store.ts';
import { Kairo, Suspended, tickHandler } from './workflow.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-sdk-schedule-'));
// Built by scripts/check.sh (KAIRO_WASM), or here.
const wasm = process.env.KAIRO_WASM || join(dir, 'kairo.wasm');
const hasGo = spawnSync('go', ['version']).status === 0;
const servers: Server[] = [];

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

after(() => {
	for (const s of servers) s.close();
	rmSync(dir, { recursive: true, force: true });
});

async function listen(handler: (req: Request) => Promise<Response>): Promise<string> {
	const s = createServer(nodeHandler(handler));
	servers.push(s);
	await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
	return `http://127.0.0.1:${(s.address() as AddressInfo).port}`;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('waking the runtime (ADR 0053)', { skip: !hasGo }, () => {
	test('a scheduler ticks over HTTP with its token', async () => {
		const path = join(dir, 'tick.db');
		const wakes: number[] = [];
		const k = new Kairo({
			backend: await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm }),
			mode: 'suspend',
			tickSecret: 'cron-secret',
			wake: async (at) => void wakes.push(at),
		});
		k.defineAction('llm', { effect: 'unprotected', handler: async (q: string) => q.toUpperCase() });
		k.workflow('w', async (ctx, q: string) => {
			await ctx.sleep(300);
			return ctx.call('llm', q);
		});
		await k.start();
		const base = await listen(k.fetchHandler());

		const t0 = Date.now();
		await assert.rejects(k.run('w', 'hi', { id: 'tick-1' }), Suspended);
		assert.equal(wakes.length, 1, 'told when to wake');
		assert.ok(wakes[0]! >= t0 + 300 && wakes[0]! < t0 + 2000, `wake at ${wakes[0]! - t0}ms`);

		assert.equal((await fetch(base + '/kairo/tick')).status, 401, 'no token');
		assert.equal((await fetch(base + '/kairo/tick', { headers: { authorization: 'Bearer nope' } })).status, 401, 'a wrong token');
		const auth = { authorization: 'Bearer cron-secret' };
		// Too early: nothing to do; the same time comes back.
		assert.deepEqual(await (await fetch(base + '/kairo/tick', { headers: auth })).json(), { next: wakes[0] });

		await sleep(Math.max(0, wakes[0]! - Date.now()) + 20);
		const r = await fetch(base + '/kairo/tick', { method: 'POST', headers: auth });
		assert.deepEqual(await r.json(), { next: null }, 'nothing left to do');
		assert.equal(await k.run('w', 'hi', { id: 'tick-1' }), 'HI');
		await k.close();

		// Without tickSecret, /tick is not served.
		const k2 = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm }), mode: 'suspend' });
		await k2.start();
		const base2 = await listen(k2.fetchHandler());
		assert.equal((await fetch(base2 + '/kairo/tick', { headers: auth })).status, 404);
		await k2.close();
	});

	test('the next wake is the earliest timer or lease expiry', async () => {
		const store = new SQLiteStore(join(dir, 'next.db'));
		await store.init();
		assert.equal(await store.nextWake(), null);
		await store.withRun('r', () => ({ events: [], setTimers: [{ run: 'r', timer: 1, act: 1, at: 5000 }], result: undefined }));
		assert.equal(await store.nextWake(), 5000);
		await store.withRun('r', () => ({ events: [], setLeases: [{ run: 'r', act: 2, attempt: 1, owner: 'x', until: 4000 }], result: undefined }));
		assert.equal(await store.nextWake(), 4000);
		await store.withRun('r', () => ({ events: [], endLeases: [2], result: undefined }));
		assert.equal(await store.nextWake(), 5000);
		await store.close();
	});

	test('wake drives a workflow to its end, each tick a new process, without polling', async () => {
		const path = join(dir, 'wake.db');
		const runs = { llm: 0 };
		let ticks = 0;
		let pending: Promise<unknown> = Promise.resolve();
		// The "one-off scheduler": calls the function at the time it is given.
		const wake = async (at: number) => {
			const due = (async () => {
				await sleep(Math.max(0, at - Date.now()));
				ticks++;
				await tick();
			})();
			pending = pending.then(() => due);
		};
		async function make(): Promise<Kairo> {
			const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm }), mode: 'suspend', wake });
			k.defineAction('llm', { effect: 'unprotected', handler: async (q: string) => (runs.llm++, q.toUpperCase()) });
			k.workflow('w', async (ctx) => {
				await ctx.sleep(150);
				const a = await ctx.call('llm', 'a');
				await ctx.sleep(150);
				const b = await ctx.call('llm', 'b');
				return a + b;
			});
			await k.start();
			return k;
		}
		const tick = tickHandler(make);

		const k = await make();
		await assert.rejects(k.run('w', null, { id: 'wake-1' }), Suspended);
		await k.close();
		for (let i = 0; i < 200 && runs.llm < 2; i++) await sleep(25);
		await pending;

		const k2 = await make();
		assert.equal(await k2.run('w', null, { id: 'wake-1' }), 'AB');
		await k2.close();
		assert.equal(runs.llm, 2);
		assert.equal(ticks, 2, 'woken once per sleep');
	});
});
