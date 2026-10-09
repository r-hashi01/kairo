// The test env (createTestEnv): a workflow that sleeps a day and waits an
// hour for an approval, tested without waiting; the same test as sdk/go's
// kairotest TestEnv. Needs go (to build kairo.wasm from this repository).

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, test } from 'node:test';

import { createTestEnv } from './testing.ts';
import { Suspended, TimedOutError } from './workflow.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-testing-'));
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

interface Order {
	id: string;
	amount: number;
}

const HOUR = 3600_000;

test('test env: a day and an hour without waiting', { skip: !hasGo && !process.env.KAIRO_WASM }, async () => {
	const env = await createTestEnv({ wasm });
	try {
		let charged = 0;
		env.k.defineAction<Order, string>('charge', {
			effect: 'real',
			handler: async (o) => {
				charged++;
				return `charged ${o.id}`;
			},
		});
		env.k.workflow<Order, string>('refund', async (ctx, o) => {
			await ctx.sleep(24 * HOUR);
			let by: string;
			try {
				by = await ctx.waitFor<string>('approve', { timeout: HOUR });
			} catch (e) {
				if (e instanceof TimedOutError) return 'not approved';
				throw e;
			}
			const r = await ctx.call<string>('charge', o);
			return `${r} by ${by}`;
		});
		await env.start();
		const t0 = env.now();
		assert.equal(t0, Date.UTC(2026, 0, 1));

		await assert.rejects(env.k.run('refund', { id: 'o-1', amount: 5 }, { id: 'r-1' }), Suspended);
		await env.advance(23 * HOUR);
		await assert.rejects(env.k.result('r-1'), Suspended, 'an hour early');
		await env.advance(HOUR);
		await env.signal('r-1', 'approve', 'alice');
		assert.equal(await env.k.result<string>('r-1'), 'charged o-1 by alice');
		assert.equal(charged, 1);
		const calls = await env.calls('r-1');
		assert.deepEqual(
			calls.map((c) => `${c.kind} ${c.name} ${c.status}`),
			['call kairo.sleep completed', 'wait approve completed', 'call charge completed'],
		);
		assert.deepEqual(calls[2].input, { id: 'o-1', amount: 5 });
		assert.equal(calls[1].output, 'alice');

		// No approval within the hour.
		await assert.rejects(env.k.run('refund', { id: 'o-2', amount: 0 }, { id: 'r-2' }), Suspended);
		await env.advance(25 * HOUR);
		assert.equal(await env.k.result<string>('r-2'), 'not approved');
		assert.equal(charged, 1);
		assert.equal(env.now(), t0 + 24 * HOUR + 25 * HOUR);
	} finally {
		await env.close();
	}
});
