import assert from 'node:assert/strict';
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, test } from 'node:test';

import { CancelledError, Kairo } from './workflow.ts';

// Against a real kairod, built from this repository (needs go).
const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-sdk-'));
const port = 18420 + (process.pid % 1000);
const url = `http://127.0.0.1:${port}`;
const sock = join(dir, 'worker.sock');
let kairod: ChildProcess | undefined;
const hasGo = spawnSync('go', ['version']).status === 0;

before(async () => {
	if (!hasGo) return;
	const bin = join(dir, 'kairod');
	const b = spawnSync('go', ['build', '-o', bin, './cmd/kairod'], { cwd: repo, stdio: 'inherit' });
	assert.equal(b.status, 0, 'building kairod');
	kairod = spawn(bin, ['-data', join(dir, 'data'), '-http', `127.0.0.1:${port}`, '-socket', sock, '-nosync'], { stdio: 'inherit' });
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

const runs = { write: 0, llm: 0, slow: 0 };
let hang = true;
let slowAborted = false;

function kairo(): Kairo {
	const k = new Kairo({ url, worker: sock, concurrency: 8 });
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
	return k;
}

async function waitUntil(cond: () => boolean | Promise<boolean>): Promise<void> {
	for (let i = 0; i < 400; i++) {
		if (await cond()) return;
		await new Promise((r) => setTimeout(r, 25));
	}
	throw new Error('timed out');
}

test('a workflow resumed elsewhere runs no finished call again', { skip: !hasGo }, async () => {
	const k1 = kairo();
	await k1.start();
	const first = k1.run('edit', { files: ['a', 'b', 'c'] }, { id: 'wf-edit-1' }).catch(() => {});
	await waitUntil(() => runs.write === 1);
	assert.equal(runs.llm, 3);
	await k1.close(); // the first process "stops" while the workflow hangs
	void first;

	hang = false;
	const k2 = kairo();
	await k2.start();
	const out = await k2.run('edit', { files: ['a', 'b', 'c'] }, { id: 'wf-edit-1' });
	assert.deepEqual(out, {
		answers: [{ answer: 'A' }, { answer: 'B' }, { answer: 'C' }],
		w: { wrote: 'a' },
		again: { answer: 'DONE' },
	});
	assert.equal(runs.write, 1, 'the real action ran once');
	assert.equal(runs.llm, 4);
	// Finished: running it again returns its result and runs nothing.
	assert.deepEqual(await k2.run('edit', { files: ['a', 'b', 'c'] }, { id: 'wf-edit-1' }), out);
	assert.equal(runs.llm, 4);
	// A child workflow.
	assert.equal(await k2.run('parent', 7, { id: 'wf-parent-1' }), 'C7');
	await k2.close();
});

test('sleeps in kairo and waits for a signal', { skip: !hasGo }, async () => {
	const k = kairo();
	await k.start();
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

test('a cancelled workflow stops its calls', { skip: !hasGo }, async () => {
	const k = kairo();
	await k.start();
	const done = k.run('long', null, { id: 'wf-long-1' });
	await waitUntil(() => runs.slow === 1);
	await k.cancel('wf-long-1');
	await assert.rejects(done, CancelledError);
	await waitUntil(() => slowAborted);
	await k.close();
});
