// The run viewer (ADR 0088): loopback only, a key on every request, no other
// Host or Origin; reading changes nothing; operations are the SDK's; an
// open page hears of steps as they finish. Needs go (kairo.wasm).

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { request } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, describe, test } from 'node:test';

import { EmbeddedBackend } from './backend.ts';
import type { RunProgress } from './progress.ts';
import { SQLiteStore } from './store.ts';
import { serveViewer } from './viewer.ts';
import { Kairo } from './workflow.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-sdk-viewer-'));
const wasm = process.env.KAIRO_WASM || join(dir, 'kairo.wasm');
const hasGo = spawnSync('go', ['version']).status === 0;

before(() => {
	if (!hasGo || process.env.KAIRO_WASM) return;
	const w = spawnSync('go', ['build', '-buildmode=c-shared', '-o', wasm, './cmd/kairo-wasm'], {
		cwd: repo,
		stdio: 'inherit',
		env: { ...process.env, GOOS: 'wasip1', GOARCH: 'wasm' },
	});
	assert.equal(w.status, 0, 'building kairo.wasm');
});

after(() => rmSync(dir, { recursive: true, force: true }));

interface Reply {
	status: number;
	body: string;
}

/** A request to the viewer, with what headers are given (Host included). */
function call(base: URL, path: string, opts: { method?: string; headers?: Record<string, string>; body?: string } = {}): Promise<Reply> {
	return new Promise((res, rej) => {
		const req = request({ host: base.hostname, port: base.port, path, method: opts.method ?? 'GET', headers: opts.headers }, (r) => {
			let body = '';
			r.setEncoding('utf8');
			r.on('data', (c) => (body += c));
			r.on('end', () => res({ status: r.statusCode ?? 0, body }));
		});
		req.on('error', rej);
		req.end(opts.body);
	});
}

/** The progress each time the viewer's stream sends one, until stop. */
function watch(base: URL, key: string, id: string, got: (p: RunProgress) => void): () => void {
	const req = request({ host: base.hostname, port: base.port, path: `/api/watch?id=${encodeURIComponent(id)}`, headers: { 'x-kairo-key': key } }, (r) => {
		let buf = '';
		r.setEncoding('utf8');
		r.on('data', (c: string) => {
			buf += c;
			let i: number;
			while ((i = buf.indexOf('\n\n')) >= 0) {
				const line = buf.slice(0, i);
				buf = buf.slice(i + 2);
				if (line.startsWith('data: ')) got(JSON.parse(line.slice(6)));
			}
		});
	});
	req.on('error', () => {});
	req.end();
	return () => req.destroy();
}

async function waitUntil(cond: () => boolean | Promise<boolean>): Promise<void> {
	const deadline = Date.now() + 10_000;
	while (!(await cond())) {
		if (Date.now() > deadline) throw new Error('timed out');
		await new Promise((r) => setTimeout(r, 20));
	}
}

describe('run viewer (ADR 0088)', { skip: !hasGo }, () => {
	test('refuses requests without the key or from elsewhere, and reading changes nothing', async () => {
		const path = join(dir, 'guard.db');
		const runner = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm }) });
		runner.defineAction('step', { effect: 'unprotected', handler: async (n: number) => n + 1 });
		runner.workflow('guarded', async (ctx, n: number) => {
			await ctx.call('step', n);
			return ctx.waitFor<string>('go');
		});
		await runner.start();
		// The viewer is another process's view: its own runtime on the same file, not started.
		const viewing = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm }) });
		const viewer = await serveViewer(viewing, { refreshMs: 50 });
		const out = runner.run('guarded', 1, { id: 'guarded-1' });
		try {
			await waitUntil(async () => (await runner.children('guarded-1')).some((c) => c.plan === 'kairo.wait/go'));
			const url = new URL(viewer.url);
			const key = url.searchParams.get('key')!;
			const host = url.host;
			assert.ok(['127.0.0.1'].includes(url.hostname));

			// The key, the Host, the Origin; operations only with the key in a header, as JSON.
			assert.equal((await call(url, '/api/runs', { headers: { host } })).status, 403);
			assert.equal((await call(url, '/api/runs', { headers: { host, 'x-kairo-key': 'x'.repeat(key.length) } })).status, 403);
			assert.equal((await call(url, '/api/runs', { headers: { host: `attacker.example:${url.port}`, 'x-kairo-key': key } })).status, 403);
			assert.equal((await call(url, '/api/runs', { headers: { host, origin: 'http://attacker.example', 'x-kairo-key': key } })).status, 403);
			assert.equal((await call(url, `/?key=${key}`, { headers: { host } })).status, 200);
			const json = { 'content-type': 'application/json' };
			const signal = JSON.stringify({ id: 'guarded-1', name: 'go', payload: 'no' });
			assert.equal((await call(url, `/api/signal?key=${key}`, { method: 'POST', headers: { host, ...json }, body: signal })).status, 403);
			assert.equal((await call(url, '/api/signal', { method: 'POST', headers: { host, 'x-kairo-key': key, 'content-type': 'text/plain' }, body: signal })).status, 403);
			await assert.rejects(serveViewer(viewing, { host: '0.0.0.0' as '127.0.0.1' }), /loopback/);

			// Reading: the list, the progress, the page's stream; nothing changes.
			const ids = ['guarded-1', ...(await runner.children('guarded-1')).map((c) => c.run_id)];
			const before = await Promise.all(ids.map((id) => runner.backend.get(id)));
			const auth = { host, 'x-kairo-key': key };
			const list = await call(url, '/api/runs', { headers: auth });
			assert.equal(list.status, 200);
			assert.deepEqual(JSON.parse(list.body).map((r: { run_id: string }) => r.run_id), ['guarded-1']);
			const p = await call(url, '/api/progress?id=guarded-1', { headers: auth });
			assert.equal(p.status, 200);
			assert.deepEqual((JSON.parse(p.body) as RunProgress).runs.map((r) => r.name), ['guarded', 'step', 'go']);
			assert.equal((await call(url, '/api/progress?id=absent', { headers: auth })).status, 404);
			const seen: RunProgress[] = [];
			const stop = watch(url, key, 'guarded-1', (q) => seen.push(q));
			await waitUntil(() => seen.length > 0);
			await new Promise((r) => setTimeout(r, 200)); // reads again, more than once
			stop();
			assert.equal(seen.length, 1, 'the stream sends only what changed');
			assert.deepEqual(await Promise.all(ids.map((id) => runner.backend.get(id))), before);
		} finally {
			await viewer.close();
			await viewing.close();
			await runner.signal('guarded-1', 'go', 'done').catch(() => {});
			await out.catch(() => {});
			await runner.close();
		}
	});

	test('sends a signal and a cancellation through the SDK, and the open page hears of the steps', async () => {
		const path = join(dir, 'ops.db');
		const runner = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(path, { notify: true }), wasm }) });
		let release!: () => void;
		const gate = new Promise<void>((r) => (release = r));
		runner.defineAction('slow', { effect: 'unprotected', handler: async () => (await gate, 'slow done') });
		runner.workflow('approved', async (ctx) => {
			await ctx.call('slow', null);
			return ctx.waitFor<string>('approve');
		});
		runner.workflow('held', async (ctx) => ctx.waitFor<string>('never'));
		await runner.start();
		const viewing = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(path, { notify: true }), wasm }) });
		const viewer = await serveViewer(viewing, { refreshMs: 50 });
		let closed = false;
		const approved = runner.run('approved', null, { id: 'ops-approved' });
		// It does not end here: its process closes before the viewer cancels it.
		runner.run('held', null, { id: 'ops-held' }).catch(() => {});
		try {
			const url = new URL(viewer.url);
			const key = url.searchParams.get('key')!;
			const headers = { host: url.host, 'x-kairo-key': key, 'content-type': 'application/json' };
			await waitUntil(async () => (await runner.children('ops-approved')).length === 1);

			// The open page hears of the step as it finishes, then of the wait.
			const seen: RunProgress[] = [];
			const stop = watch(url, key, 'ops-approved', (q) => seen.push(q));
			await waitUntil(() => seen.length > 0);
			assert.deepEqual(seen[0]!.runs.find((r) => r.name === 'slow')!.events.map((e) => e.kind), ['started']);

			// This viewer has not the action's definition: it refuses to cancel the call, and the workflow, rather than do it in part.
			const ids = ['ops-approved', ...(await runner.children('ops-approved')).map((c) => c.run_id)];
			const before = await Promise.all(ids.map((id) => runner.backend.get(id)));
			const partly = await call(url, '/api/cancel', { method: 'POST', headers, body: JSON.stringify({ id: 'ops-approved' }) });
			assert.equal(partly.status, 409);
			assert.match(JSON.parse(partly.body).error, /kairo\.call\/slow/);
			assert.deepEqual(await Promise.all(ids.map((id) => runner.backend.get(id))), before);
			release();
			await waitUntil(() => seen.some((q) => q.runs.some((r) => r.name === 'approve')));
			const last = seen[seen.length - 1]!;
			assert.deepEqual(last.runs.find((r) => r.name === 'slow')!.events.map((e) => e.kind), ['started', 'succeeded']);
			stop();

			// A signal from the viewer is the SDK's: the workflow that runs elsewhere takes it up.
			const sent = await call(url, '/api/signal', { method: 'POST', headers, body: JSON.stringify({ id: 'ops-approved', name: 'approve', payload: 'yes' }) });
			assert.equal(sent.status, 200, sent.body);
			assert.equal(await approved, 'yes');
			const refused = await call(url, '/api/signal', { method: 'POST', headers, body: JSON.stringify({ id: 'absent', name: 'approve' }) });
			assert.equal(refused.status, 409);

			// A cancellation is taken at once, and the workflow ends cancelled. (Its
			// process is stopped first: a cancellation from another process races
			// the process that drives it, in the SDK, viewer or not.)
			await runner.close();
			closed = true;
			const cancel = await call(url, '/api/cancel', { method: 'POST', headers, body: JSON.stringify({ id: 'ops-held' }) });
			assert.equal(cancel.status, 202);
			await waitUntil(async () => (await viewing.backend.get('ops-held')).status === 'cancelled');
			for (const c of await viewing.children('ops-held')) assert.equal(c.status, 'cancelled');
		} finally {
			release();
			await viewer.close();
			await viewing.close();
			if (!closed) await runner.close();
		}
	});
});
