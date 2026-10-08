// Actions called over HTTP(S) (ADR 0052): the runtime embedded here calls
// an action at its URL; the outcome comes in the answer (200), or later on
// the callback (202). Needs go (the WASM core) and openssl (the HTTPS test).

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import { createServer as createTLSServer } from 'node:https';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, describe, test } from 'node:test';

import { EmbeddedBackend } from './backend.ts';
import { checkURL, nodeHandler, post, sign } from './http.ts';
import { SQLiteStore } from './store.ts';
import { Kairo, Suspended } from './workflow.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-sdk-http-'));
const hasGo = spawnSync('go', ['version']).status === 0;
const hasOpenSSL = spawnSync('openssl', ['version']).status === 0;
// Built by scripts/check.sh (KAIRO_WASM), or here.
const wasm = process.env.KAIRO_WASM || join(dir, 'kairo.wasm');
const secret = 'test-secret';
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

type Handler = (req: Request) => Promise<Response>;

/** Serves handler on this machine; returns its base URL. */
async function listen(handler: Handler, tls?: { key: string; cert: string }): Promise<string> {
	const s = tls ? createTLSServer(tls, nodeHandler(handler)) : createServer(nodeHandler(handler));
	servers.push(s);
	await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
	const port = (s.address() as AddressInfo).port;
	return tls ? `https://localhost:${port}` : `http://127.0.0.1:${port}`;
}

/**
 * The callback's server, in front of whichever process is "running" now:
 * a callback may reach a process other than the one that called.
 */
async function callbackServer(): Promise<{ url: string; to: (k: Kairo) => void }> {
	let current: Kairo | undefined;
	const url = await listen(async (req) => {
		if (!current) return new Response('no process', { status: 503 });
		return current.fetchHandler()(req);
	});
	return { url: url + '/kairo/callback', to: (k) => (current = k) };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('actions over HTTP(S)', { skip: !hasGo }, () => {
	test('an action answers at once (200)', async () => {
		let runs = 0;
		const remote = new Kairo({ secret });
		remote.defineAction('shout', { effect: 'real', handler: async (i: { s: string }) => (runs++, i.s.toUpperCase()) });
		const actionURL = (await listen(remote.fetchHandler())) + '/kairo/action';
		const cb = await callbackServer();

		const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(join(dir, 'sync.db')), wasm }), secret, callbackUrl: cb.url });
		k.defineAction('shout', { effect: 'real', url: actionURL, handler: async () => assert.fail('runs at the URL') });
		k.workflow('w', async (ctx, s: string) => ctx.call('shout', { s }));
		await k.start();
		cb.to(k);
		assert.equal(await k.run('w', 'hi', { id: 'sync-1' }), 'HI');
		assert.equal(runs, 1);
		await k.close();
	});

	test('an action answers later, on the callback, to another process (202)', async () => {
		let runs = 0;
		let release!: () => void;
		const released = new Promise<void>((r) => (release = r));
		const remote = new Kairo({ secret, waitUntil: () => {} });
		remote.defineAction('render', {
			effect: 'real',
			async: true,
			handler: async (i: { n: number }) => {
				runs++;
				await released;
				return { frames: i.n * 2 };
			},
		});
		const actionURL = (await listen(remote.fetchHandler())) + '/kairo/action';
		const cb = await callbackServer();
		const path = join(dir, 'async.db');
		async function invocation(): Promise<Kairo> {
			const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm }), mode: 'suspend', secret, callbackUrl: cb.url });
			k.defineAction('render', { effect: 'real', url: actionURL, async: true, handler: async () => assert.fail('runs at the URL') });
			k.workflow('w', async (ctx, n: number) => ctx.call('render', { n }));
			await k.start();
			cb.to(k);
			return k;
		}

		// 1: calls the action, which accepts it; this process ends.
		let k = await invocation();
		await assert.rejects(k.run('w', 21, { id: 'async-1' }), Suspended);
		assert.equal(runs, 1);
		await k.close();

		// 2: another process takes the callback, and the workflow ends.
		k = await invocation();
		release();
		for (let i = 0; i < 400; i++) {
			try {
				assert.deepEqual(await k.run('w', 21, { id: 'async-1' }), { frames: 42 });
				break;
			} catch (e) {
				if (!(e instanceof Suspended) || i === 399) throw e;
				await sleep(25);
			}
		}
		assert.equal(runs, 1, 'the action ran once');
		await k.close();
	});

	test('an action whose callback never comes: unprotected retries, real stops for review', async () => {
		const calls = { idem: 0, real: 0 };
		const remote = async (req: Request): Promise<Response> => {
			const m = await req.json();
			if (m.action === 'idem') {
				// The first call is accepted and forgotten; the retry answers.
				if (calls.idem++ === 0) return Response.json({ accepted: true }, { status: 202 });
				return Response.json({ output: `attempt ${m.attempt}` });
			}
			calls.real++;
			return Response.json({ accepted: true }, { status: 202 });
		};
		const actionURL = await listen(remote);
		const cb = await callbackServer();
		const path = join(dir, 'expire.db');
		async function invocation(): Promise<Kairo> {
			const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(path), wasm }), mode: 'suspend', secret, callbackUrl: cb.url });
			k.defineAction('idem', { effect: 'unprotected', url: actionURL, timeout: '200ms', handler: async () => null });
			k.defineAction('real', { effect: 'real', url: actionURL, timeout: '200ms', handler: async () => null });
			k.workflow('w', async (ctx) => {
				const a = await ctx.call('idem', {});
				try {
					await ctx.call('real', {});
					return { a, real: 'ok' };
				} catch (e) {
					if (e instanceof Suspended) throw e;
					return { a, real: String((e as Error).message) };
				}
			});
			await k.start();
			cb.to(k);
			return k;
		}

		let k = await invocation();
		await assert.rejects(k.run('w', null, { id: 'expire-1' }), Suspended);
		await k.close();
		// The scheduler's ticks, in new processes: leases expire, the
		// unprotected step is retried (after its backoff), the real one stops.
		let out: { a: string; real: string } | undefined;
		for (let i = 0; i < 40 && !out; i++) {
			await sleep(100);
			k = await invocation();
			await k.tick();
			out = (await k.run('w', null, { id: 'expire-1' }).catch((e) => {
				if (e instanceof Suspended) return undefined;
				throw e;
			})) as typeof out;
			await k.close();
		}
		assert.ok(out, 'the workflow ended');
		assert.equal(out.a, 'attempt 2');
		assert.match(out.real, /blocked/);
		assert.deepEqual(calls, { idem: 2, real: 1 }, 'the real action is not called again');
	});

	test('4xx fails the step; 5xx is unknown: unprotected retries, real stops for review', async () => {
		const calls = { bad: 0, flaky: 0, down: 0 };
		const actionURL = await listen(async (req) => {
			const m = await req.json();
			calls[m.action as keyof typeof calls]++;
			if (m.action === 'bad') return Response.json({ error: 'no such thing' }, { status: 422 });
			if (m.action === 'flaky' && calls.flaky === 1) return new Response('oops', { status: 503 });
			if (m.action === 'flaky') return Response.json({ output: 'ok' });
			return new Response('oops', { status: 500 });
		});
		const cb = await callbackServer();
		const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(join(dir, 'status.db')), wasm }), secret, callbackUrl: cb.url });
		for (const [name, effect] of [['bad', 'unprotected'], ['flaky', 'unprotected'], ['down', 'real']] as const) {
			k.defineAction(name, { effect, url: actionURL, handler: async () => null });
		}
		k.workflow('w', async (ctx, name: string) => {
			try {
				return await ctx.call(name, {});
			} catch (e) {
				return String((e as Error).message);
			}
		});
		await k.start();
		cb.to(k);
		assert.match(String(await k.run('w', 'bad', { id: 'st-bad' })), /failed.*422/);
		assert.equal(calls.bad, 1, 'a 4xx is not retried');
		assert.equal(await k.run('w', 'flaky', { id: 'st-flaky' }), 'ok');
		assert.equal(calls.flaky, 2);
		assert.match(String(await k.run('w', 'down', { id: 'st-down' })), /blocked/);
		assert.equal(calls.down, 1);
		await k.close();
	});

	test('calls and callbacks without the right signature are refused', async () => {
		let runs = 0;
		const k = new Kairo({ secret });
		k.defineAction('x', { handler: async () => (runs++, 1) });
		const base = await listen(k.fetchHandler());
		const body = JSON.stringify({ run_id: 'r', act: 1, attempt: 1, action: 'x', input: null });
		for (const path of ['/kairo/action', '/kairo/callback']) {
			for (const signature of [undefined, sign('other', body), sign(secret, body + ' '), sign(secret, body, Date.now() - 10 * 60 * 1000)]) {
				const r = await post(base + path, body, { headers: signature ? { 'kairo-signature': signature } : {} });
				assert.equal(r.status, 401, `${path} with ${signature}`);
			}
		}
		assert.equal(runs, 0);
		const r = await post(base + '/kairo/action', body, { headers: { 'kairo-signature': sign(secret, body) } });
		assert.equal(r.status, 200);
		assert.deepEqual(JSON.parse(r.body), { output: 1 });
	});

	test('plain http only to this machine, or when allowed', async () => {
		assert.throws(() => checkURL('http://example.com/a'), /plain http/);
		assert.throws(() => checkURL('ftp://localhost/a'), /only http/);
		for (const u of ['https://example.com/a', 'http://localhost:1/a', 'http://127.0.0.1/a', 'http://[::1]:8/a']) checkURL(u);
		checkURL('http://10.0.0.5/a', true);

		const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(join(dir, 'urls.db')), wasm }), secret, callbackUrl: 'http://10.0.0.5/cb' });
		assert.throws(() => k.defineAction('a', { url: 'http://10.0.0.5/a', handler: async () => null }), /plain http/);
		k.defineAction('a', { url: 'http://10.0.0.5/a', allowInsecure: true, handler: async () => null });
		await assert.rejects(k.start(), /plain http/, 'the callback URL is checked too');
		await k.close();
	});

	test('over https, trusting a certificate of our own', { skip: !hasOpenSSL }, async () => {
		const key = join(dir, 'key.pem');
		const cert = join(dir, 'cert.pem');
		const g = spawnSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key, '-out', cert, '-days', '1',
			'-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'], { stdio: 'ignore' });
		assert.equal(g.status, 0, 'openssl');
		const tls = { key: readFileSync(key, 'utf8'), cert: readFileSync(cert, 'utf8') };

		const remote = new Kairo({ secret });
		remote.defineAction('shout', { handler: async (s: string) => s.toUpperCase() });
		const actionURL = (await listen(remote.fetchHandler(), tls)) + '/kairo/action';
		await assert.rejects(post(actionURL, '{}'), 'a certificate not trusted is refused');

		const cb = await callbackServer();
		const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(join(dir, 'tls.db')), wasm }), secret, callbackUrl: cb.url, ca: tls.cert });
		k.defineAction('shout', { effect: 'unprotected', url: actionURL, handler: async () => null });
		k.workflow('w', async (ctx, s: string) => ctx.call('shout', s));
		await k.start();
		cb.to(k);
		assert.equal(await k.run('w', 'hi', { id: 'tls-1' }), 'HI');
		await k.close();
	});
});
