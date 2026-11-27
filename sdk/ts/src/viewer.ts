// The run viewer (ADR 0088): a page on this machine's loopback that shows a
// code workflow's progress, from the public reads only (list, progress),
// and sends signals, resolutions and cancellations through the public
// operations. It does not drive workflows: the processes that run them
// take up what it sends.
//
// Inputs and outputs are the user's data, and the operations change runs:
// it listens on loopback only, every request carries a key made at start
// (given only in the URL printed to who started it), and a request of
// another Host or Origin is refused. The page loads nothing from outside.

import { randomBytes, timingSafeEqual } from 'node:crypto';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';

import { finished, KairoError, type RunInfo } from './client.ts';
import { Embedded } from './embedded.ts';
import type { RunProgress } from './progress.ts';
import { namedPlan } from './plans.ts';
import { PAGE } from './viewer-page.ts';
import type { Kairo } from './workflow.ts';

export interface ViewerOptions {
	/** The loopback address to listen on: 127.0.0.1 (default) or ::1. No other. */
	host?: '127.0.0.1' | '::1';
	/** The port (default: one the system gives). */
	port?: number;
	/** How often an open page's workflow is read again while the page is connected (ms; default 1000). */
	refreshMs?: number;
	/** How many workflows the list shows, newest first (default 200). */
	listLimit?: number;
}

export interface Viewer {
	/** The page, with its key: give it only to who may see and act on the runs. */
	readonly url: string;
	close(): Promise<void>;
}

const MAX_BODY = 1 << 20;

/**
 * Opens the viewer on k's runs (the embedded runtime). Reading changes
 * nothing; the operations are k's (signal, resolve, cancel).
 */
export async function serveViewer(k: Kairo, opts: ViewerOptions = {}): Promise<Viewer> {
	const host = opts.host ?? '127.0.0.1';
	if (host !== '127.0.0.1' && host !== '::1') throw new Error('the viewer listens on loopback only (127.0.0.1 or ::1)');
	const given = (k.backend as { runtime?: unknown }).runtime;
	if (!(given instanceof Embedded)) throw new Error('the viewer needs the embedded runtime');
	const rt: Embedded = given;
	// Plans made from their name (waits, workflow runs): a viewer of another
	// process's runs can apply its operations to them. It does not make this
	// runtime drive anything: nothing here ticks or sweeps.
	rt.planFor ??= namedPlan;
	const refreshMs = Math.max(100, opts.refreshMs ?? 1000);
	const listLimit = Math.max(1, opts.listLimit ?? 200);
	const key = randomBytes(24).toString('base64url');
	const keyBytes = Buffer.from(key);
	const streams = new Set<ServerResponse>();
	const pending = new Set<Promise<unknown>>();
	let hosts = new Set<string>();

	const server: Server = createServer((req, res) => {
		handle(req, res).catch((e) => {
			if (res.headersSent) return void res.destroy();
			// What was asked is wrong (400, 404, 413), or the operation was refused (a run not waiting, settled: 409).
			const status = e instanceof KairoError ? e.status : e instanceof SyntaxError ? 400 : req.method === 'POST' ? 409 : 500;
			send(res, status, { error: message(e) });
		});
	});

	function allowed(req: IncomingMessage, url: URL): boolean {
		if (!hosts.has(req.headers.host ?? '')) return false;
		const origin = req.headers.origin;
		if (origin !== undefined && !hosts.has(origin.replace(/^http:\/\//, ''))) return false;
		const given = req.headers['x-kairo-key'] ?? url.searchParams.get('key') ?? '';
		const b = Buffer.from(Array.isArray(given) ? given[0]! : given);
		return b.length === keyBytes.length && timingSafeEqual(b, keyBytes);
	}

	async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
		const url = new URL(req.url ?? '/', 'http://viewer');
		if (!allowed(req, url)) {
			req.resume();
			return send(res, 403, { error: 'forbidden' });
		}
		if (req.method === 'GET') {
			if (url.pathname === '/') return page(res);
			if (url.pathname === '/api/runs') return send(res, 200, await runs());
			if (url.pathname === '/api/progress') return send(res, 200, await progress(param(url, 'id')));
			if (url.pathname === '/api/watch') return watch(req, res, param(url, 'id'));
			return send(res, 404, { error: 'not found' });
		}
		if (req.method === 'POST') {
			// Operations: the key in a header (a form or a link cannot set it), JSON only.
			if (!req.headers['x-kairo-key'] || !(req.headers['content-type'] ?? '').startsWith('application/json')) {
				req.resume();
				return send(res, 403, { error: 'forbidden' });
			}
			const body = await json(req);
			if (url.pathname === '/api/signal') {
				await k.signal(str(body.id), str(body.name), body.payload ?? null);
				return send(res, 200, { ok: true });
			}
			if (url.pathname === '/api/resolve') {
				await operable([await k.backend.get(str(body.callId))]);
				if (body.failed) await k.resolveFailed(str(body.callId), typeof body.error === 'string' ? body.error : undefined);
				else await k.resolve(str(body.callId), body.output ?? null);
				return send(res, 200, { ok: true });
			}
			if (url.pathname === '/api/cancel') {
				// Taken: the viewer does not wait for the workflow to settle; the page shows it.
				const id = str(body.id);
				await operable((await k.progress(id)).runs);
				const p = k.cancel(id).catch((e) => rt.logger.warn('kairo viewer: cancel failed', { runId: id, err: e }));
				pending.add(p);
				void p.finally(() => pending.delete(p));
				return send(res, 202, { ok: true });
			}
			return send(res, 404, { error: 'not found' });
		}
		req.resume();
		send(res, 405, { error: 'method not allowed' });
	}

	/**
	 * Refuses an operation that this process could apply only in part: one
	 * on unfinished runs whose plans it does not have (calls of actions
	 * defined in the process that runs the workflow).
	 */
	async function operable(runs: RunInfo[]): Promise<void> {
		const unknown = [...new Set(runs.filter((r) => !finished(r) && !rt.knowsPlan(r.plan)).map((r) => r.plan))];
		if (unknown.length > 0)
			throw new KairoError(
				409,
				`this viewer does not have the definitions of ${unknown.join(', ')}: open it in the process that runs the workflow (serveViewer)`,
			);
	}

	async function runs(): Promise<RunInfo[]> {
		// The newest: the list is in creation order, so read pages and keep the last listLimit.
		let out: RunInfo[] = [];
		let after: string | undefined;
		for (;;) {
			const page = await k.list({ after, limit: 500 });
			out = out.concat(page.filter((r) => r.plan === 'kairo.workflow'));
			if (out.length > listLimit) out = out.slice(out.length - listLimit);
			if (page.length < 500) break;
			after = page[page.length - 1]!.run_id;
		}
		return out.reverse().map(({ input: _input, output: _output, ...r }) => r);
	}

	async function progress(id: string): Promise<RunProgress> {
		return k.progress(id);
	}

	function watch(req: IncomingMessage, res: ServerResponse, id: string): void {
		res.writeHead(200, { ...HEADERS, 'content-type': 'text/event-stream', 'cache-control': 'no-store' });
		streams.add(res);
		let last = '';
		let timer: ReturnType<typeof setTimeout> | undefined;
		let reading = false;
		let again = false;
		let closed = false;
		const read = async () => {
			if (closed) return;
			if (reading) {
				again = true;
				return;
			}
			reading = true;
			clearTimeout(timer);
			try {
				let data: string;
				try {
					data = JSON.stringify(await progress(id));
				} catch (e) {
					data = JSON.stringify({ error: message(e), status: e instanceof KairoError ? e.status : 500 });
				}
				if (data !== last && !closed) {
					last = data;
					res.write(`data: ${data}\n\n`);
				}
			} finally {
				reading = false;
				if (!closed) {
					if (again) {
						again = false;
						void read();
					} else timer = setTimeout(read, refreshMs);
				}
			}
		};
		// A run settled elsewhere: read now (a hint; the interval reads anyway).
		const stop = rt.onSettledElsewhere(() => void read());
		const stopHere = rt.onSettled(() => void read());
		req.on('close', () => {
			closed = true;
			clearTimeout(timer);
			stop();
			stopHere();
			streams.delete(res);
		});
		void read();
	}

	await new Promise<void>((resolve, reject) => {
		server.once('error', reject);
		server.listen(opts.port ?? 0, host, () => resolve());
	});
	const port = (server.address() as AddressInfo).port;
	const shown = host === '::1' ? `[::1]:${port}` : `${host}:${port}`;
	hosts = new Set([shown, `localhost:${port}`]);
	return {
		url: `http://${shown}/?key=${key}`,
		async close() {
			for (const s of streams) s.end();
			await new Promise<void>((resolve) => server.close(() => resolve()));
			server.closeAllConnections?.();
			await Promise.allSettled([...pending]);
		},
	};
}

const HEADERS = {
	'x-content-type-options': 'nosniff',
	'referrer-policy': 'no-referrer',
	'cache-control': 'no-store',
	'x-frame-options': 'DENY',
};

function page(res: ServerResponse): void {
	res.writeHead(200, {
		...HEADERS,
		'content-type': 'text/html; charset=utf-8',
		// Nothing from outside: the page's own script and style only.
		'content-security-policy': "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
	});
	res.end(PAGE);
}

function send(res: ServerResponse, status: number, body: unknown): void {
	res.writeHead(status, { ...HEADERS, 'content-type': 'application/json; charset=utf-8' });
	res.end(JSON.stringify(body));
}

function param(url: URL, name: string): string {
	const v = url.searchParams.get(name);
	if (!v) throw new KairoError(400, `${name} is required`);
	return v;
}

function str(v: unknown): string {
	if (typeof v !== 'string' || !v) throw new KairoError(400, 'a required field is missing');
	return v;
}

async function json(req: IncomingMessage): Promise<Record<string, any>> {
	const chunks: Buffer[] = [];
	let size = 0;
	for await (const c of req) {
		size += (c as Buffer).length;
		if (size > MAX_BODY) throw new KairoError(413, 'request too large');
		chunks.push(c as Buffer);
	}
	const v = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
	if (!v || typeof v !== 'object' || Array.isArray(v)) throw new KairoError(400, 'a JSON object is required');
	return v;
}

function message(e: unknown): string {
	return e instanceof Error ? e.message : String(e);
}
