// Actions called over HTTP(S) (ADR 0052): signing, posting, and checking
// where a request may go. No dependency: node:http(s) and node:crypto.

import { createHmac, timingSafeEqual } from 'node:crypto';
import { request as httpRequest, type IncomingMessage, type ServerResponse } from 'node:http';
import { request as httpsRequest } from 'node:https';

/** How old a signed request may be (ms). */
export const SIGNATURE_TOLERANCE = 5 * 60 * 1000;

export class SignatureError extends Error {}

/** The Kairo-Signature header of body at time t: "t=<unix ms>,v1=<hex HMAC-SHA256(secret, t + '.' + body)>". */
export function sign(secret: string, body: string, t = Date.now()): string {
	return `t=${t},v1=${createHmac('sha256', secret).update(`${t}.${body}`).digest('hex')}`;
}

/** Throws SignatureError unless header signs body with secret, recently. */
export function verify(secret: string, body: string, header: string | null, now = Date.now()): void {
	const parts = Object.fromEntries((header ?? '').split(',').map((p) => p.split('=', 2) as [string, string]));
	const t = Number(parts.t);
	if (!parts.v1 || !Number.isFinite(t)) throw new SignatureError('missing signature');
	if (Math.abs(now - t) > SIGNATURE_TOLERANCE) throw new SignatureError('signature too old');
	const want = Buffer.from(createHmac('sha256', secret).update(`${t}.${body}`).digest('hex'));
	const got = Buffer.from(parts.v1);
	if (got.length !== want.length || !timingSafeEqual(got, want)) throw new SignatureError('bad signature');
}

const LOCAL = new Set(['localhost', '127.0.0.1', '[::1]', '::1']);

/**
 * Checks where a request may go: https, or http to this machine, or http
 * when allowInsecure says so (ADR 0052).
 */
export function checkURL(url: string, allowInsecure = false): URL {
	let u: URL;
	try {
		u = new URL(url);
	} catch {
		throw new Error(`${url}: not a URL`);
	}
	if (u.protocol === 'https:') return u;
	if (u.protocol === 'http:' && (LOCAL.has(u.hostname) || allowInsecure)) return u;
	if (u.protocol === 'http:') throw new Error(`${url}: plain http is only for this machine; use https, or allowInsecure`);
	throw new Error(`${url}: only http(s)`);
}

export interface PostOptions {
	/** Certificates to trust (PEM), besides the system's. */
	ca?: string | Buffer | Array<string | Buffer>;
	timeoutMs?: number;
	headers?: Record<string, string>;
}

export interface PostResponse {
	status: number;
	body: string;
}

/** POSTs a JSON body; rejects on a network error or a timeout. */
export function post(url: string, body: string, opts: PostOptions = {}): Promise<PostResponse> {
	const u = new URL(url);
	const req = u.protocol === 'https:' ? httpsRequest : httpRequest;
	return new Promise((resolve, reject) => {
		const r = req(
			u,
			{
				method: 'POST',
				headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), ...opts.headers },
				...(u.protocol === 'https:' && opts.ca ? { ca: opts.ca } : {}),
				timeout: opts.timeoutMs ?? 30_000,
			},
			(res) => {
				const chunks: Buffer[] = [];
				res.on('data', (c: Buffer) => chunks.push(c));
				res.on('end', () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString('utf8') }));
				res.on('error', reject);
			},
		);
		r.on('timeout', () => r.destroy(new Error('timeout')));
		r.on('error', reject);
		r.end(body);
	});
}

/** Serves a fetch-style handler (Request -> Response) from node:http(s). */
export function nodeHandler(handler: (req: Request) => Promise<Response>) {
	return (req: IncomingMessage, res: ServerResponse) => {
		const chunks: Buffer[] = [];
		req.on('data', (c: Buffer) => chunks.push(c));
		req.on('end', async () => {
			try {
				const headers = new Headers();
				for (const [k, v] of Object.entries(req.headers)) if (typeof v === 'string') headers.set(k, v);
				const body = Buffer.concat(chunks);
				const r = await handler(
					new Request(`http://${req.headers.host ?? 'localhost'}${req.url ?? '/'}`, {
						method: req.method,
						headers,
						...(body.length && req.method !== 'GET' ? { body } : {}),
					}),
				);
				res.writeHead(r.status, Object.fromEntries(r.headers));
				res.end(Buffer.from(await r.arrayBuffer()));
			} catch (e) {
				res.writeHead(500);
				res.end(String((e as Error)?.message ?? e));
			}
		});
	};
}
