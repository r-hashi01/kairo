// Workflows written as code (ADR 0049). A workflow is a function that runs
// in this process; each call it makes (an action, a wait, a clock read)
// is a run of its own in kairo, whose id comes from the call: running the
// function again with the same workflow id returns the calls that
// finished, and runs nothing twice.

import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';

import { HttpBackend, type Backend } from './backend.ts';
import { SignatureError, checkURL, post, sign, verify } from './http.ts';
import { KairoError, finished, type EffectName, type NodeSpec, type RunInfo } from './client.ts';
import type { Result, Task } from './protocol.ts';
import type { Address, TaskContext } from './worker.ts';

/** An action: what a step of a workflow runs, on this process's worker. */
export interface ActionDef<I = any, O = any> {
	/** "real" (the default) acts outside and never runs twice; "unprotected" may run again. */
	effect?: EffectName;
	handler: (input: I, ctx: TaskContext) => Promise<O>;
	/** How long a call may run before its outcome is unknown (e.g. "5m"). */
	timeout?: string;
	/** The rate-limit key (default: the action). */
	destination?: string;
	/**
	 * Where the action runs (ADR 0052): its steps are called over HTTP(S)
	 * at this URL, and the handler runs there (fetchHandler). https, or
	 * http to this machine, or http with allowInsecure.
	 */
	url?: string;
	/**
	 * Where the URL serves it: answer at once (202) and run the handler
	 * after, sending its outcome to the callback. For long actions.
	 */
	async?: boolean;
	allowInsecure?: boolean;
}

export type WorkflowFn<I = any, O = any> = (ctx: Context, input: I) => Promise<O>;

export interface KairoOptions {
	/**
	 * Where calls run: kairod (HttpBackend, the default, from url and
	 * worker) or the runtime embedded here (EmbeddedBackend, ADR 0051).
	 */
	backend?: Backend;
	/** kairod's HTTP API. */
	url?: string;
	/** kairod's worker socket (a UNIX socket path, or {host, port}). */
	worker?: Address;
	token?: string;
	tenant?: string;
	/** kairod's -idempotency-ttl: a workflow older than this is not resumed. */
	idempotencyTTL?: number;
	/** Tasks at once on this process's worker. */
	concurrency?: number;
	/**
	 * "wait" (the default): run() drives a workflow to its end, waiting in
	 * this process for timers and signals. "suspend" (serverless, embedded
	 * only): run() goes as far as it can now and throws Suspended at a call
	 * that waits for a timer or a signal; tick() and signal() drive the
	 * workflows on when their calls settle (ADR 0051).
	 */
	mode?: 'wait' | 'suspend';
	/** Actions over HTTP(S) (ADR 0052): the secret both sides sign with. */
	secret?: string;
	/** Actions over HTTP(S): where this process takes outcomes (fetchHandler's /callback). */
	callbackUrl?: string;
	/** Certificates to trust (PEM) for actions and callbacks over https, besides the system's. */
	ca?: string | Buffer | Array<string | Buffer>;
	/** Allows plain http to other machines (callbackUrl, and actions that do not say otherwise). */
	allowInsecure?: boolean;
	/**
	 * Keeps the platform running work after a response (e.g. Vercel's or
	 * Cloudflare's waitUntil): used for async actions served by fetchHandler.
	 */
	waitUntil?: (p: Promise<unknown>) => void;
	/**
	 * Suspend mode (ADR 0053): the bearer token a scheduler calls
	 * fetchHandler's /tick with. Without it, /tick is not served.
	 */
	tickSecret?: string;
	/**
	 * Suspend mode (ADR 0053): told, before run, tick, signal and callbacks
	 * return, when something is next to do (unix ms): schedule a one-off
	 * call of /tick (or tick()) then, instead of polling.
	 */
	wake?: (at: number) => Promise<void>;
}

/** The workflow ran into a call whose result kairo no longer keeps. */
export class ResultLostError extends Error {}
/** The workflow was cancelled. */
export class CancelledError extends Error {}
/** The workflow waits (a timer, a signal): tick() or signal() drives it on. */
export class Suspended extends Error {}

const PLAN_CALL = 'kairo.call/';
const PLAN_WAIT = 'kairo.wait/';
const PLAN_WORKFLOW = 'kairo.workflow';
const BUILTIN = { now: 'kairo.now', random: 'kairo.random', sleep: 'kairo.sleep' };

/** Whether header is "Bearer <token>", compared in constant time. */
function bearer(header: string | null, token: string): boolean {
	const digest = (s: string) => createHash('sha256').update(s).digest();
	return timingSafeEqual(digest(header ?? ''), digest(`Bearer ${token}`));
}

/**
 * A function's entry for a scheduler that calls it, not over HTTP (e.g.
 * AWS Lambda from EventBridge Scheduler, ADR 0053): each call makes a
 * Kairo (started, suspend mode), ticks, and closes it. Returns when
 * something is next to do.
 */
export function tickHandler(make: () => Promise<Kairo>): () => Promise<{ next: number | null }> {
	return async () => {
		const k = await make();
		try {
			return { next: await k.tick() };
		} finally {
			await k.close();
		}
	};
}

/** A result as actions over HTTP(S) send it (ADR 0052). */
interface Wire {
	output?: unknown;
	error?: string;
	retryable?: boolean;
	error_type?: string;
	wait?: { until: number; output?: unknown };
}

function fromWire(w: Wire): Result {
	if (typeof w !== 'object' || w === null) throw new Error('not a result');
	if (w.error !== undefined) return { error: String(w.error), retryable: !!w.retryable, ...(w.error_type ? { errorType: w.error_type } : {}) };
	if (w.wait) return { wait: { until: Number(w.wait.until), output: w.wait.output ?? null } };
	return { output: w.output ?? null };
}

/** Milliseconds of a duration such as "300ms", "5s", "5m", "1h". */
function durationMs(d: string | undefined): number | undefined {
	const m = d ? /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(d) : null;
	if (!m) return undefined;
	return Number(m[1]) * { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[m[2] as 'ms' | 's' | 'm' | 'h'];
}

/** JSON with object keys sorted: equal values, equal text. */
function canonical(v: unknown): string {
	if (Array.isArray(v)) return '[' + v.map(canonical).join(',') + ']';
	if (v && typeof v === 'object') {
		const o = v as Record<string, unknown>;
		return '{' + Object.keys(o).filter((k) => o[k] !== undefined).sort().map((k) => JSON.stringify(k) + ':' + canonical(o[k])).join(',') + '}';
	}
	return JSON.stringify(v ?? null);
}

export class Kairo {
	readonly backend: Backend;
	private readonly opts: KairoOptions;
	private readonly actions = new Map<string, ActionDef>();
	private readonly workflows = new Map<string, WorkflowFn>();
	private readonly planned = new Set<string>();
	private readonly driving = new Set<AbortController>();
	readonly suspend: boolean;
	/** Suspend mode: workflows being driven on here, and those to drive again after. */
	private readonly drivingIds = new Set<string>();
	private readonly again = new Set<string>();
	private readonly redrives = new Set<Promise<void>>();
	private stopHook?: () => void;

	constructor(opts: KairoOptions = {}) {
		this.opts = opts;
		this.backend = opts.backend ?? new HttpBackend({ url: opts.url, worker: opts.worker, token: opts.token, concurrency: opts.concurrency });
		this.suspend = opts.mode === 'suspend';
		if (this.suspend && !this.backend.idle) throw new Error('suspend mode needs the embedded backend');
	}

	defineAction<I, O>(name: string, def: ActionDef<I, O>): void {
		if (name.startsWith('kairo.')) throw new Error(`action ${name}: names beginning with "kairo." are kairo's`);
		if (def.url) checkURL(def.url, def.allowInsecure ?? this.opts.allowInsecure);
		this.actions.set(name, def);
	}

	workflow<I, O>(name: string, fn: WorkflowFn<I, O>): void {
		this.workflows.set(name, fn);
	}

	/** Registers the actions and their plans, and starts running the actions' steps. */
	async start(): Promise<void> {
		const specs: NodeSpec[] = [...this.actions].map(([action, def]) => ({
			action,
			effect: def.effect ?? 'real',
			...(def.timeout ? { timeout: def.timeout } : {}),
			...(def.destination ? { destination: def.destination } : {}),
		}));
		for (const action of Object.values(BUILTIN)) specs.push({ action, effect: 'unprotected' });
		if ([...this.actions.values()].some((d) => d.url)) {
			// Actions over HTTP(S) (ADR 0052) take their outcomes on the callback.
			if (!this.opts.secret || !this.opts.callbackUrl) throw new Error('actions with a url need secret and callbackUrl');
			checkURL(this.opts.callbackUrl, this.opts.allowInsecure);
			if (!this.backend.complete) throw new Error('actions with a url need the embedded backend');
		}
		await this.backend.start(specs, (task, ctx) => this.serve(task, ctx));
		for (const { action } of specs) await this.plan(PLAN_CALL + action, { kind: 'step', id: 'call', action, input: { in: '$input.in' } });
		await this.plan(PLAN_WORKFLOW, { kind: 'wait', id: 'done', signal: 'done' }, { started_at: { type: 'integer', value: 0 } });
		if (this.suspend) {
			// A call that settles drives its workflow on (its id is the
			// workflow's id, "/", the call's key).
			this.stopHook = this.backend.onSettled!((r) => {
				const i = r.run_id.lastIndexOf('/');
				if (i > 0) this.redrive(r.run_id.slice(0, i));
			});
		}
	}

	/**
	 * Stops serving actions and driving workflows, as a process that stops
	 * does: the workflows it drove stay unfinished in kairo, to be resumed.
	 */
	async close(): Promise<void> {
		this.stopHook?.();
		for (const a of this.driving) a.abort();
		await this.backend.close();
	}

	/**
	 * Suspend mode: takes up due timers and steps whose process stopped,
	 * drives on the workflows whose calls settle, and returns once that is
	 * done (call it from a scheduler). Returns when something is next to
	 * do (unix ms), or null.
	 */
	async tick(): Promise<number | null> {
		await this.backend.tick?.();
		await this.settle();
		return this.wakeUp();
	}

	/** Suspend mode: tells wake when something is next to do (ADR 0053); returns it. */
	private async wakeUp(): Promise<number | null> {
		if (!this.suspend) return null;
		const at = (await this.backend.nextWake?.()) ?? null;
		if (at !== null) await this.opts.wake?.(at);
		return at;
	}

	/** Returns once the work started here and the workflows driven on are done. */
	private async settle(): Promise<void> {
		for (;;) {
			await this.backend.idle?.();
			if (this.redrives.size === 0) {
				await this.backend.idle?.();
				if (this.redrives.size === 0) return;
			}
			await Promise.allSettled([...this.redrives]);
		}
	}

	/** Suspend mode: drives workflow id on (again, if it is being driven now). */
	private redrive(id: string): void {
		if (this.drivingIds.has(id)) {
			this.again.add(id);
			return;
		}
		const p = (async () => {
			do {
				this.again.delete(id);
				let info: RunInfo;
				try {
					info = await this.backend.get(id);
				} catch {
					return;
				}
				const input = info.input as { workflow?: string; input?: unknown } | undefined;
				if (info.plan !== PLAN_WORKFLOW || finished(info) || !input?.workflow) return;
				try {
					await this.runAs(input.workflow, input.input, id);
				} catch {
					// Suspended again, failed (recorded), or cancelled.
				}
			} while (this.again.has(id));
		})();
		this.redrives.add(p);
		void p.finally(() => this.redrives.delete(p));
	}

	private async plan(name: string, root: unknown, vars?: Record<string, unknown>): Promise<void> {
		if (this.planned.has(name)) return;
		await this.backend.registerPlan({ name, root, ...(vars ? { vars } : {}) });
		this.planned.add(name);
	}

	private async serve(task: Task, ctx: TaskContext): Promise<Result> {
		const input = (task.input as { in?: unknown } | null)?.in;
		switch (task.action) {
			case BUILTIN.now:
				return { output: Date.now() };
			case BUILTIN.random:
				return { output: Math.random() };
			case BUILTIN.sleep:
				// Waits in kairo, not here (ADR 0045).
				return { wait: { until: Date.now() + Number((input as { ms: number }).ms), output: null } };
		}
		const def = this.actions.get(task.action);
		if (!def) return { error: `no action ${task.action} here` };
		if (def.url) return this.callRemote(task, def, input);
		return { output: (await def.handler(input, ctx)) ?? null };
	}

	/** Calls an action over HTTP(S) (ADR 0052). */
	private async callRemote(task: Task, def: ActionDef, input: unknown): Promise<Result> {
		const body = JSON.stringify({
			run_id: task.run_id,
			step_id: task.step_id,
			act: task.act,
			attempt: task.attempt,
			action: task.action,
			input: input ?? null,
			idempotency_key: task.idempotency_key,
			callback: this.opts.callbackUrl,
		});
		const leaseMs = durationMs(def.timeout) ?? 15 * 60 * 1000;
		let res;
		try {
			res = await post(def.url!, body, {
				ca: this.opts.ca,
				timeoutMs: Math.min(leaseMs, 60_000),
				headers: { 'idempotency-key': task.idempotency_key, 'kairo-signature': sign(this.opts.secret!, body) },
			});
		} catch (e) {
			// It may have run: unknown (invariant 5).
			return { error: `calling ${def.url}: ${(e as Error).message}`, unknown: true, retryable: true, errorType: 'http' };
		}
		if (res.status === 202) return { pending: { owner: `remote:${def.url}`, leaseMs } };
		if (res.status >= 200 && res.status < 300) {
			try {
				return fromWire(JSON.parse(res.body));
			} catch {
				return { error: `${def.url}: the answer is not JSON`, unknown: true, retryable: true, errorType: 'http' };
			}
		}
		if (res.status >= 400 && res.status < 500) return { error: `${def.url}: ${res.status} ${res.body.slice(0, 200)}`, errorType: `http_${res.status}` };
		return { error: `${def.url}: ${res.status}`, unknown: true, retryable: true, errorType: `http_${res.status}` };
	}

	/**
	 * Serves actions called over HTTP(S) and takes their outcomes (ADR
	 * 0052), as a fetch-style handler: POST <base>/action runs an action's
	 * handler here; POST <base>/callback applies an outcome to its run.
	 * Both are signed with secret. GET or POST <base>/tick, with
	 * "Authorization: Bearer <tickSecret>", is a scheduler's tick (ADR
	 * 0053): it answers {"next": <unix ms> | null}.
	 */
	fetchHandler(): (req: Request) => Promise<Response> {
		const json = (status: number, v: unknown) => new Response(JSON.stringify(v), { status, headers: { 'content-type': 'application/json' } });
		return async (req) => {
			if (new URL(req.url).pathname.endsWith('/tick')) {
				if (!this.opts.tickSecret || !this.suspend) return json(404, { error: 'no such path' });
				if (req.method !== 'GET' && req.method !== 'POST') return json(405, { error: 'GET or POST' });
				if (!bearer(req.headers.get('authorization'), this.opts.tickSecret)) return json(401, { error: 'bad token' });
				return json(200, { next: await this.tick() });
			}
			if (req.method !== 'POST') return json(405, { error: 'POST only' });
			if (!this.opts.secret) return json(500, { error: 'no secret configured' });
			const body = await req.text();
			try {
				verify(this.opts.secret, body, req.headers.get('kairo-signature'));
			} catch (e) {
				if (e instanceof SignatureError) return json(401, { error: e.message });
				throw e;
			}
			const path = new URL(req.url).pathname;
			const m = JSON.parse(body);
			if (path.endsWith('/callback')) {
				if (!this.backend.complete) return json(501, { error: 'no embedded runtime here' });
				await this.backend.complete(m.run_id, m.act, m.attempt, fromWire(m.result));
				if (this.suspend) {
					await this.settle();
					await this.wakeUp();
				}
				return json(200, { ok: true });
			}
			if (!path.endsWith('/action')) return json(404, { error: 'no such path' });
			const def = this.actions.get(m.action);
			if (!def) return json(404, { error: `no action ${m.action} here` });
			const run = async (): Promise<Wire> => {
				try {
					return { output: (await def.handler(m.input, { signal: new AbortController().signal, emit: () => {} })) ?? null };
				} catch (e) {
					return { error: String((e as Error)?.message ?? e), error_type: (e as Error)?.name };
				}
			};
			if (!def.async) return json(200, await run());
			// Answer now; run after, and send the outcome to the callback.
			const work = (async () => {
				const result = await run();
				const cb = JSON.stringify({ run_id: m.run_id, act: m.act, attempt: m.attempt, result });
				checkURL(m.callback, this.opts.allowInsecure);
				await post(m.callback, cb, { ca: this.opts.ca, headers: { 'kairo-signature': sign(this.opts.secret!, cb) } });
			})().catch(() => {}); // a lost callback: the lease expires and the step is taken up
			this.opts.waitUntil?.(work);
			return json(202, { accepted: true });
		};
	}

	/**
	 * Runs workflow name as execution id, or resumes it: calls that
	 * finished return their recorded results. Returns its result.
	 */
	async run<O = any>(name: string, input: unknown, opts: { id?: string } = {}): Promise<O> {
		try {
			return (await this.runAs(name, input, opts.id ?? randomUUID())) as O;
		} finally {
			await this.wakeUp();
		}
	}

	/** Sends a signal to the first wait for it in workflow id that has not received one. */
	async signal(id: string, name: string, payload: unknown = null): Promise<void> {
		// The waits' plan, which this process may not have needed yet.
		await this.plan(PLAN_WAIT + name, waitRoot(name));
		for (let n = 0; ; n++) {
			const runId = callId(id, PLAN_WAIT + name, null, n);
			let r: RunInfo;
			try {
				r = await this.backend.get(runId);
			} catch (e) {
				if (e instanceof KairoError && e.status === 404) throw new Error(`workflow ${id} does not wait for ${name}`);
				throw e;
			}
			if (finished(r)) continue;
			await this.backend.signal(runId, name, payload);
			if (this.suspend) {
				await this.settle();
				await this.wakeUp();
			}
			return;
		}
	}

	/** Cancels workflow id: the calls it is waiting for are cancelled with it. */
	async cancel(id: string): Promise<void> {
		await this.backend.cancel(id);
	}

	private async runAs(name: string, input: unknown, id: string, parent?: AbortSignal): Promise<unknown> {
		const fn = this.workflows.get(name);
		if (!fn) throw new Error(`no workflow ${name}`);
		await this.plan(PLAN_WORKFLOW, { kind: 'wait', id: 'done', signal: 'done' }, { started_at: { type: 'integer', value: 0 } });
		const started = await this.backend.run(PLAN_WORKFLOW, { workflow: name, input: input ?? null }, { runId: id, vars: { started_at: Date.now() } });
		const info = await this.backend.get(id);
		if (finished(info)) return done(id, info);
		if (started.existing) {
			// Resuming: the calls' records must still be there (ADR 0049).
			const at = Number((info.vars as { started_at?: number } | undefined)?.started_at ?? 0);
			const ttl = this.opts.idempotencyTTL ?? 24 * 3600 * 1000;
			if (at > 0 && Date.now() - at > ttl) {
				throw new ResultLostError(`workflow ${id} started more than ${ttl}ms ago: its calls' records may be gone`);
			}
		}
		const abort = new AbortController();
		this.driving.add(abort);
		const stop = () => abort.abort();
		parent?.addEventListener('abort', stop);
		if (!this.suspend) {
			// The workflow's own run ends when it is cancelled: stop the calls.
			// (Suspended, a cancelled workflow stops when it is driven next.)
			void this.backend.wait(id, abort.signal).then((r) => {
				if (r.status === 'cancelled') abort.abort();
			}, () => {});
		}
		this.drivingIds.add(id);
		const ctx = new Context(this, id, abort.signal);
		try {
			const value = await fn(ctx, input);
			await this.backend.signal(id, 'done', { ok: true, value: value ?? null });
			return value;
		} catch (e) {
			if (e instanceof Suspended) throw e; // goes on later
			if (abort.signal.aborted) throw new CancelledError(`workflow ${id} cancelled`);
			await this.backend.signal(id, 'done', { ok: false, error: String((e as Error)?.message ?? e) }).catch(() => {});
			throw e;
		} finally {
			parent?.removeEventListener('abort', stop);
			this.driving.delete(abort);
			this.drivingIds.delete(id);
			abort.abort();
		}
	}

	/** One call: a run of its own, found again by its id. */
	async callRun(plan: string, root: unknown | null, input: unknown, runId: string, signal: AbortSignal): Promise<unknown> {
		if (root) await this.plan(plan, root);
		await this.backend.run(plan, { in: input }, { runId });
		let r: RunInfo;
		if (this.suspend) {
			// Whatever this process can do for the call is done once it is
			// idle; a call still going then waits for a timer or a signal.
			await this.backend.idle!();
			r = await this.backend.get(runId);
			if (!finished(r) && r.status !== 'blocked') throw new Suspended(`call ${runId} waits`);
		} else {
			try {
				r = await this.backend.wait(runId, signal);
			} catch (e) {
				if (signal.aborted) {
					await this.backend.cancel(runId).catch(() => {});
					throw new CancelledError(`call ${runId} cancelled`);
				}
				throw e;
			}
		}
		if (r.trimmed) throw new ResultLostError(`call ${runId} finished, but kairo no longer keeps its result`);
		if (r.status !== 'completed') throw new Error(`call ${runId} ${r.status}: ${r.error ?? ''}`);
		return r.output;
	}

	async childWorkflow(name: string, input: unknown, id: string, signal: AbortSignal): Promise<unknown> {
		return this.runAs(name, input, id, signal);
	}
}

/** The plan of a wait for signal name. */
function waitRoot(name: string) {
	return { kind: 'wait', id: 'w', signal: name };
}

/** The result of a finished workflow run. */
function done(id: string, info: RunInfo): unknown {
	if (info.trimmed) throw new ResultLostError(`workflow ${id} finished, but kairo no longer keeps its result`);
	if (info.status === 'cancelled') throw new CancelledError(`workflow ${id} cancelled`);
	const out = (info.output as { payload?: { ok: boolean; value?: unknown; error?: string } } | undefined)?.payload;
	if (!out) throw new Error(`workflow ${id} ${info.status}: ${info.error ?? ''}`);
	if (!out.ok) throw new Error(out.error);
	return out.value;
}

/** A call's run id: the workflow's, the call's kind and input, and how many such calls came before. */
function callId(workflow: string, kind: string, input: unknown, n: number): string {
	const h = createHash('sha256').update(kind + '\0' + canonical(input)).digest('hex').slice(0, 24);
	return `${workflow}/${h}.${n}`;
}

/** What a workflow function calls. */
export class Context {
	readonly id: string;
	readonly signal: AbortSignal;
	private readonly k: Kairo;
	private readonly seen = new Map<string, number>();

	constructor(k: Kairo, id: string, signal: AbortSignal) {
		this.k = k;
		this.id = id;
		this.signal = signal;
	}

	private next(kind: string, input: unknown): string {
		const key = kind + '\0' + canonical(input);
		const n = this.seen.get(key) ?? 0;
		this.seen.set(key, n + 1);
		return callId(this.id, kind, input, n);
	}

	/** Runs action with input (once, however often the workflow runs again). */
	async call<O = any>(action: string, input: unknown = null): Promise<O> {
		const id = this.next(PLAN_CALL + action, input);
		return (await this.k.callRun(PLAN_CALL + action, null, input, id, this.signal)) as O;
	}

	/** Runs the calls at once; their results in order. */
	parallel<T extends readonly (() => Promise<unknown>)[]>(fns: T): Promise<{ [K in keyof T]: Awaited<ReturnType<T[K]>> }> {
		return Promise.all(fns.map((f) => f())) as any;
	}

	/** Waits for signal name (see Kairo.signal); returns its payload. */
	async waitFor<P = any>(name: string): Promise<P> {
		const id = this.next(PLAN_WAIT + name, null);
		const out = (await this.k.callRun(PLAN_WAIT + name, waitRoot(name), null, id, this.signal)) as {
			payload: P;
		};
		return out.payload;
	}

	/** Waits ms milliseconds, in kairo (the process may stop meanwhile). */
	async sleep(ms: number): Promise<void> {
		await this.call(BUILTIN.sleep, { ms });
	}

	/** The time (unix ms), the same each time the workflow runs again. */
	now(): Promise<number> {
		return this.call(BUILTIN.now);
	}

	/** A random number in [0, 1), the same each time the workflow runs again. */
	random(): Promise<number> {
		return this.call(BUILTIN.random);
	}

	/** Runs workflow name as a child of this one. */
	async workflow<O = any>(name: string, input: unknown = null): Promise<O> {
		const id = this.next('kairo.workflow/' + name, input);
		return (await this.k.childWorkflow(name, input, id, this.signal)) as O;
	}
}
