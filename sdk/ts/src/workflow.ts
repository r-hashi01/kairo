// Workflows written as code (ADR 0049). A workflow is a function that runs
// in this process; each call it makes (an action, a wait, a clock read)
// is a run of its own in kairo, whose id comes from the call: running the
// function again with the same workflow id returns the calls that
// finished, and runs nothing twice.

import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';

import { HttpBackend, type Backend } from './backend.ts';
import { SignatureError, checkURL, post, sign, verify } from './http.ts';
import { KairoError, finished, type EffectName, type NodeSpec, type RunInfo } from './client.ts';
import { Embedded, newToken } from './embedded.ts';
import type { Result, Task } from './protocol.ts';
import { consoleLogger, observe, stepStatus, type Logger, type Observer } from './observe.ts';
import type { LeaseRow } from './store.ts';
import type { Address, TaskContext } from './worker.ts';

/** An action: what a step of a workflow runs, on this process's worker. */
export interface ActionDef<I = any, O = any> {
	/** "real" (the default) acts outside and never runs twice; "unprotected" may run again. */
	effect?: EffectName;
	handler: (input: I, ctx: TaskContext) => Promise<O>;
	/** How long a call may run before its outcome is unknown (e.g. "5m"). */
	timeout?: string;
	/** The rate-limit key (default: the action). Actions of one destination share limit and rate: the strictest. */
	destination?: string;
	/**
	 * A step of the action is tried at most this many times when it fails
	 * retryably (RetryableError; default 3, or 1 for a real action: such a
	 * step is retried only when it did not take effect).
	 */
	maxAttempts?: number;
	/** The wait before a step's second attempt, doubled for each one after (e.g. "10ms"; default "200ms"). */
	backoff?: string;
	/** At most this many steps of the action (of its destination) run in this process at once (ADR 0059). */
	limit?: number;
	/** At most this many steps of the action (of its destination) start in this process a minute, spaced evenly (ADR 0059). */
	rate?: number;
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

/** A workflow's version (ADR 0060). */
export interface WorkflowOptions {
	/**
	 * This function's version of the workflow. A run is driven by the
	 * version it started with, to its end: a process drives only the runs
	 * whose version it has. Default "": none.
	 */
	version?: string;
	/** This version only finishes the runs that started with it; new runs start with the current one (the one not draining). */
	draining?: boolean;
}

/** A workflow's versions (ADR 0060): the current one starts new runs; the others only finish the runs they started. */
interface WorkflowDef {
	versions: Map<string, WorkflowFn>;
	current: string;
	currents: number;
}

/** A workflow run's input: the workflow, its version (when it has one) and the input it was given. */
function workflowInput(name: string, version: string, input: unknown): Record<string, unknown> {
	return { workflow: name, input: input ?? null, ...(version ? { version } : {}) };
}

/** Reads a workflow run's input. */
function workflowOf(input: unknown): { name: string; version: string; input: unknown } | undefined {
	const w = input as { workflow?: unknown; version?: unknown; input?: unknown } | null | undefined;
	if (!w || typeof w.workflow !== 'string' || !w.workflow) return undefined;
	return { name: w.workflow, version: typeof w.version === 'string' ? w.version : '', input: w.input };
}

/** A workflow run whose version this process does not have (another process's to drive). */
class NotHereError extends KairoError {
	override name = 'NotHereError';
}

function notHere(name: string, version: string): NotHereError {
	return new NotHereError(404, `workflow ${name} version ${JSON.stringify(version)} is not registered here`);
}

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
	/**
	 * Steps at once in this process: on kairod's worker, or in the embedded
	 * runtime (ADR 0059; kairo.now, kairo.random and kairo.sleep are not
	 * counted). Steps over it wait, leased, for a slot. Default: no limit.
	 */
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
	/**
	 * Takes what goes wrong in the runtime's background work: lease
	 * renewals, sweeps, timers, steps' outcomes, workflows driven in the
	 * background, callbacks (default: the console).
	 */
	logger?: Logger;
	/**
	 * Given each Observation as it happens: runs that start and settle,
	 * steps' attempts that start and finish (to count, time and trace them).
	 * It is called in the runtime, as it works: it must not block. With
	 * kairod (HttpBackend), only the steps this process's worker runs.
	 */
	observe?: Observer;
}

/** The workflow ran into a call whose result kairo no longer keeps. */
export class ResultLostError extends Error {}
/** The workflow was cancelled. */
export class CancelledError extends Error {}
/** The workflow waits (a timer, a signal): tick() or signal() drives it on. */
export class Suspended extends Error {}
/**
 * The caller stopped waiting (its signal aborted), or the process is
 * closing. The workflow is not cancelled: it goes on (ADR 0059).
 */
export class StoppedError extends Error {}
/** A wait's timeout came before its signal (waitFor's timeout, ADR 0059). */
export class TimedOutError extends Error {}
/** Another process holds the workflow's drive lease (ADR 0059). */
class DrivenElsewhere extends Error {}

/** Thrown by an action's handler: a failure that may be retried (it did not take effect). */
export class RetryableError extends Error {
	override name = 'RetryableError';
}

/**
 * Thrown by an action's handler: its outcome is unknown (it may have taken
 * effect). Never taken for success (invariant 5): a real step stops for
 * review, an unprotected one is retried.
 */
export class UnknownOutcomeError extends Error {
	override name = 'UnknownOutcomeError';
}

/**
 * A call that did not complete: it failed (its action's failure, after its
 * retries) or was cancelled (then its cause is a CancelledError).
 */
export class CallError extends Error {
	override name = 'CallError';
	/** The call's run. */
	readonly runId: string;
	/** The action called ("" for a wait). */
	readonly action: string;
	/** "failed" or "cancelled". */
	readonly status: string;
	/** The failure as recorded ("" when cancelled). */
	readonly error: string;

	constructor(runId: string, action: string, status: string, error = '') {
		const what = action ? `${runId} (${action})` : runId;
		super(`call ${what} ${status}${error ? `: ${error}` : ''}`, status === 'cancelled' ? { cause: new CancelledError(`call ${runId} cancelled`) } : undefined);
		this.runId = runId;
		this.action = action;
		this.status = status;
		this.error = error;
	}
}

/**
 * A workflow that failed: its function threw (cause: what it threw, when
 * it failed in this process). error is what is recorded.
 */
export class WorkflowError extends Error {
	override name = 'WorkflowError';
	readonly id: string;
	readonly error: string;

	constructor(id: string, error: string, opts?: { cause?: unknown }) {
		super(`workflow ${id} failed: ${error}`, opts);
		this.id = id;
		this.error = error;
	}
}

/** The result of a handler that threw. */
function failure(e: unknown): Result {
	const error = String((e as Error)?.message ?? e);
	const errorType = (e as Error)?.name;
	if (e instanceof UnknownOutcomeError) return { error, unknown: true, retryable: true, errorType };
	return { error, ...(e instanceof RetryableError ? { retryable: true } : {}), ...(errorType ? { errorType } : {}) };
}

/** A result as an action over HTTP(S) answers it (ADR 0052): an unknown outcome has no 200 body. */
function toWire(r: Result): { wire: Wire; unknown: boolean } {
	if (r.unknown) return { wire: { error: r.error ?? '', ...(r.errorType ? { error_type: r.errorType } : {}) }, unknown: true };
	if (r.error !== undefined) return { wire: { error: r.error, ...(r.retryable ? { retryable: true } : {}), ...(r.errorType ? { error_type: r.errorType } : {}) }, unknown: false };
	return { wire: { output: r.output ?? null }, unknown: false };
}

/** The action a call's plan calls ("" for a wait). */
function calledAction(plan: string): string {
	return plan.startsWith(PLAN_CALL) ? plan.slice(PLAN_CALL.length) : '';
}

/** What list selects (ADR 0059): workflows of workflow, in status, created in [since, until), after the run after, at most limit. */
export interface ListOptions {
	workflow?: string;
	status?: string;
	since?: number | Date;
	until?: number | Date;
	after?: string;
	limit?: number;
}

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
	private readonly workflows = new Map<string, WorkflowDef>();
	private readonly planned = new Set<string>();
	private readonly driving = new Set<AbortController>();
	readonly suspend: boolean;
	/** Suspend mode: workflows being driven on here, and those to drive again after. */
	private readonly drivingIds = new Set<string>();
	private readonly again = new Set<string>();
	private readonly redrives = new Set<Promise<void>>();
	private stopHook?: () => void;
	/** Closing: calls interrupted now go on in kairo, to be resumed (not cancelled). */
	private closing = false;
	/** The embedded runtime, when that is the backend (drive leases, ADR 0059). */
	private readonly rt?: Embedded;
	/** Wait mode: workflows driven in the background here (by id), and their drives. */
	private readonly owned = new Set<string>();
	private readonly background = new Set<Promise<void>>();
	/** Ends with the process (close): what waits in it stops. */
	private readonly life = new AbortController();
	/** Wait mode: the sweep, once a lease period, of what stopped processes left (ADR 0059). */
	private sweepTimer?: ReturnType<typeof setInterval>;
	private sweeping?: Promise<void>;
	/** Limits (ADR 0059): steps at once in this process (embedded), and by destination. */
	private slots?: Slots;
	private readonly limits = new Map<string, Limiter>();
	private readonly logger: Logger;

	constructor(opts: KairoOptions = {}) {
		this.opts = opts;
		this.backend = opts.backend ?? new HttpBackend({ url: opts.url, worker: opts.worker, token: opts.token, concurrency: opts.concurrency });
		this.suspend = opts.mode === 'suspend';
		if (this.suspend && !this.backend.idle) throw new Error('suspend mode needs the embedded backend');
		const rt = (this.backend as { runtime?: unknown }).runtime;
		if (rt instanceof Embedded) {
			this.rt = rt;
			if (opts.logger) rt.logger = opts.logger;
			if (opts.observe) rt.observer = opts.observe;
		}
		this.logger = opts.logger ?? this.rt?.logger ?? consoleLogger;
	}

	defineAction<I, O>(name: string, def: ActionDef<I, O>): void {
		if (name.startsWith('kairo.')) throw new Error(`action ${name}: names beginning with "kairo." are kairo's`);
		if (def.url) checkURL(def.url, def.allowInsecure ?? this.opts.allowInsecure);
		this.actions.set(name, def);
	}

	/** Declares a workflow, or one of its versions (ADR 0060). */
	workflow<I, O>(name: string, fn: WorkflowFn<I, O>, opts: WorkflowOptions = {}): void {
		let d = this.workflows.get(name);
		if (!d) this.workflows.set(name, (d = { versions: new Map(), current: '', currents: 0 }));
		const version = opts.version ?? '';
		if (!opts.draining) {
			d.current = version;
			d.currents++;
		}
		d.versions.set(version, fn);
	}

	/** Workflow name's function at version (undefined: not here). */
	private fnFor(name: string, version: string): WorkflowFn | undefined {
		return this.workflows.get(name)?.versions.get(version);
	}

	/** The version new runs of workflow name start with (undefined: no such workflow). */
	private currentVersion(name: string): string | undefined {
		const d = this.workflows.get(name);
		return d && d.currents > 0 ? d.current : undefined;
	}

	/** Registers the actions and their plans, and starts running the actions' steps. */
	async start(): Promise<void> {
		for (const [name, d] of this.workflows) {
			if (d.currents !== 1) throw new Error(`workflow ${name}: ${d.currents} current versions (one, the others draining)`);
		}
		const specs: NodeSpec[] = [...this.actions].map(([action, def]) => ({
			action,
			effect: def.effect ?? 'real',
			...(def.timeout ? { timeout: def.timeout } : {}),
			...(def.destination ? { destination: def.destination } : {}),
			...(def.maxAttempts ? { max_attempts: def.maxAttempts } : {}),
			...(def.backoff ? { backoff: def.backoff } : {}),
		}));
		for (const action of Object.values(BUILTIN)) specs.push({ action, effect: 'unprotected' });
		if ([...this.actions.values()].some((d) => d.url)) {
			// Actions over HTTP(S) (ADR 0052) take their outcomes on the callback.
			if (!this.opts.secret || !this.opts.callbackUrl) throw new Error('actions with a url need secret and callbackUrl');
			checkURL(this.opts.callbackUrl, this.opts.allowInsecure);
			if (!this.backend.complete) throw new Error('actions with a url need the embedded backend');
		}
		for (const [name, def] of this.actions) {
			if (!def.limit && !def.rate) continue;
			// Actions of one destination share its limits: the strictest.
			const key = def.destination || name;
			let l = this.limits.get(key);
			if (!l) this.limits.set(key, (l = new Limiter()));
			l.tighten(def.limit ?? 0, def.rate ?? 0);
		}
		// kairod's worker limits its own tasks.
		if (this.rt && this.opts.concurrency) this.slots = new Slots(this.opts.concurrency);
		// The embedded runtime observes the steps it runs; with kairod, the worker's are observed here.
		const observer = this.rt ? undefined : this.opts.observe;
		await this.backend.start(specs, observer ? (task, ctx) => this.serveObserved(observer, task, ctx) : (task, ctx) => this.serve(task, ctx));
		for (const { action } of specs) await this.plan(PLAN_CALL + action, { kind: 'step', id: 'call', action, input: { in: '$input.in' } });
		await this.plan(PLAN_WORKFLOW, { kind: 'wait', id: 'done', signal: 'done' }, { started_at: { type: 'integer', value: 0 } });
		const rt = this.rt;
		if (rt) {
			rt.lostDrive = (l) => this.lostDrive(l);
			rt.planFor = waitPlan;
			rt.leaseParents = this.suspend;
		}
		if (this.suspend) {
			// A call that settles drives its workflow on (its id is the
			// workflow's id, "/", the call's key).
			this.stopHook = this.backend.onSettled!((r) => {
				const i = r.run_id.lastIndexOf('/');
				if (i > 0) this.redrive(r.run_id.slice(0, i));
			});
		} else if (rt) {
			// Wait mode: a resident process takes up, once a lease period, what
			// processes that stopped left (ADR 0059): their workflows, steps and
			// timers. One timer for the process, not one for each run; it does
			// not keep the process alive by itself.
			const sweep = () => {
				if (this.sweeping || this.closing) return;
				this.sweeping = (async () => {
					try {
						await rt.tick();
						await rt.recheck();
					} catch (e) {
						// Again next time.
						if (!this.closing) this.logger.warn('kairo: sweeping', { err: e });
					} finally {
						this.sweeping = undefined;
					}
				})();
			};
			sweep();
			this.sweepTimer = setInterval(sweep, rt.lease);
			this.sweepTimer.unref?.();
		}
	}

	/**
	 * Stops serving actions and driving workflows, as a process that stops
	 * does: the workflows it drove stay unfinished in kairo, to be resumed.
	 */
	async close(): Promise<void> {
		this.closing = true;
		this.stopHook?.();
		clearInterval(this.sweepTimer);
		this.life.abort();
		for (const a of this.driving) a.abort();
		// The drives here end; their leases are left expired, for another
		// process to take up (ADR 0059).
		await Promise.allSettled([...this.background, ...this.redrives, this.sweeping]);
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
				const w = workflowOf(info.input);
				if (info.plan !== PLAN_WORKFLOW || finished(info) || !w) return;
				try {
					await this.drive(id, w.name, w.input, info.parent, 0);
				} catch {
					// Suspended again, failed (recorded), cancelled, driven
					// elsewhere, or of a version not here.
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

	/** serve, observed (kairod's worker). */
	private async serveObserved(observer: Observer, task: Task, ctx: TaskContext): Promise<Result> {
		const step = { runId: task.run_id, action: task.action, stepId: task.step_id, attempt: task.attempt };
		observe(observer, this.logger, Date.now, { kind: 'step.started', ...step });
		const began = performance.now();
		const res = await this.serve(task, ctx);
		observe(observer, this.logger, Date.now, {
			kind: 'step.finished',
			...step,
			status: stepStatus(res),
			...(res.error ? { error: res.error } : {}),
			duration: performance.now() - began,
		});
		return res;
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
		// Over a limit, the step waits here for a slot, leased (ADR 0059).
		let release: () => void;
		try {
			release = await this.admit(task.action, def, ctx.signal);
		} catch {
			return { error: `step of ${task.action} stopped waiting for a slot`, retryable: true, errorType: 'cancelled' };
		}
		try {
			if (def.url) return await this.callRemote(task, def, input);
			return { output: (await def.handler(input, ctx)) ?? null };
		} catch (e) {
			return failure(e);
		} finally {
			release();
		}
	}

	/**
	 * Waits for a step of action to be let run (ADR 0059): its
	 * destination's slot and turn, then the process's slot. Returns what
	 * gives the slots back.
	 */
	private async admit(action: string, def: ActionDef, signal: AbortSignal): Promise<() => void> {
		const held: Slots[] = [];
		const release = () => {
			for (const s of held.splice(0)) s.give();
		};
		try {
			const l = this.limits.get(def.destination || action);
			if (l?.slots) {
				await l.slots.take(signal);
				held.push(l.slots);
			}
			if (l && l.every > 0) {
				const now = Date.now();
				const at = Math.max(l.next, now);
				l.next = at + l.every;
				if (at > now) await delay(at - now, signal);
			}
			if (this.slots) {
				await this.slots.take(signal);
				held.push(this.slots);
			}
			return release;
		} catch (e) {
			release();
			throw e;
		}
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
			const run = async (): Promise<{ wire: Wire; unknown: boolean }> => {
				try {
					return toWire({ output: (await def.handler(m.input, { signal: new AbortController().signal, emit: () => {} })) ?? null });
				} catch (e) {
					return toWire(failure(e));
				}
			};
			if (!def.async) {
				// An unknown outcome: 502, which the caller takes as unknown.
				const { wire, unknown } = await run();
				return json(unknown ? 502 : 200, wire);
			}
			// Answer now; run after, and send the outcome to the callback.
			const work = (async () => {
				const { wire: result, unknown } = await run();
				if (unknown) return; // no callback: the lease expires and the step is taken up
				const cb = JSON.stringify({ run_id: m.run_id, act: m.act, attempt: m.attempt, result });
				checkURL(m.callback, this.opts.allowInsecure);
				await post(m.callback, cb, { ca: this.opts.ca, headers: { 'kairo-signature': sign(this.opts.secret!, cb) } });
			})().catch((e) => {
				// A lost callback: the lease expires and the step is taken up.
				this.logger.warn("kairo: an action's callback", { action: m.action, err: e });
			});
			this.opts.waitUntil?.(work);
			return json(202, { accepted: true });
		};
	}

	/**
	 * Runs workflow name as execution id, or resumes it: calls that
	 * finished return their recorded results. Returns its result. In wait
	 * mode, with the embedded runtime, the workflow is driven in the
	 * background, by this process or by whichever process drives it now
	 * (ADR 0059): a signal that aborts throws StoppedError, and the
	 * workflow goes on. In suspend mode it is driven here until it waits:
	 * Suspended. meta is kept with a new workflow (RunInfo.meta, list).
	 */
	async run<O = any>(name: string, input: unknown, opts: { id?: string; meta?: Record<string, unknown>; signal?: AbortSignal } = {}): Promise<O> {
		const id = opts.id ?? randomUUID();
		try {
			const token = await this.begin(name, input, id, opts.meta);
			if (this.suspend) {
				try {
					return (await this.drive(id, name, input, undefined, token)) as O;
				} catch (e) {
					if (e instanceof DrivenElsewhere) throw new Suspended(e.message);
					throw e;
				}
			}
			// kairod: driven here, as the caller waits.
			if (!this.rt) return (await this.drive(id, name, input, undefined, 0)) as O;
			this.driveBackground(id, name, input, undefined, token);
			return await this.result<O>(id, { signal: opts.signal });
		} finally {
			await this.wakeUp();
		}
	}

	/**
	 * Starts workflow name and returns its id once the start is recorded,
	 * without waiting for it (ADR 0059): result(id) gives its result, from
	 * any process. In wait mode it is driven in the background; in suspend
	 * mode it is driven here until it waits (what it comes to is recorded:
	 * nothing is thrown). The id is an idempotency key, as in run.
	 */
	async submit(name: string, input: unknown, opts: { id?: string; meta?: Record<string, unknown> } = {}): Promise<string> {
		const id = opts.id ?? randomUUID();
		const token = await this.begin(name, input, id, opts.meta);
		if (!this.suspend) {
			this.driveBackground(id, name, input, undefined, token);
			return id;
		}
		await this.drive(id, name, input, undefined, token).catch(() => {});
		await this.wakeUp();
		return id;
	}

	/**
	 * Workflow id's result once it has finished. In wait mode it waits; a
	 * signal that aborts, or the process closing, throws StoppedError (the
	 * workflow goes on). In suspend mode it does not wait: Suspended while
	 * the workflow has not finished.
	 */
	async result<O = any>(id: string, opts: { signal?: AbortSignal } = {}): Promise<O> {
		if (this.suspend) {
			const info = await this.backend.get(id);
			if (!finished(info)) throw new Suspended(`workflow ${id} has not finished`);
			return done(id, info) as O;
		}
		const signal = opts.signal ? AbortSignal.any([opts.signal, this.life.signal]) : this.life.signal;
		let info: RunInfo;
		try {
			info = await this.backend.wait(id, signal);
		} catch (e) {
			if (signal.aborted) throw new StoppedError(`workflow ${id}: stopped waiting for it`);
			throw e;
		}
		return done(id, info) as O;
	}

	/**
	 * Workflows started here or elsewhere (root runs: not child workflows,
	 * not calls), in the order they were created (ADR 0059). The embedded
	 * runtime only.
	 */
	async list(opts: ListOptions = {}): Promise<RunInfo[]> {
		if (!this.rt) throw new Error('list needs the embedded backend');
		const ms = (t: number | Date | undefined) => (t instanceof Date ? t.getTime() : t);
		return this.rt.list({ ...opts, since: ms(opts.since), until: ms(opts.until) });
	}

	/**
	 * Makes workflow name's run as id (or finds it). A new one is leased to
	 * this process to drive, in the same transaction: its token (0: it
	 * existed, or no leases here).
	 */
	private async begin(name: string, input: unknown, id: string, meta?: Record<string, unknown>): Promise<number> {
		const version = this.currentVersion(name);
		if (version === undefined) throw new Error(`no workflow ${name}`);
		await this.plan(PLAN_WORKFLOW, { kind: 'wait', id: 'done', signal: 'done' }, { started_at: { type: 'integer', value: 0 } });
		const token = this.rt ? newToken() : 0;
		const started = await this.backend.run(
			PLAN_WORKFLOW,
			workflowInput(name, version, input),
			{ runId: id, vars: { started_at: Date.now() }, workflow: name, ...(meta ? { meta } : {}), ...(token ? { drive: token } : {}) },
		);
		return started.existing ? 0 : token;
	}

	/**
	 * Runs workflow id's function here, holding its drive lease (ADR 0059):
	 * token is the lease set with its start, or 0 to claim it now. What it
	 * leaves: nothing when it finished (the lease ends with it), no lease
	 * when it waits (suspend mode: what it waits for drives it on), an
	 * expired lease when it stopped otherwise (a tick takes it up).
	 */
	private async drive(id: string, name: string, input: unknown, parent: string | undefined, token: number): Promise<unknown> {
		const rt = this.rt;
		if (rt && token === 0) {
			const info = await this.backend.get(id);
			if (finished(info)) return done(id, info);
			const w = workflowOf(info.input);
			if (w && !this.fnFor(name, w.version)) throw notHere(name, w.version); // not claimed: another process's
			token = newToken();
			if (!(await rt.claimDrive(id, token))) throw new DrivenElsewhere(`workflow ${id} is driven by another process`);
		}
		rt?.driving(true);
		try {
			return await this.runAs(name, input, id, parent ? { id: parent } : undefined);
		} catch (e) {
			if (rt) await rt.endDrive(id, token, !(e instanceof Suspended)).catch((err) => this.logger.warn('kairo: ending a drive lease', { workflow: id, err }));
			throw e;
		} finally {
			rt?.driving(false);
		}
	}

	/** Wait mode: drives workflow id in the background, unless this process drives it already. */
	private driveBackground(id: string, name: string, input: unknown, parent: string | undefined, token: number): void {
		if (this.owned.has(id) || this.closing) return;
		this.owned.add(id);
		const p: Promise<void> = this.drive(id, name, input, parent, token)
			.then(
				() => {},
				(e) => {
					// Stopped, cancelled, driven elsewhere, or of a version not
					// here: nothing to do here.
					if (e instanceof DrivenElsewhere || e instanceof StoppedError || e instanceof CancelledError) return;
					if (e instanceof KairoError && e.status === 404) return;
					this.logger.warn('kairo: driving a workflow', { workflow: id, err: e });
				},
			)
			.finally(() => {
				this.owned.delete(id);
				this.background.delete(p);
			});
		this.background.add(p);
	}

	/** Takes up workflow l.run, whose driver stopped: its drive lease expired (ADR 0059). */
	private async lostDrive(l: LeaseRow): Promise<void> {
		const rt = this.rt!;
		let info: RunInfo | undefined;
		try {
			info = await this.backend.get(l.run);
		} catch (e) {
			if (!(e instanceof KairoError && e.status === 404)) throw e;
		}
		if (!info || info.plan !== PLAN_WORKFLOW || finished(info)) {
			// Nothing left to drive.
			await rt.endDrive(l.run, l.attempt, false, l.owner);
			return;
		}
		const w = workflowOf(info.input);
		if (!w) return;
		if (!this.fnFor(w.name, w.version)) throw notHere(w.name, w.version); // another process's
		if (this.suspend) {
			this.redrive(l.run);
			return;
		}
		this.driveBackground(l.run, w.name, w.input, info.parent, 0);
	}

	/**
	 * Sends a signal to the first wait for it in workflow id that has not
	 * received one. With the embedded runtime, a wait the workflow has not
	 * reached yet receives it when it does (ADR 0059): signals of one name
	 * go to its waits in order.
	 */
	async signal(id: string, name: string, payload: unknown = null): Promise<void> {
		// The waits' plan, which this process may not have needed yet.
		await this.plan(PLAN_WAIT + name, waitRoot(name));
		for (let n = 0; ; n++) {
			const runId = callId(id, PLAN_WAIT + name, null, n);
			let r: RunInfo;
			try {
				r = await this.backend.get(runId);
			} catch (e) {
				if (!(e instanceof KairoError && e.status === 404)) throw e;
				if (!this.rt) throw new Error(`workflow ${id} does not wait for ${name}`);
				if (await this.signalAhead(id, name, runId, payload)) break;
				n--; // the workflow made the wait meanwhile: signal it
				continue;
			}
			if (finished(r)) continue;
			await this.backend.signal(runId, name, payload);
			break;
		}
		if (this.suspend) {
			await this.settle();
			await this.wakeUp();
		}
	}

	/** Makes wait runId of workflow id, as the workflow will when it reaches it, with the signal applied in the same transaction. */
	private async signalAhead(id: string, name: string, runId: string, payload: unknown): Promise<boolean> {
		let wf: RunInfo;
		try {
			wf = await this.backend.get(id);
		} catch (e) {
			if (e instanceof KairoError && e.status === 404) throw new Error(`no workflow ${id}`);
			throw e;
		}
		if (wf.plan !== PLAN_WORKFLOW) throw new Error(`no workflow ${id}`);
		if (finished(wf)) throw new Error(`workflow ${id} has finished`);
		const r = await this.backend.run(PLAN_WAIT + name, { in: null }, { runId, parent: id, then: [{ kind: 'signal', name, data: payload ?? null }] });
		return !r.existing;
	}

	/** Cancels workflow id: the calls it is waiting for are cancelled with it. */
	async cancel(id: string): Promise<void> {
		await this.backend.cancel(id);
	}

	/**
	 * Settles call callId, stopped for review (blocked: a real step whose
	 * outcome was unknown) or with a real attempt out that cannot go on (its
	 * action is no longer real, ADR 0060), with the output it had: it did
	 * take effect. The embedded runtime only.
	 */
	async resolve(callId: string, output: unknown = null): Promise<void> {
		await this.settleCall(callId, output, undefined);
	}

	/** Settles call callId as resolve does, as not done: it failed with message. */
	async resolveFailed(callId: string, message = 'resolved as failed'): Promise<void> {
		await this.settleCall(callId, null, message || 'resolved as failed');
	}

	private async settleCall(callId: string, output: unknown, error: string | undefined): Promise<void> {
		if (!this.backend.resolve) throw new Error('resolve needs the embedded backend');
		await this.backend.resolve(callId, output, error);
		if (this.suspend) {
			await this.settle();
			await this.wakeUp();
		}
	}

	/**
	 * Drives workflow id here. parent: the workflow that made it, if any;
	 * with signal and cancelled when it runs inside it (a child workflow).
	 */
	private async runAs(
		name: string,
		input: unknown,
		id: string,
		parent?: { id: string; signal?: AbortSignal; cancelled?: () => boolean },
	): Promise<unknown> {
		const current = this.currentVersion(name);
		if (current === undefined) throw new Error(`no workflow ${name}`);
		await this.plan(PLAN_WORKFLOW, { kind: 'wait', id: 'done', signal: 'done' }, { started_at: { type: 'integer', value: 0 } });
		// A new run starts with the current version; one that exists goes on
		// with the version it started with (ADR 0060).
		const started = await this.backend.run(
			PLAN_WORKFLOW,
			workflowInput(name, current, input),
			{ runId: id, vars: { started_at: Date.now() }, workflow: name, ...(parent ? { parent: parent.id } : {}) },
		);
		const info = await this.backend.get(id);
		if (finished(info)) return done(id, info);
		// kairod does not report a run's input: there, the current version.
		const version = 'input' in info ? (workflowOf(info.input)?.version ?? '') : current;
		const fn = this.fnFor(name, version);
		if (!fn) throw notHere(name, version);
		if (started.existing && !this.backend.keepsCalls) {
			// Resuming: the calls' records must still be there (ADR 0049).
			// (The embedded runtime keeps them while the workflow runs, ADR 0054.)
			const at = Number((info.vars as { started_at?: number } | undefined)?.started_at ?? 0);
			const ttl = this.opts.idempotencyTTL ?? 24 * 3600 * 1000;
			if (at > 0 && Date.now() - at > ttl) {
				throw new ResultLostError(`workflow ${id} started more than ${ttl}ms ago: its calls' records may be gone`);
			}
		}
		const abort = new AbortController();
		this.driving.add(abort);
		const stop = () => abort.abort();
		parent?.signal?.addEventListener('abort', stop);
		if (parent?.signal?.aborted) abort.abort();
		// Cancelled in kairo (its run, or a workflow above it): only then
		// are its calls cancelled. Otherwise an abort only stops the driving
		// here (the process closes), and the workflow goes on later.
		let self = false;
		const cancelled = () => self || (parent?.cancelled?.() ?? false);
		if (!this.suspend) {
			// The workflow's own run ends when it is cancelled: stop the calls.
			// (Suspended, a cancelled workflow stops when it is driven next.)
			void this.backend.wait(id, abort.signal).then((r) => {
				if (r.status === 'cancelled') {
					self = true;
					abort.abort();
				}
			}, () => {});
		}
		this.drivingIds.add(id);
		const ctx = new Context(this, id, abort.signal, cancelled);
		try {
			// A function that throws, at once or later, fails the workflow; one
			// that does not stop when the workflow is cancelled or the process
			// closes is left behind.
			const running = Promise.resolve().then(() => fn(ctx, input));
			running.catch(() => {});
			const value = await Promise.race([running, aborted(abort.signal)]);
			await this.backend.signal(id, 'done', { ok: true, value: value ?? null });
			return value;
		} catch (e) {
			if (e instanceof Suspended) throw e; // goes on later
			// A child workflow whose version is not here: this one is left
			// to another process, not failed (ADR 0060).
			if (e instanceof NotHereError) throw e;
			if (abort.signal.aborted) {
				if (cancelled()) {
					// Cancelled with the workflow above it: so is its run.
					if (!self) await this.backend.cancel(id).catch(() => {});
					throw new CancelledError(`workflow ${id} cancelled`);
				}
				throw new StoppedError(`workflow ${id}: not driven here any more`);
			}
			const error = String((e as Error)?.message ?? e);
			await this.backend.signal(id, 'done', { ok: false, error }).catch(() => {});
			throw new WorkflowError(id, error, { cause: e });
		} finally {
			parent?.signal?.removeEventListener('abort', stop);
			this.driving.delete(abort);
			this.drivingIds.delete(id);
			abort.abort();
		}
	}

	/** One call of workflow ctx: a run of its own, found again by its id. */
	async callRun(plan: string, root: unknown | null, input: unknown, runId: string, ctx: Context): Promise<unknown> {
		if (root) await this.plan(plan, root);
		// Kept and removed with the workflow that makes it (ADR 0054).
		await this.backend.run(plan, { in: input }, { runId, parent: ctx.id });
		let r: RunInfo;
		if (this.suspend) {
			// Whatever this process can do for the call is done once it is
			// idle; a call still going then waits for a timer or a signal.
			await this.backend.idle!();
			r = await this.backend.get(runId);
			if (!finished(r) && r.status !== 'blocked') throw new Suspended(`call ${runId} waits`);
		} else {
			try {
				r = await this.backend.wait(runId, ctx.signal);
			} catch (e) {
				if (ctx.signal.aborted) {
					// The workflow was cancelled in kairo: so is the call. If not,
					// the workflow is only no longer driven here: the call goes on
					// in kairo, and the workflow resumes it later.
					if (ctx.cancelled()) {
						await this.backend.cancel(runId).catch(() => {});
						throw new CallError(runId, calledAction(plan), 'cancelled');
					}
					throw new StoppedError(`call ${runId}: stopped waiting for it`);
				}
				throw e;
			}
		}
		if (r.trimmed) throw new ResultLostError(`call ${runId} finished, but kairo no longer keeps its result`);
		if (r.status !== 'completed') throw new CallError(runId, calledAction(plan), r.status, r.error ?? '');
		return r.output;
	}

	async childWorkflow(name: string, input: unknown, id: string, ctx: Context): Promise<unknown> {
		return this.runAs(name, input, id, { id: ctx.id, signal: ctx.signal, cancelled: ctx.cancelled });
	}
}

/** The plan of a wait for signal name, with a timeout (ms; 0: none). */
function waitRoot(name: string, timeoutMs = 0) {
	return { kind: 'wait', id: 'w', signal: name, ...(timeoutMs > 0 ? { timeout: `${timeoutMs}ms` } : {}) };
}

/** A wait's plan from its name (kairo.wait/<signal>[@<ms>]): one made by another process (ADR 0059). */
function waitPlan(name: string): { name: string; root: unknown } | undefined {
	if (!name.startsWith(PLAN_WAIT)) return undefined;
	const rest = name.slice(PLAN_WAIT.length);
	const i = rest.lastIndexOf('@');
	if (i >= 0 && /^\d+$/.test(rest.slice(i + 1)) && Number(rest.slice(i + 1)) > 0) {
		return { name, root: waitRoot(rest.slice(0, i), Number(rest.slice(i + 1))) };
	}
	return { name, root: waitRoot(rest) };
}

/** Rejects when signal aborts. */
function aborted(signal: AbortSignal): Promise<never> {
	return new Promise((_, rej) => {
		if (signal.aborted) return rej(signal.reason);
		signal.addEventListener('abort', () => rej(signal.reason), { once: true });
	});
}

/** Resolves after ms, or rejects when signal aborts first. */
function delay(ms: number, signal: AbortSignal): Promise<void> {
	return new Promise((res, rej) => {
		if (signal.aborted) return rej(signal.reason);
		const stop = () => {
			clearTimeout(t);
			rej(signal.reason);
		};
		const t = setTimeout(() => {
			signal.removeEventListener('abort', stop);
			res();
		}, ms);
		signal.addEventListener('abort', stop, { once: true });
	});
}

/** Slots taken and given back; a slot given back goes to the step waiting longest (no polling). */
class Slots {
	private free: number;
	private readonly queue: Array<() => void> = [];
	readonly size: number;

	constructor(size: number) {
		this.size = size;
		this.free = size;
	}

	/** Takes a slot, waiting for one; rejects when signal aborts first. */
	take(signal: AbortSignal): Promise<void> {
		if (this.free > 0 && this.queue.length === 0) {
			this.free--;
			return Promise.resolve();
		}
		return new Promise((res, rej) => {
			if (signal.aborted) return rej(signal.reason);
			const go = () => {
				signal.removeEventListener('abort', stop);
				res();
			};
			const stop = () => {
				const i = this.queue.indexOf(go);
				if (i >= 0) this.queue.splice(i, 1);
				rej(signal.reason);
			};
			this.queue.push(go);
			signal.addEventListener('abort', stop, { once: true });
		});
	}

	give(): void {
		const next = this.queue.shift();
		if (next) next();
		else this.free++;
	}
}

/** The limits of one destination (ADR 0059): steps at once, and the spacing of their starts (ms). */
class Limiter {
	slots?: Slots;
	every = 0;
	/** When the next step may start (unix ms). */
	next = 0;

	tighten(limit: number, perMinute: number): void {
		if (limit > 0 && (!this.slots || limit < this.slots.size)) this.slots = new Slots(limit);
		if (perMinute > 0) this.every = Math.max(this.every, 60_000 / perMinute);
	}
}

/** The result of a finished workflow run. */
function done(id: string, info: RunInfo): unknown {
	if (info.trimmed) throw new ResultLostError(`workflow ${id} finished, but kairo no longer keeps its result`);
	if (info.status === 'cancelled') throw new CancelledError(`workflow ${id} cancelled`);
	const out = (info.output as { payload?: { ok: boolean; value?: unknown; error?: string } } | undefined)?.payload;
	if (!out) throw new WorkflowError(id, `${info.status}: ${info.error ?? ''}`);
	if (!out.ok) throw new WorkflowError(id, out.error ?? '');
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
	/** Aborts when the workflow is cancelled, and when it stops being driven here (the process closes). */
	readonly signal: AbortSignal;
	/** Whether the workflow (or one above it) was cancelled in kairo: only then are its calls cancelled. */
	readonly cancelled: () => boolean;
	private readonly k: Kairo;
	private readonly seen = new Map<string, number>();

	constructor(k: Kairo, id: string, signal: AbortSignal, cancelled: () => boolean = () => false) {
		this.k = k;
		this.id = id;
		this.signal = signal;
		this.cancelled = cancelled;
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
		return (await this.k.callRun(PLAN_CALL + action, null, input, id, this)) as O;
	}

	/** Runs the calls at once; their results in order. */
	parallel<T extends readonly (() => Promise<unknown>)[]>(fns: T): Promise<{ [K in keyof T]: Awaited<ReturnType<T[K]>> }> {
		return Promise.all(fns.map((f) => f())) as any;
	}

	/**
	 * Waits for signal name (see Kairo.signal); returns its payload. With
	 * timeout (ms), throws TimedOutError if no signal comes in time; the
	 * timeout is kept in kairo (ADR 0059).
	 */
	async waitFor<P = any>(name: string, opts: { timeout?: number } = {}): Promise<P> {
		// The timeout is the plan's, not the call's: the id is the same with
		// or without one, so a signal sent ahead finds it (ADR 0059).
		const id = this.next(PLAN_WAIT + name, null);
		const ms = Math.floor(opts.timeout ?? 0);
		const plan = ms > 0 ? `${PLAN_WAIT}${name}@${ms}` : PLAN_WAIT + name;
		const out = (await this.k.callRun(plan, waitRoot(name, ms), null, id, this)) as { timed_out?: boolean; payload: P };
		if (out.timed_out) throw new TimedOutError(`wait for ${name} timed out`);
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
		return (await this.k.childWorkflow(name, input, id, this)) as O;
	}
}
