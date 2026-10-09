// The embedded runtime (ADR 0051): kairo without a resident server. The
// pure core (WASM) runs in this process; runs live in a database (Store).
// Each event is one transaction: lock the run, apply the event, append it,
// replace the state, arm or disarm timers. The commands are carried out
// after the commit: a real step only once its intent was committed with it
// (invariant 4).
//
// A step dispatched here is leased to this process while it runs. A
// process that stops leaves its leases to expire; whoever finds an expired
// lease applies an unknown outcome to the step, as kairod does when a
// worker goes away: an unprotected step runs again, a real one stops for
// review (invariant 5). Expired leases and due timers are looked for when
// the runtime opens, along the way of its work (at most once a lease
// period) and on tick(): no polling.

import { fileURLToPath } from 'node:url';

import { Core, type Compiled, type CoreCommand, type CoreEvent } from './core.ts';
import { KairoError, type NodeSpec, type RunInfo } from './client.ts';
import type { Result, Task } from './protocol.ts';
import { randomInt, randomUUID } from 'node:crypto';

import { consoleLogger, observe, stepStatus, type Logger, type Observation, type Observer } from './observe.ts';
import type { LeaseRow, ListFilter, RunRow, Store, TimerRow } from './store.ts';
import type { TaskContext } from './worker.ts';

/** Runs the actions of dispatched steps in this process. */
export type ActionHandler = (task: Task, ctx: TaskContext) => Promise<Result>;

export interface EmbeddedOptions {
	store: Store;
	/** kairo.wasm (cmd/kairo-wasm): a path or its bytes. Default: the one in this package (npm run build:wasm). */
	wasm?: string | Uint8Array;
	now?: () => number;
	/** How long a step is leased to this process without renewal (ms; default 30 s). */
	leaseMs?: number;
	/**
	 * This process's name in the leases (default: random). A process that
	 * gives the name its earlier self had takes that self's steps as
	 * stopped when it opens, without waiting for the leases to expire.
	 */
	owner?: string;
	/**
	 * How long a finished tree of runs is kept before tick removes it (ms;
	 * ADR 0054). Infinity keeps it. Default: KAIRO_KEEP_FINISHED ("30m",
	 * "24h", "7d", "forever"), or 24 hours.
	 */
	keepFinished?: number;
	/** Takes what goes wrong in the runtime's background work (lease renewals, sweeps, timers; default: the console). */
	logger?: Logger;
	/**
	 * Given each Observation as it happens: runs that start and settle,
	 * steps' attempts that start and finish. Called in the runtime, as it
	 * works: it must not block.
	 */
	observe?: Observer;
	/**
	 * Timers are kept in the store but not armed in this process: only
	 * tick fires them. For tests on a clock they move (createTestEnv), and
	 * for schedulers that own time.
	 */
	manualTimers?: boolean;
}

/** Finished trees removed per tick at most (ADR 0054); the rest next time. */
const REMOVE_PER_TICK = 100;

/** keepFinished from the options, or KAIRO_KEEP_FINISHED, or 24 hours (ADR 0054). */
export function keepFinished(opt: number | undefined, env = process.env.KAIRO_KEEP_FINISHED): number {
	if (opt !== undefined) return opt;
	if (!env) return 24 * 3600 * 1000;
	if (env === 'forever') return Infinity;
	const m = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/.exec(env);
	if (!m) throw new Error(`KAIRO_KEEP_FINISHED=${env}: a duration such as 30m, 24h, 7d, or forever`);
	return Number(m[1]) * { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 }[m[2] as 'ms' | 's' | 'm' | 'h' | 'd'];
}

const DONE = new Set(['completed', 'failed', 'cancelled']);
/** A run that will not go on by itself: finished, or stopped for review. */
const SETTLED = new Set([...DONE, 'blocked']);
const OUTCOMES = new Set(['step_ok', 'step_err', 'step_wait']);

/**
 * A call's action is no longer real while a real attempt of it is out (ADR
 * 0060). The call waits: restore the action, or settle the call with
 * resolve, resolveFailed or cancel.
 */
export class EffectWeakenedError extends KairoError {
	override name = 'EffectWeakenedError';
	constructor(message: string) {
		super(409, message);
	}
}

/** EffectWeakenedError's message, without the run. */
export const EFFECT_WEAKENED = 'a real attempt is out and its action is no longer real';

const PLAN_WORKFLOW = 'kairo.workflow';

export class Embedded {
	private readonly core: Core;
	private readonly store: Store;
	/** The runtime's clock (unix ms): EmbeddedOptions.now, or Date.now. */
	readonly now: () => number;
	private readonly manualTimers: boolean;
	private readonly plans = new Map<string, Compiled>();
	private handler?: ActionHandler;
	private readonly waiters = new Map<string, Set<(r: RunInfo) => void>>();
	/** Called with each run that settles in this process. */
	private readonly settledHooks = new Set<(r: RunInfo) => void>();
	private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
	private readonly running = new Map<string, AbortController>();
	/** Work started by this runtime (steps, timers): close waits for it. */
	private readonly busy = new Set<Promise<unknown>>();
	private closed = false;
	readonly owner: string;
	private readonly leaseMs: number;
	private readonly keepMs: number;
	private renewal?: ReturnType<typeof setInterval>;
	private lastSweep = 0;
	private unlisten?: () => Promise<void>;
	/** Workflows driven by this process now: their drive leases are renewed with its steps' (ADR 0059). */
	private drives = 0;
	/**
	 * Set by Kairo (ADR 0059). leaseParents: a run that settles leases its
	 * parent workflow to this process, to be driven on (suspend mode).
	 * lostDrive takes up a drive lease whose owner stopped. planFor gives
	 * the definition of a plan not registered here (a wait made elsewhere).
	 */
	leaseParents = false;
	lostDrive?: (l: LeaseRow) => Promise<void>;
	planFor?: (name: string) => { name: string; [k: string]: unknown } | undefined;
	/** Set by Kairo when given (KairoOptions.logger, observe). */
	logger: Logger;
	observer?: Observer;

	private constructor(core: Core, opts: EmbeddedOptions) {
		this.core = core;
		this.store = opts.store;
		this.now = opts.now ?? Date.now;
		this.manualTimers = !!opts.manualTimers;
		this.owner = opts.owner ?? randomUUID();
		this.leaseMs = opts.leaseMs ?? 30_000;
		this.keepMs = keepFinished(opts.keepFinished);
		this.logger = opts.logger ?? consoleLogger;
		this.observer = opts.observe;
	}

	private observe(o: Omit<Observation, 'at'>): void {
		observe(this.observer, this.logger, this.now, o);
	}

	/** Tracks background work p, logging what goes wrong in it (but a run whose plan is another process's). */
	private background(p: Promise<unknown>, msg: string, attrs: Record<string, unknown> = {}): void {
		this.track(p.catch((e) => {
			try {
				skipUnknownPlan(e);
			} catch {
				this.logger.warn(msg, { ...attrs, err: e });
			}
		}));
	}

	static async open(opts: EmbeddedOptions): Promise<Embedded> {
		const core = await Core.load(opts.wasm ?? fileURLToPath(new URL('../wasm/kairo.wasm', import.meta.url)));
		await opts.store.init();
		const e = new Embedded(core, opts);
		// A process of the same name that stopped: its steps are not running.
		if (opts.owner) await opts.store.expireLeases(opts.owner, e.now());
		// Runs that settle in other processes wake the waits here.
		if (opts.store.listen) e.unlisten = await opts.store.listen((id) => e.settledElsewhere(id));
		return e;
	}

	private settledElsewhere(runId: string): void {
		const ws = this.waiters.get(runId);
		if (!ws || ws.size === 0) return;
		this.track(this.get(runId).then((r) => {
			if (SETTLED.has(r.status)) for (const w of this.waiters.get(runId) ?? []) w(r);
		}));
	}

	/** Calls hook with each run that settles in this process; returns how to stop. */
	onSettled(hook: (r: RunInfo) => void): () => void {
		this.settledHooks.add(hook);
		return () => this.settledHooks.delete(hook);
	}

	/** Returns once the work started here (steps, timers that fired) is done. */
	async idle(): Promise<void> {
		while (this.busy.size > 0) await Promise.allSettled([...this.busy]);
	}

	/** Registers node specs and the handler that runs their steps here. */
	registerActions(specs: NodeSpec[], handler: ActionHandler): void {
		this.core.register(specs);
		this.handler = handler;
	}

	/** Compiles a plan; runs of it name it. */
	registerPlan(definition: { name: string; [k: string]: unknown }): Compiled {
		const c = this.core.compile(definition);
		this.plans.set(c.name, c);
		return c;
	}

	/** Plan name, registered here, or made now from planFor. */
	private plan(name: string): Compiled | undefined {
		const c = this.plans.get(name);
		if (c || !this.planFor) return c;
		const def = this.planFor(name);
		return def ? this.registerPlan(def) : undefined;
	}

	/** The lease period (ms). */
	get lease(): number {
		return this.leaseMs;
	}

	/**
	 * Starts a run of plan, or finds it: a run id is an idempotency key, and
	 * a run that exists (running or finished) is not started again. parent:
	 * the run that makes this one, which it is kept and removed with (ADR 0054).
	 */
	async run(plan: string, input: unknown, opts: RunStart): Promise<{ run_id: string; existing: boolean }> {
		const p = this.plan(plan);
		if (!p) throw new KairoError(404, `no plan ${plan}`);
		const at = this.now();
		const ev: CoreEvent = { kind: 'start', at, data: input ?? null, ...(opts.vars ? { vars: opts.vars } : {}) };
		const then = (opts.then ?? []).map((e) => ({ ...e, at }) as CoreEvent);
		try {
			const existing = await this.process(opts.runId, [ev, ...then], { ...opts, plan: p, at });
			return { run_id: opts.runId, existing };
		} catch (e) {
			// Two processes starting one id at once: the one that lost finds it.
			if (await this.store.get(opts.runId).catch(() => undefined)) return { run_id: opts.runId, existing: true };
			throw e;
		}
	}

	/** Claims workflow run's drive lease with token for this process (ADR 0059): whether no other process holds it. */
	claimDrive(run: string, token: number): Promise<boolean> {
		const now = this.now();
		return this.store.claimDrive({ run, act: 0, attempt: token, owner: this.owner, until: now + this.leaseMs }, now);
	}

	/** Ends a drive lease (this process's, unless owner is given): removed, or (resume) left expired. */
	endDrive(run: string, token: number, resume: boolean, owner = this.owner): Promise<void> {
		return this.store.endDrive(run, owner, token, this.now(), resume);
	}

	/** Counts a workflow driven here (its lease renewed) while it is. */
	driving(on: boolean): void {
		this.drives += on ? 1 : -1;
		this.renew();
	}

	/** Root runs in creation order (ADR 0059). */
	async list(f: ListFilter): Promise<RunInfo[]> {
		return (await this.store.list(f)).map(info);
	}

	/** Without listen, reads the runs waited for here: one may have settled in another process (ADR 0059). */
	async recheck(): Promise<void> {
		if (this.store.listen) return;
		for (const id of [...this.waiters.keys()]) {
			const r = await this.get(id).catch(() => undefined);
			if (r && SETTLED.has(r.status)) for (const w of this.waiters.get(id) ?? []) w(r);
		}
	}

	async get(runId: string): Promise<RunInfo> {
		const row = await this.store.get(runId);
		if (!row) throw new KairoError(404, `no run ${runId}`);
		return info(row);
	}

	/** Waits until the run has finished or stopped for review (in this process's view). */
	async wait(runId: string, signal?: AbortSignal): Promise<RunInfo> {
		let resolve!: (r: RunInfo) => void;
		const done = new Promise<RunInfo>((r) => (resolve = r));
		let set = this.waiters.get(runId);
		if (!set) this.waiters.set(runId, (set = new Set()));
		set.add(resolve);
		try {
			// Registered first, then read: an end between the two still wakes us.
			const now = await this.get(runId);
			if (SETTLED.has(now.status)) return now;
			if (!signal) return await done;
			return await new Promise<RunInfo>((res, rej) => {
				const abort = () => rej(signal.reason ?? new Error('aborted'));
				if (signal.aborted) return abort();
				signal.addEventListener('abort', abort, { once: true });
				done.then((r) => {
					signal.removeEventListener('abort', abort);
					res(r);
				});
			});
		} finally {
			set.delete(resolve);
			if (set.size === 0) this.waiters.delete(runId);
		}
	}

	async signal(runId: string, name: string, payload: unknown = null): Promise<void> {
		await this.process(runId, [{ kind: 'signal', at: this.now(), name, data: payload }]);
	}

	async cancel(runId: string): Promise<void> {
		await this.process(runId, [{ kind: 'cancel', at: this.now(), error: 'cancelled' }]);
	}

	/**
	 * Settles a step stopped for review (ADR 0058), or a real attempt out
	 * that cannot go on (its action is no longer real, ADR 0060): it took
	 * effect (with output), or it did not (error; the run fails).
	 */
	async resolve(runId: string, output: unknown, error?: string): Promise<void> {
		const row = await this.store.get(runId);
		if (!row) throw new KairoError(404, `no run ${runId}`);
		const ins = this.core.inspect(row.state);
		// Stopped for review; or a real attempt out that cannot go on because
		// its action is no longer real (ADR 0060): only then, not one out as
		// usual (unknown marks it so to the core).
		let acts = ins.review ?? [];
		let out = false;
		if (acts.length === 0 && ins.intents?.length) {
			const plan = this.plan(row.plan);
			if (plan && row.hash !== plan.hash && this.effectWeakened(plan, row)) [acts, out] = [ins.intents, true];
		}
		if (acts.length === 0) throw new KairoError(409, `run ${runId} has no step stopped for review`);
		const ev: CoreEvent = {
			kind: 'resolve',
			at: this.now(),
			act: acts[0],
			...(out ? { unknown: true } : {}),
			...(error !== undefined ? { error } : { data: output ?? null }),
		};
		await this.process(runId, [ev]);
	}

	/**
	 * Takes up what no process is doing: steps whose lease expired (their
	 * process stopped) and timers that are due (also those armed by other
	 * processes). Runs whose plan is not registered here are left alone. One
	 * run that cannot go on does not stop the others (ADR 0060): it is
	 * logged and observed (run.stuck), and put off a lease period, so that it
	 * does not stay first of what each tick takes up. Throws only when the
	 * store fails.
	 */
	async tick(): Promise<void> {
		this.lastSweep = this.now();
		for (const l of await this.store.expiredLeases(this.now(), 1000)) {
			if (this.closed) return;
			// A workflow's driver stopped (ADR 0059).
			const p = l.act === 0 ? this.lostDrive?.(l) : this.recover(l);
			if (await this.stuck(l.run, p))
				await this.store.handOver({ ...l, owner: '', until: this.now() + this.leaseMs }).catch((e) => this.logger.warn('kairo: putting off a lease', { run: l.run, err: e }));
		}
		for (const t of await this.store.dueTimers(this.now(), 1000)) {
			if (this.closed) return;
			if (!(await this.stuck(t.run, this.fire(t)))) continue;
			const later: TimerRow = { ...t, at: this.now() + this.leaseMs };
			await this.store
				.withRun(t.run, (row) => (row ? { events: [], setTimers: [later], result: undefined } : { events: [], result: undefined }))
				.catch((e) => this.logger.warn('kairo: putting off a timer', { run: t.run, err: e }));
		}
		// Finished trees past the time they are kept (ADR 0054).
		if (Number.isFinite(this.keepMs)) await this.store.removeFinished(this.now() - this.keepMs, REMOVE_PER_TICK);
	}

	/**
	 * Awaits p; logs and observes the run if it could not go on now (but one
	 * whose plan is another process's): whether it could not.
	 */
	private async stuck(runId: string, p: Promise<unknown> | undefined): Promise<boolean> {
		try {
			await p;
			return false;
		} catch (e) {
			try {
				skipUnknownPlan(e);
				return false;
			} catch {
				this.logger.warn('kairo: a run cannot go on', { run: runId, err: e });
				this.observe({ kind: 'run.stuck', runId, error: String((e as Error)?.message ?? e) });
				return true;
			}
		}
	}

	/** The process that ran l's step stopped: its outcome is unknown. */
	private recover(l: LeaseRow): Promise<boolean> {
		return this.process(l.run, [{ kind: 'step_err', at: this.now(), act: l.act, attempt: l.attempt, unknown: true, retryable: true,
			error: 'the process running the step stopped', error_type: 'process_lost' }]);
	}

	/**
	 * Stops, as a process that stops: no new step starts, timers are
	 * dropped (they stay in the database), steps running here are aborted.
	 * Returns once what this runtime was writing is written.
	 */
	async close(): Promise<void> {
		this.closed = true;
		for (const t of this.timers.values()) clearTimeout(t);
		this.timers.clear();
		for (const a of this.running.values()) a.abort();
		this.running.clear();
		this.renew();
		while (this.busy.size > 0) await Promise.allSettled([...this.busy]);
		await this.unlisten?.();
		await this.store.close();
	}

	/** Renews this process's leases while it runs steps; one timer for all of them. */
	private renew(): void {
		if ((this.running.size > 0 || this.drives > 0) && !this.closed) {
			this.renewal ??= setInterval(() => {
				this.background(this.store.renewLeases(this.owner, this.now() + this.leaseMs), 'kairo: renewing leases');
			}, Math.max(this.leaseMs / 3, 10));
			return;
		}
		clearInterval(this.renewal);
		this.renewal = undefined;
	}

	private track(p: Promise<unknown>): void {
		this.busy.add(p);
		void p.catch(() => {}).finally(() => this.busy.delete(p));
	}

	/**
	 * Applies events to run id in one transaction, then carries out the
	 * commands. With start, a new run of that plan (or the existing one: the
	 * answer says which).
	 */
	private async process(runId: string, events: CoreEvent[], start?: RunStart & { plan: Compiled; at: number }): Promise<boolean> {
		const out = await this.store.withRun(runId, (row) => {
			// A step's outcome ends its lease, applied or not (a stale one).
			const endLeases = events.filter((e) => OUTCOMES.has(e.kind) && e.act !== undefined).map((e) => e.act!);
			if (row && start) return { events: [], result: { existing: true, started: false, settled: false, commands: [] as CoreCommand[], row } };
			if (!row && !start)
				return { events: [], endLeases, result: { existing: false, started: false, settled: false, commands: [] as CoreCommand[], row: undefined } };
			const plan = start ? start.plan : this.plan(row!.plan);
			if (!plan) throw new KairoError(404, `run ${runId}: plan ${row!.plan} is not registered here`);
			// Goes on under the current plan, if it may (ADR 0060): next has its hash.
			if (row && row.hash !== plan.hash) this.adoptable(runId, plan, row, events);
			let state = row?.state ?? new Uint8Array();
			const recorded: CoreEvent[] = [];
			const commands: CoreCommand[] = [];
			let res = { status: row?.status ?? 'running', output: row?.output ? JSON.parse(row.output) : undefined, error: row?.error ?? undefined };
			const apply = (ev: CoreEvent) => {
				const [s, r] = this.core.apply(plan.plan, runId, state, ev);
				if (r.ignored) return;
				state = s;
				recorded.push(ev);
				res = { status: r.status, output: r.output, error: r.error };
				for (const c of r.commands) {
					commands.push(c);
					// A real step's intent, committed with what dispatched it.
					if (c.kind === 'dispatch' && c.effect === 'real') apply({ kind: 'intent', at: ev.at, act: c.act, attempt: c.attempt });
				}
			};
			for (const ev of events) apply(ev);
			if (recorded.length === 0) return { events: [], endLeases, result: { existing: false, started: false, settled: false, commands, row } };
			const at = start?.at ?? this.now();
			const next: RunRow = {
				id: runId,
				plan: plan.name,
				hash: plan.hash,
				state,
				input: row ? row.input : JSON.stringify(events[0]?.data ?? null),
				status: res.status,
				output: res.output === undefined ? null : JSON.stringify(res.output),
				error: res.error || null,
				seq: 0, // set by the store
				createdAt: row?.createdAt ?? at,
				updatedAt: at,
				parent: row ? row.parent : (start?.parent ?? null),
				workflow: row ? row.workflow : (start?.workflow ?? null),
				meta: row ? row.meta : start?.meta !== undefined ? JSON.stringify(start.meta) : null,
			};
			const setTimers: TimerRow[] = [];
			const deleteTimers: number[] = [];
			const setLeases: LeaseRow[] = [];
			for (const c of commands) {
				if (c.kind === 'timer') setTimers.push({ run: runId, timer: c.timer!, act: c.act ?? 0, at: c.at! });
				if (c.kind === 'cancel_timer') deleteTimers.push(c.timer!);
				// Dispatched to this process: leased to it while it runs.
				if (c.kind === 'dispatch') setLeases.push({ run: runId, act: c.act!, attempt: c.attempt ?? 0, owner: this.owner, until: at + this.leaseMs });
			}
			const done = DONE.has(res.status);
			// Driven by this process from its start (ADR 0059).
			if (!row && start?.drive && !done) setLeases.push({ run: runId, act: 0, attempt: start.drive, owner: this.owner, until: at + this.leaseMs });
			const settled = SETTLED.has(res.status) && res.status !== row?.status;
			// Its workflow is to be driven on: by this process, or, if it
			// stops first, by whichever takes the lease up (ADR 0059).
			if (settled && next.parent && this.leaseParents)
				setLeases.push({ run: next.parent, act: 0, attempt: newToken(), owner: this.owner, until: at + this.leaseMs });
			return {
				notify: settled,
				events: recorded,
				row: next,
				setTimers,
				deleteTimers,
				clearTimers: done,
				endLeases,
				setLeases,
				result: { existing: false, started: !row, settled, commands, row: next },
			};
		});
		if (this.closed) return out.existing;
		const row = out.row;
		if (out.started && row) this.observe({ kind: 'run.started', runId, ...runAttrs(row) });
		for (const c of out.commands) this.carryOut(runId, c);
		// Settled by this transaction (not a run found settled already).
		if (out.settled && row) {
			this.observe({ kind: 'run.settled', runId, ...runAttrs(row), status: row.status, ...(row.error ? { error: row.error } : {}) });
			const r = info(row);
			for (const w of this.waiters.get(runId) ?? []) w(r);
			for (const h of this.settledHooks) h(r);
		}
		// Along the way: what no process is doing (at most once a lease period).
		if (this.now() - this.lastSweep >= this.leaseMs) this.background(this.tick(), 'kairo: sweeping');
		return out.existing;
	}

	/**
	 * Throws unless run row, which started under another version of plan,
	 * may go on under the current one (ADR 0060). Only the SDK's own plans
	 * (one node each) may; and not while a real attempt is out whose action
	 * the current plan does not treat as real (it would be retried on an
	 * unknown outcome, invariant 5), unless the events are safe under any
	 * settings (an outcome that is in, cancel, resolve).
	 */
	private adoptable(runId: string, plan: Compiled, row: RunRow, events: CoreEvent[]): void {
		if (!sdkPlan(plan.name)) throw new KairoError(409, `run ${runId}: plan ${row.plan} changed since it started`);
		if (events.length > 0 && events.every((e) => SAFE.has(e.kind))) return;
		if (this.effectWeakened(plan, row)) throw new EffectWeakenedError(`run ${runId}: ${EFFECT_WEAKENED} (${row.plan})`);
	}

	/** Run row, of an SDK plan, has a real attempt out whose action plan does not treat as real (ADR 0060). */
	private effectWeakened(plan: Compiled, row: RunRow): boolean {
		return sdkPlan(plan.name) && !strictlyReal(plan) && this.core.inspect(row.state).intent_durable;
	}

	private carryOut(runId: string, c: CoreCommand): void {
		const key = `${runId}\0${c.act ?? 0}`;
		switch (c.kind) {
			case 'dispatch':
				this.background(this.dispatch(runId, c), "kairo: applying a step's outcome", { run: runId });
				return;
			case 'timer': {
				if (this.manualTimers) return; // fired by tick only
				const tk = `${runId}\0t${c.timer}`;
				clearTimeout(this.timers.get(tk));
				const t: TimerRow = { run: runId, timer: c.timer!, act: c.act ?? 0, at: c.at! };
				this.timers.set(tk, setTimeout(() => {
					this.timers.delete(tk);
					if (!this.closed) this.background(this.fire(t), 'kairo: firing a timer', { run: runId });
				}, Math.max(0, c.at! - this.now())));
				return;
			}
			case 'cancel_timer': {
				const tk = `${runId}\0t${c.timer}`;
				clearTimeout(this.timers.get(tk));
				this.timers.delete(tk);
				return;
			}
			case 'abort':
				this.running.get(key)?.abort();
				return;
		}
	}

	private async fire(t: TimerRow): Promise<void> {
		await this.process(t.run, [{ kind: 'timer', at: this.now(), act: t.act, timer: t.timer }]);
	}

	/** Runs a dispatched step here and applies its outcome. */
	private async dispatch(runId: string, c: CoreCommand): Promise<void> {
		if (!this.handler) return;
		const key = `${runId}\0${c.act}`;
		const abort = new AbortController();
		this.running.set(key, abort);
		this.renew();
		const task = {
			run_id: runId,
			step_id: c.step_id ?? '',
			act: c.act ?? 0,
			attempt: c.attempt ?? 0,
			idempotency_key: c.idempotency_key ?? '',
			action: c.action ?? '',
			input: c.input ?? null,
		} as Task;
		const step = { runId, action: task.action, stepId: task.step_id, attempt: task.attempt };
		this.observe({ kind: 'step.started', ...step });
		const began = performance.now();
		let res: Result;
		try {
			res = await this.handler(task, { signal: abort.signal, emit: () => {} });
		} catch (e) {
			res = { error: String((e as Error)?.message ?? e), errorType: (e as Error)?.name };
		} finally {
			this.running.delete(key);
			this.renew();
		}
		this.observe({ kind: 'step.finished', ...step, status: stepStatus(res), ...(res.error ? { error: res.error } : {}), duration: performance.now() - began });
		if (this.closed) return;
		if (res.pending) {
			// It runs elsewhere (ADR 0052): its lease goes there, until its
			// outcome comes (complete) or the lease expires. An outcome that
			// came first ended the lease; then nothing is handed over.
			const lease = { run: runId, act: c.act!, attempt: c.attempt ?? 0, owner: res.pending.owner, until: this.now() + res.pending.leaseMs };
			await this.store.handOver(lease).catch((e) => this.logger.warn('kairo: handing over a lease', { run: runId, err: e }));
			return;
		}
		await this.process(runId, [outcome(res, c.act!, c.attempt ?? 0, this.now())]);
	}

	/** When something is next to do: the earliest timer or lease expiry (ADR 0053). */
	nextWake(): Promise<number | null> {
		return this.store.nextWake();
	}

	/** Applies the outcome of a step that ran elsewhere (ADR 0052). A stale one is ignored. */
	async complete(runId: string, act: number, attempt: number, res: Result): Promise<void> {
		this.observe({ kind: 'step.finished', runId, attempt, status: stepStatus(res), ...(res.error ? { error: res.error } : {}) });
		await this.process(runId, [outcome(res, act, attempt, this.now())]);
	}
}

/** The event of a step's outcome. */
function outcome(res: Result, act: number, attempt: number, at: number): CoreEvent {
	if (res.error !== undefined || res.unknown) {
		return { kind: 'step_err', at, act, attempt, error: res.error || 'outcome unknown', retryable: !!res.retryable, unknown: !!res.unknown,
			...(res.errorType ? { error_type: res.errorType } : {}) };
	}
	if (res.wait) return { kind: 'step_wait', at, act, attempt, deadline: Math.floor(res.wait.until), data: res.wait.output ?? null };
	return { kind: 'step_ok', at, act, attempt, data: res.output ?? null };
}

/** What observations of a run say of it. */
function runAttrs(row: RunRow): { parent?: string; plan: string; workflow?: string } {
	return { plan: row.plan, ...(row.parent ? { parent: row.parent } : {}), ...(row.workflow ? { workflow: row.workflow } : {}) };
}

/** What a new run starts with, besides its plan and input. */
export interface RunStart {
	runId: string;
	vars?: Record<string, unknown>;
	/** The run that makes this one: kept and removed with it (ADR 0054). */
	parent?: string;
	/** A workflow run's workflow, and what the application gave it (ADR 0059). Set once. */
	workflow?: string;
	meta?: Record<string, unknown>;
	/** A workflow run: its drive lease for this process, set with its start (a token; ADR 0059). */
	drive?: number;
	/** Events applied in the start's transaction (a signal received before its wait, ADR 0059). */
	then?: Array<Omit<CoreEvent, 'at'>>;
}

/** A drive lease's token: a nonzero int32 (ADR 0059). */
export function newToken(): number {
	for (;;) {
		const t = randomInt(-0x80000000, 0x80000000);
		if (t !== 0) return t;
	}
}

/** Events applied under the current plan whatever it is (ADR 0060): an outcome that is in, cancel, resolve. */
const SAFE = new Set(['cancel', 'resolve', 'step_ok', 'step_wait']);

/** The SDK's own plans (one node each), which go on under the current version (ADR 0060). */
function sdkPlan(name: string): boolean {
	return name === PLAN_WORKFLOW || name.startsWith('kairo.call/') || name.startsWith('kairo.wait/');
}

/** Every action of plan is real and not retried on an unknown outcome (ADR 0060). */
function strictlyReal(plan: Compiled): boolean {
	const effects = Object.entries(plan.effects ?? {});
	return effects.length > 0 && effects.every(([a, e]) => e === 'real' && !plan.idempotent?.[a]);
}

/** A run whose plan this process does not have is another process's to take up. */
function skipUnknownPlan(e: unknown): void {
	if (!(e instanceof KairoError && e.status === 404)) throw e;
}

function info(row: RunRow): RunInfo {
	// A workflow run's version (ADR 0060).
	const version = row.plan === PLAN_WORKFLOW && row.input !== null ? (JSON.parse(row.input) as { version?: unknown } | null)?.version : undefined;
	return {
		run_id: row.id,
		plan: row.plan,
		tenant: 'default',
		status: row.status,
		...(row.input !== null ? { input: JSON.parse(row.input) } : {}),
		...(row.output !== null ? { output: JSON.parse(row.output) } : {}),
		...(row.error ? { error: row.error } : {}),
		...(row.parent ? { parent: row.parent } : {}),
		...(row.workflow ? { workflow: row.workflow } : {}),
		...(typeof version === 'string' && version ? { version } : {}),
		...(row.meta !== null ? { meta: JSON.parse(row.meta) } : {}),
		createdAt: row.createdAt,
		updatedAt: row.updatedAt,
	};
}
