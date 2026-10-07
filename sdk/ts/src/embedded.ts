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
import { randomUUID } from 'node:crypto';

import type { LeaseRow, RunRow, Store, TimerRow } from './store.ts';
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
}

const DONE = new Set(['completed', 'failed', 'cancelled']);
/** A run that will not go on by itself: finished, or stopped for review. */
const SETTLED = new Set([...DONE, 'blocked']);
const OUTCOMES = new Set(['step_ok', 'step_err', 'step_wait']);

export class Embedded {
	private readonly core: Core;
	private readonly store: Store;
	private readonly now: () => number;
	private readonly plans = new Map<string, Compiled>();
	private handler?: ActionHandler;
	private readonly waiters = new Map<string, Set<(r: RunInfo) => void>>();
	private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
	private readonly running = new Map<string, AbortController>();
	/** Work started by this runtime (steps, timers): close waits for it. */
	private readonly busy = new Set<Promise<unknown>>();
	private closed = false;
	readonly owner: string;
	private readonly leaseMs: number;
	private renewal?: ReturnType<typeof setInterval>;
	private lastSweep = 0;
	private unlisten?: () => Promise<void>;

	private constructor(core: Core, opts: EmbeddedOptions) {
		this.core = core;
		this.store = opts.store;
		this.now = opts.now ?? Date.now;
		this.owner = opts.owner ?? randomUUID();
		this.leaseMs = opts.leaseMs ?? 30_000;
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

	/**
	 * Starts a run of plan, or finds it: a run id is an idempotency key, and
	 * a run that exists (running or finished) is not started again.
	 */
	async run(plan: string, input: unknown, opts: { runId: string; vars?: Record<string, unknown> }): Promise<{ run_id: string; existing: boolean }> {
		const p = this.plans.get(plan);
		if (!p) throw new KairoError(404, `no plan ${plan}`);
		const at = this.now();
		const ev: CoreEvent = { kind: 'start', at, data: input ?? null, ...(opts.vars ? { vars: opts.vars } : {}) };
		const existing = await this.process(opts.runId, [ev], { plan: p, at });
		return { run_id: opts.runId, existing };
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
	 * Takes up what no process is doing: steps whose lease expired (their
	 * process stopped) and timers that are due (also those armed by other
	 * processes). Runs whose plan is not registered here are left alone.
	 */
	async tick(): Promise<void> {
		this.lastSweep = this.now();
		for (const l of await this.store.expiredLeases(this.now(), 1000)) await this.recover(l).catch(skipUnknownPlan);
		for (const t of await this.store.dueTimers(this.now(), 1000)) await this.fire(t).catch(skipUnknownPlan);
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
	}

	/** Renews this process's leases while it runs steps; one timer for all of them. */
	private renew(): void {
		if (this.running.size > 0 && !this.closed) {
			this.renewal ??= setInterval(() => {
				this.track(this.store.renewLeases(this.owner, this.now() + this.leaseMs));
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
	private async process(runId: string, events: CoreEvent[], start?: { plan: Compiled; at: number }): Promise<boolean> {
		const out = await this.store.withRun(runId, (row) => {
			// A step's outcome ends its lease, applied or not (a stale one).
			const endLeases = events.filter((e) => OUTCOMES.has(e.kind) && e.act !== undefined).map((e) => e.act!);
			if (row && start) return { events: [], result: { existing: true, commands: [] as CoreCommand[], row } };
			if (!row && !start) return { events: [], endLeases, result: { existing: false, commands: [] as CoreCommand[], row: undefined } };
			const plan = start ? start.plan : this.plans.get(row!.plan);
			if (!plan) throw new KairoError(404, `run ${runId}: plan ${row!.plan} is not registered here`);
			if (row && row.hash !== plan.hash) throw new KairoError(409, `run ${runId}: plan ${row.plan} changed since it started`);
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
			if (recorded.length === 0) return { events: [], endLeases, result: { existing: false, commands, row } };
			const at = start?.at ?? this.now();
			const next: RunRow = {
				id: runId,
				plan: plan.name,
				hash: plan.hash,
				state,
				status: res.status,
				output: res.output === undefined ? null : JSON.stringify(res.output),
				error: res.error || null,
				seq: 0, // set by the store
				createdAt: row?.createdAt ?? at,
				updatedAt: at,
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
			return {
				notify: SETTLED.has(res.status) && res.status !== row?.status,
				events: recorded,
				row: next,
				setTimers,
				deleteTimers,
				clearTimers: done,
				endLeases,
				setLeases,
				result: { existing: false, commands, row: next },
			};
		});
		if (this.closed) return out.existing;
		for (const c of out.commands) this.carryOut(runId, c);
		if (out.row && SETTLED.has(out.row.status)) {
			const r = info(out.row);
			for (const w of this.waiters.get(runId) ?? []) w(r);
		}
		// Along the way: what no process is doing (at most once a lease period).
		if (this.now() - this.lastSweep >= this.leaseMs) this.track(this.tick());
		return out.existing;
	}

	private carryOut(runId: string, c: CoreCommand): void {
		const key = `${runId}\0${c.act ?? 0}`;
		switch (c.kind) {
			case 'dispatch':
				this.track(this.dispatch(runId, c));
				return;
			case 'timer': {
				const tk = `${runId}\0t${c.timer}`;
				clearTimeout(this.timers.get(tk));
				const t: TimerRow = { run: runId, timer: c.timer!, act: c.act ?? 0, at: c.at! };
				this.timers.set(tk, setTimeout(() => {
					this.timers.delete(tk);
					if (!this.closed) this.track(this.fire(t));
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
		let res: Result;
		try {
			res = await this.handler(task, { signal: abort.signal, emit: () => {} });
		} catch (e) {
			res = { error: String((e as Error)?.message ?? e), errorType: (e as Error)?.name };
		} finally {
			this.running.delete(key);
			this.renew();
		}
		if (this.closed) return;
		const at = this.now();
		let ev: CoreEvent;
		if (res.error !== undefined || res.unknown) {
			ev = { kind: 'step_err', at, act: c.act, attempt: c.attempt, error: res.error || 'outcome unknown', retryable: !!res.retryable,
				unknown: !!res.unknown, ...(res.errorType ? { error_type: res.errorType } : {}) };
		} else if (res.wait) {
			ev = { kind: 'step_wait', at, act: c.act, attempt: c.attempt, deadline: Math.floor(res.wait.until), data: res.wait.output ?? null };
		} else {
			ev = { kind: 'step_ok', at, act: c.act, attempt: c.attempt, data: res.output ?? null };
		}
		await this.process(runId, [ev]);
	}
}

/** A run whose plan this process does not have is another process's to take up. */
function skipUnknownPlan(e: unknown): void {
	if (!(e instanceof KairoError && e.status === 404)) throw e;
}

function info(row: RunRow): RunInfo {
	return {
		run_id: row.id,
		plan: row.plan,
		tenant: 'default',
		status: row.status,
		...(row.output !== null ? { output: JSON.parse(row.output) } : {}),
		...(row.error ? { error: row.error } : {}),
	};
}
