// The embedded runtime (ADR 0051): kairo without a resident server. The
// pure core (WASM) runs in this process; runs live in a database (Store).
// Each event is one transaction: lock the run, apply the event, append it,
// replace the state, arm or disarm timers. The commands are carried out
// after the commit: a real step only once its intent was committed with it
// (invariant 4).

import { fileURLToPath } from 'node:url';

import { Core, type Compiled, type CoreCommand, type CoreEvent } from './core.ts';
import { KairoError, type NodeSpec, type RunInfo } from './client.ts';
import type { Result, Task } from './protocol.ts';
import type { RunRow, Store, TimerRow } from './store.ts';
import type { TaskContext } from './worker.ts';

/** Runs the actions of dispatched steps in this process. */
export type ActionHandler = (task: Task, ctx: TaskContext) => Promise<Result>;

export interface EmbeddedOptions {
	store: Store;
	/** kairo.wasm (cmd/kairo-wasm): a path or its bytes. Default: the one in this package (npm run build:wasm). */
	wasm?: string | Uint8Array;
	now?: () => number;
}

const DONE = new Set(['completed', 'failed', 'cancelled']);

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

	private constructor(core: Core, opts: EmbeddedOptions) {
		this.core = core;
		this.store = opts.store;
		this.now = opts.now ?? Date.now;
	}

	static async open(opts: EmbeddedOptions): Promise<Embedded> {
		const core = await Core.load(opts.wasm ?? fileURLToPath(new URL('../wasm/kairo.wasm', import.meta.url)));
		await opts.store.init();
		return new Embedded(core, opts);
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

	/** Waits until the run has finished (in this process's view). */
	async wait(runId: string, signal?: AbortSignal): Promise<RunInfo> {
		let resolve!: (r: RunInfo) => void;
		const done = new Promise<RunInfo>((r) => (resolve = r));
		let set = this.waiters.get(runId);
		if (!set) this.waiters.set(runId, (set = new Set()));
		set.add(resolve);
		try {
			// Registered first, then read: an end between the two still wakes us.
			const now = await this.get(runId);
			if (DONE.has(now.status)) return now;
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

	/** Fires the timers that are due (also those armed by other processes). */
	async tick(): Promise<void> {
		for (const t of await this.store.dueTimers(this.now(), 1000)) await this.fire(t);
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
		while (this.busy.size > 0) await Promise.allSettled([...this.busy]);
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
			if (row && start) return { events: [], result: { existing: true, commands: [] as CoreCommand[], row } };
			if (!row && !start) return { events: [], result: { existing: false, commands: [] as CoreCommand[], row: undefined } };
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
			if (recorded.length === 0) return { events: [], result: { existing: false, commands, row } };
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
			for (const c of commands) {
				if (c.kind === 'timer') setTimers.push({ run: runId, timer: c.timer!, act: c.act ?? 0, at: c.at! });
				if (c.kind === 'cancel_timer') deleteTimers.push(c.timer!);
			}
			const done = DONE.has(res.status);
			return { events: recorded, row: next, setTimers, deleteTimers, clearTimers: done, result: { existing: false, commands, row: next } };
		});
		if (this.closed) return out.existing;
		for (const c of out.commands) this.carryOut(runId, c);
		if (out.row && DONE.has(out.row.status)) {
			const r = info(out.row);
			for (const w of this.waiters.get(runId) ?? []) w(r);
		}
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
