// Workflows written as code (ADR 0049). A workflow is a function that runs
// in this process; each call it makes (an action, a wait, a clock read)
// is a run of its own in kairo, whose id comes from the call: running the
// function again with the same workflow id returns the calls that
// finished, and runs nothing twice.

import { createHash, randomUUID } from 'node:crypto';

import { Client, KairoError, finished, type EffectName, type NodeSpec, type RunInfo } from './client.ts';
import type { Result, Task } from './protocol.ts';
import { Worker, type Address, type TaskContext } from './worker.ts';

/** An action: what a step of a workflow runs, on this process's worker. */
export interface ActionDef<I = any, O = any> {
	/** "real" (the default) acts outside and never runs twice; "unprotected" may run again. */
	effect?: EffectName;
	handler: (input: I, ctx: TaskContext) => Promise<O>;
	/** How long a call may run before its outcome is unknown (e.g. "5m"). */
	timeout?: string;
	/** The rate-limit key (default: the action). */
	destination?: string;
}

export type WorkflowFn<I = any, O = any> = (ctx: Context, input: I) => Promise<O>;

export interface KairoOptions {
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
}

/** The workflow ran into a call whose result kairo no longer keeps. */
export class ResultLostError extends Error {}
/** The workflow was cancelled. */
export class CancelledError extends Error {}

const PLAN_CALL = 'kairo.call/';
const PLAN_WAIT = 'kairo.wait/';
const PLAN_WORKFLOW = 'kairo.workflow';
const BUILTIN = { now: 'kairo.now', random: 'kairo.random', sleep: 'kairo.sleep' };

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
	readonly client: Client;
	private readonly opts: KairoOptions;
	private readonly actions = new Map<string, ActionDef>();
	private readonly workflows = new Map<string, WorkflowFn>();
	private readonly planned = new Set<string>();
	private readonly driving = new Set<AbortController>();
	private worker?: Worker;

	constructor(opts: KairoOptions = {}) {
		this.opts = opts;
		this.client = new Client(opts.url);
	}

	defineAction<I, O>(name: string, def: ActionDef<I, O>): void {
		if (name.startsWith('kairo.')) throw new Error(`action ${name}: names beginning with "kairo." are kairo's`);
		this.actions.set(name, def);
	}

	workflow<I, O>(name: string, fn: WorkflowFn<I, O>): void {
		this.workflows.set(name, fn);
	}

	/** Registers the actions and their plans with kairod, and serves the actions on its worker socket. */
	async start(): Promise<void> {
		const specs: NodeSpec[] = [...this.actions].map(([action, def]) => ({
			action,
			effect: def.effect ?? 'real',
			...(def.timeout ? { timeout: def.timeout } : {}),
			...(def.destination ? { destination: def.destination } : {}),
		}));
		for (const action of Object.values(BUILTIN)) specs.push({ action, effect: 'unprotected' });
		await this.client.registerNodes(specs);
		for (const { action } of specs) await this.plan(PLAN_CALL + action, { kind: 'step', id: 'call', action, input: { in: '$input.in' } });
		await this.plan(PLAN_WORKFLOW, { kind: 'wait', id: 'done', signal: 'done' }, { started_at: { type: 'integer', value: 0 } });
		if (this.opts.worker !== undefined) {
			this.worker = new Worker({
				name: `kairo-sdk-${process.pid}`,
				actions: specs.map((s) => s.action),
				token: this.opts.token,
				concurrency: this.opts.concurrency,
				handler: (task, ctx) => this.serve(task, ctx),
			});
			const addr = this.opts.worker;
			void (async () => {
				while (this.worker) {
					try {
						await this.worker.run(addr);
					} catch {
						// kairod went away: connect again.
					}
					if (this.worker) await new Promise((r) => setTimeout(r, 500));
				}
			})();
		}
	}

	/**
	 * Stops serving actions and driving workflows, as a process that stops
	 * does: the workflows it drove stay unfinished in kairo, to be resumed.
	 */
	async close(): Promise<void> {
		for (const a of this.driving) a.abort();
		const w = this.worker;
		this.worker = undefined;
		w?.stop();
	}

	private async plan(name: string, root: unknown, vars?: Record<string, unknown>): Promise<void> {
		if (this.planned.has(name)) return;
		await this.client.registerPlan({ name, root, ...(vars ? { vars } : {}) });
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
		return { output: (await def.handler(input, ctx)) ?? null };
	}

	/**
	 * Runs workflow name as execution id, or resumes it: calls that
	 * finished return their recorded results. Returns its result.
	 */
	async run<O = any>(name: string, input: unknown, opts: { id?: string } = {}): Promise<O> {
		return (await this.runAs(name, input, opts.id ?? randomUUID())) as O;
	}

	/** Sends a signal to the first wait for it in workflow id that has not received one. */
	async signal(id: string, name: string, payload: unknown = null): Promise<void> {
		for (let n = 0; ; n++) {
			const runId = callId(id, PLAN_WAIT + name, null, n);
			let r: RunInfo;
			try {
				r = await this.client.get(runId);
			} catch (e) {
				if (e instanceof KairoError && e.status === 404) throw new Error(`workflow ${id} does not wait for ${name}`);
				throw e;
			}
			if (finished(r)) continue;
			await this.client.signal(runId, name, payload);
			return;
		}
	}

	/** Cancels workflow id: the calls it is waiting for are cancelled with it. */
	async cancel(id: string): Promise<void> {
		await this.client.cancel(id);
	}

	private async runAs(name: string, input: unknown, id: string, parent?: AbortSignal): Promise<unknown> {
		const fn = this.workflows.get(name);
		if (!fn) throw new Error(`no workflow ${name}`);
		await this.plan(PLAN_WORKFLOW, { kind: 'wait', id: 'done', signal: 'done' }, { started_at: { type: 'integer', value: 0 } });
		const started = await this.client.run(PLAN_WORKFLOW, { workflow: name }, { runId: id, tier: 'file', vars: { started_at: Date.now() } });
		const info = await this.client.get(id);
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
		// The workflow's own run ends when it is cancelled: stop the calls.
		void this.client.wait(id, abort.signal).then((r) => {
			if (r.status === 'cancelled') abort.abort();
		}, () => {});
		const ctx = new Context(this, id, abort.signal);
		try {
			const value = await fn(ctx, input);
			await this.client.signal(id, 'done', { ok: true, value: value ?? null });
			return value;
		} catch (e) {
			if (abort.signal.aborted) throw new CancelledError(`workflow ${id} cancelled`);
			await this.client.signal(id, 'done', { ok: false, error: String((e as Error)?.message ?? e) }).catch(() => {});
			throw e;
		} finally {
			parent?.removeEventListener('abort', stop);
			this.driving.delete(abort);
			abort.abort();
		}
	}

	/** One call: a run of its own, found again by its id. */
	async callRun(plan: string, root: unknown | null, input: unknown, runId: string, signal: AbortSignal): Promise<unknown> {
		if (root) await this.plan(plan, root);
		await this.client.run(plan, { in: input }, { runId, tier: 'file' });
		let r: RunInfo;
		try {
			r = await this.client.wait(runId, signal);
		} catch (e) {
			if (signal.aborted) {
				await this.client.cancel(runId).catch(() => {});
				throw new CancelledError(`call ${runId} cancelled`);
			}
			throw e;
		}
		if (r.trimmed) throw new ResultLostError(`call ${runId} finished, but kairo no longer keeps its result`);
		if (r.status !== 'completed') throw new Error(`call ${runId} ${r.status}: ${r.error ?? ''}`);
		return r.output;
	}

	async childWorkflow(name: string, input: unknown, id: string, signal: AbortSignal): Promise<unknown> {
		return this.runAs(name, input, id, signal);
	}
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
		const out = (await this.k.callRun(PLAN_WAIT + name, { kind: 'wait', id: 'w', signal: name }, null, id, this.signal)) as {
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
