// Where workflows written as code run their calls (ADR 0049, 0051): kairod
// over HTTP and the worker protocol, or the runtime embedded in this
// process with a database.

import { Client, type NodeSpec, type RunInfo } from './client.ts';
import { Embedded, type ActionHandler, type EmbeddedOptions, type RunStart } from './embedded.ts';
import type { Result } from './protocol.ts';
import { Worker, type Address } from './worker.ts';

export interface Backend {
	/** Registers the actions, and runs their steps with serve (here, or on a worker). */
	start(specs: NodeSpec[], serve: ActionHandler): Promise<void>;
	registerPlan(definition: { name: string; [k: string]: unknown }): Promise<void>;
	/** Starts a run, or finds it: the run id is an idempotency key. Runs are durable. */
	/** parent: the run that makes this one (the embedded runtime keeps and removes them together, ADR 0054). */
	/** workflow, meta, drive and then: the embedded runtime only (ADR 0059); kairod ignores them. */
	run(plan: string, input: unknown, opts: RunStart): Promise<{ run_id: string; existing: boolean }>;
	/** Keeps a workflow's calls while it runs, however long (the embedded runtime, ADR 0054). */
	readonly keepsCalls?: boolean;
	/** Throws KairoError 404 for a run that does not exist. */
	get(runId: string): Promise<RunInfo>;
	wait(runId: string, signal?: AbortSignal): Promise<RunInfo>;
	signal(runId: string, name: string, payload: unknown): Promise<void>;
	cancel(runId: string): Promise<void>;
	close(): Promise<void>;
	/** Embedded only: once the work started here is done. */
	idle?(): Promise<void>;
	/** Embedded only: calls hook with each run that settles here. */
	onSettled?(hook: (r: RunInfo) => void): () => void;
	/** Embedded only: takes up due timers and steps whose process stopped. */
	tick?(): Promise<void>;
	/** Embedded only: when something is next to do (ADR 0053). */
	nextWake?(): Promise<number | null>;
	/** Embedded only: the outcome of a step that ran elsewhere (ADR 0052). */
	complete?(runId: string, act: number, attempt: number, result: Result): Promise<void>;
}

export interface HttpBackendOptions {
	/** kairod's HTTP API. */
	url?: string;
	/** kairod's worker socket (a UNIX socket path, or {host, port}). */
	worker?: Address;
	token?: string;
	concurrency?: number;
}

/** kairod: its HTTP API, and a worker that pulls this process's actions. */
export class HttpBackend implements Backend {
	readonly client: Client;
	private readonly opts: HttpBackendOptions;
	private worker?: Worker;

	constructor(opts: HttpBackendOptions = {}) {
		this.opts = opts;
		this.client = new Client(opts.url);
	}

	async start(specs: NodeSpec[], serve: ActionHandler): Promise<void> {
		await this.client.registerNodes(specs);
		if (this.opts.worker === undefined) return;
		this.worker = new Worker({
			name: `kairo-sdk-${process.pid}`,
			actions: specs.map((s) => s.action),
			token: this.opts.token,
			concurrency: this.opts.concurrency,
			handler: serve,
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

	async registerPlan(definition: { name: string; [k: string]: unknown }): Promise<void> {
		await this.client.registerPlan(definition);
	}

	run(plan: string, input: unknown, opts: RunStart) {
		// Kept after they finish (ADR 0050): a workflow resumed after kairod
		// restarts still finds its finished calls' results.
		return this.client.run(plan, input, { runId: opts.runId, tier: 'file', vars: opts.vars, keepOutput: true });
	}

	get(runId: string) {
		return this.client.get(runId);
	}

	wait(runId: string, signal?: AbortSignal) {
		return this.client.wait(runId, signal);
	}

	signal(runId: string, name: string, payload: unknown) {
		return this.client.signal(runId, name, payload);
	}

	cancel(runId: string) {
		return this.client.cancel(runId);
	}

	async close(): Promise<void> {
		const w = this.worker;
		this.worker = undefined;
		w?.stop();
	}
}

/** The runtime embedded in this process, with a database (ADR 0051). */
export class EmbeddedBackend implements Backend {
	readonly runtime: Embedded;

	private constructor(runtime: Embedded) {
		this.runtime = runtime;
	}

	static async open(opts: EmbeddedOptions): Promise<EmbeddedBackend> {
		return new EmbeddedBackend(await Embedded.open(opts));
	}

	async start(specs: NodeSpec[], serve: ActionHandler): Promise<void> {
		this.runtime.registerActions(specs, serve);
	}

	async registerPlan(definition: { name: string; [k: string]: unknown }): Promise<void> {
		this.runtime.registerPlan(definition);
	}

	readonly keepsCalls = true;

	run(plan: string, input: unknown, opts: RunStart) {
		return this.runtime.run(plan, input, opts);
	}

	get(runId: string) {
		return this.runtime.get(runId);
	}

	wait(runId: string, signal?: AbortSignal) {
		return this.runtime.wait(runId, signal);
	}

	signal(runId: string, name: string, payload: unknown) {
		return this.runtime.signal(runId, name, payload);
	}

	cancel(runId: string) {
		return this.runtime.cancel(runId);
	}

	close(): Promise<void> {
		return this.runtime.close();
	}

	idle(): Promise<void> {
		return this.runtime.idle();
	}

	onSettled(hook: (r: RunInfo) => void): () => void {
		return this.runtime.onSettled(hook);
	}

	tick(): Promise<void> {
		return this.runtime.tick();
	}

	nextWake(): Promise<number | null> {
		return this.runtime.nextWake();
	}

	complete(runId: string, act: number, attempt: number, result: Result): Promise<void> {
		return this.runtime.complete(runId, act, attempt, result);
	}
}
