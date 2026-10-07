// Where workflows written as code run their calls (ADR 0049, 0051): kairod
// over HTTP and the worker protocol, or the runtime embedded in this
// process with a database.

import { Client, type NodeSpec, type RunInfo } from './client.ts';
import { Embedded, type ActionHandler, type EmbeddedOptions } from './embedded.ts';
import { Worker, type Address } from './worker.ts';

export interface Backend {
	/** Registers the actions, and runs their steps with serve (here, or on a worker). */
	start(specs: NodeSpec[], serve: ActionHandler): Promise<void>;
	registerPlan(definition: { name: string; [k: string]: unknown }): Promise<void>;
	/** Starts a run, or finds it: the run id is an idempotency key. Runs are durable. */
	run(plan: string, input: unknown, opts: { runId: string; vars?: Record<string, unknown> }): Promise<{ run_id: string; existing: boolean }>;
	/** Throws KairoError 404 for a run that does not exist. */
	get(runId: string): Promise<RunInfo>;
	wait(runId: string, signal?: AbortSignal): Promise<RunInfo>;
	signal(runId: string, name: string, payload: unknown): Promise<void>;
	cancel(runId: string): Promise<void>;
	close(): Promise<void>;
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

	run(plan: string, input: unknown, opts: { runId: string; vars?: Record<string, unknown> }) {
		return this.client.run(plan, input, { runId: opts.runId, tier: 'file', vars: opts.vars });
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

	run(plan: string, input: unknown, opts: { runId: string; vars?: Record<string, unknown> }) {
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
}
