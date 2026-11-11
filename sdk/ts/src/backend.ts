// Where workflows written as code run their calls (ADR 0049, 0051): kairod
// over HTTP and the worker protocol, or the runtime embedded in this
// process with a database.

import { Client, finished, KairoError, KairoTransportError, type NodeSpec, type RunInfo } from './client.ts';
import { Embedded, type ActionHandler, type EmbeddedOptions, type RunStart } from './embedded.ts';
import type { Result } from './protocol.ts';
import { Worker, type Address } from './worker.ts';
import { ChildrenUnsettledError, ParentUnavailableError } from './store.ts';
import { MetricEmitter, MetricName, type MetricSink } from './metrics.ts';

export interface Backend {
	/** Registers the actions, and runs their steps with serve (here, or on a worker). */
	start(specs: NodeSpec[], serve: ActionHandler): Promise<void>;
	registerPlan(definition: { name: string; [k: string]: unknown }): Promise<void>;
	/** Starts a run, or finds it: the run id is an idempotency key. Runs are durable. */
	/** parent: the unfinished workflow that owns this run (ADR 0054, 0067). */
	/** workflow and meta identify root code workflows; drive and then are embedded-only. */
	run(plan: string, input: unknown, opts: RunStart): Promise<{ run_id: string; existing: boolean }>;
	/** Keeps a workflow's calls while it runs, however long (the embedded runtime, ADR 0054). */
	readonly keepsCalls?: boolean;
	/** Throws KairoError 404 for a run that does not exist. With input: the run's input too (ADR 0060). */
	get(runId: string, opts?: { input?: boolean }): Promise<RunInfo>;
	wait(runId: string, signal?: AbortSignal): Promise<RunInfo>;
	waitQuiescent?(runId: string, signal?: AbortSignal): Promise<RunInfo>;
	signal(runId: string, name: string, payload: unknown): Promise<void>;
	children(runId: string): Promise<RunInfo[]>;
	list(opts?: { workflow?: string; status?: string; since?: number; until?: number; after?: string; limit?: number }): Promise<RunInfo[]>;
	finish?(runId: string, payload: unknown): Promise<void>;
	cancel(runId: string): Promise<void>;
	/** Settles a step stopped for review; an attempt out under weakened settings is embedded-only (ADR 0060, 0066). */
	resolve?(runId: string, output: unknown, error?: string): Promise<void>;
	drain?(signal?: AbortSignal): Promise<void>;
	close(): Promise<void>;
	/** Embedded only: once the work started here is done. */
	idle?(): Promise<void>;
	/** Embedded only: calls hook with each run that settles here. */
	onSettled?(hook: (r: RunInfo) => void): () => void;
	/** Embedded only: takes up due timers and steps whose process stopped. */
	tick?(): Promise<void>;
	/** Embedded only: when something is next to do (ADR 0053). */
	nextWake?(): Promise<number | null>;
	claimWorkflow?(workflows: Record<string, string[]>, runId?: string, timeoutMs?: number): Promise<DriveClaim | undefined>;
	releaseWorkflow?(claim: DriveClaim, retry?: boolean): Promise<void>;
	clearDue?(now: number): void;
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
	leaseMs?: number;
	metrics?: MetricSink;
}

export interface DriveClaim {
	run: RunInfo;
	token: string;
	until: number;
}

/** kairod: its HTTP API, and a worker that pulls this process's actions. */
export class HttpBackend implements Backend {
	readonly client: Client;
	private readonly opts: HttpBackendOptions;
	private worker?: Worker;
	private readonly owner = `kairo-ts-${process.pid}-${Math.random().toString(16).slice(2)}`;
	private readonly leaseMs: number;
	private readonly claims = new Map<string, DriveClaim>();
	private renewTimer?: ReturnType<typeof setInterval>;
	private nextAt: number | null = null;
	private closing = false;
	private draining = false;
	private readonly startAbort = new AbortController();
	private activeStarts = 0;
	private startIdleWaiters: Array<() => void> = [];
	private metrics?: MetricEmitter;

	constructor(opts: HttpBackendOptions = {}) {
		this.opts = opts;
		this.client = new Client(opts.url);
		this.leaseMs = opts.leaseMs ?? 30_000;
		if (opts.metrics) this.metrics = new MetricEmitter(opts.metrics);
	}

	setMetrics(metrics: MetricEmitter | undefined): void {
		this.metrics = metrics;
	}

	async start(specs: NodeSpec[], serve: ActionHandler): Promise<void> {
		await this.client.registerNodes(specs);
		this.renewTimer = setInterval(() => void this.renew(), Math.max(this.leaseMs / 3, 10));
		this.renewTimer.unref?.();
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
			while (this.worker && !this.draining) {
				try {
					await this.worker.run(addr);
				} catch {
					// kairod went away: connect again.
				}
				if (this.worker && !this.draining) await new Promise((r) => setTimeout(r, 500));
			}
		})();
	}

	async registerPlan(definition: { name: string; [k: string]: unknown }): Promise<void> {
		await this.client.registerPlan(definition);
	}

	async run(plan: string, input: unknown, opts: RunStart) {
		// Kept after they finish (ADR 0050): a workflow resumed after kairod
		// restarts still finds its finished calls' results.
		const signal = opts.signal ? AbortSignal.any([opts.signal, this.startAbort.signal]) : this.startAbort.signal;
		this.activeStarts++;
		const started = this.metrics ? performance.now() : 0;
		let outcome = 'failed';
		try {
			let delay = 50;
			let attempt = 0;
			for (;;) {
				if (signal.aborted) throw signal.reason ?? new Error(`run ${opts.runId}: stopped while confirming its start`);
				try {
					const result = await this.client.run(plan, input, {
					runId: opts.runId,
					tier: 'file',
					vars: opts.vars,
					keepOutput: true,
					parent: opts.parent,
					...(opts.parent ? {} : { workflow: opts.workflow, meta: opts.meta }),
					drive: !!opts.drive,
					signal,
				});
					outcome = 'confirmed';
					return result;
				} catch (e) {
					if (signal.aborted) throw signal.reason ?? e;
					if (e instanceof KairoError && e.status === 409) throw new ParentUnavailableError(e.message);
					if (!(e instanceof KairoTransportError) && !(e instanceof KairoError && (e.status === 504 || e.status === 429))) throw e;
					let cause = 'transport';
					if (e instanceof KairoError) cause = e.status === 429 ? 'overloaded' : 'unconfirmed';
					if (e instanceof KairoTransportError)
						this.metrics?.emit({ name: MetricName.runStartUnconfirmed, kind: 'counter', unit: '{event}', value: 1, labels: { cause: 'transport_ambiguity' } });
					else if (e.status === 504)
						this.metrics?.emit({ name: MetricName.runStartUnconfirmed, kind: 'counter', unit: '{event}', value: 1, labels: { cause: 'durability_timeout' } });
					this.metrics?.emit({ name: MetricName.runStartRetries, kind: 'counter', unit: '{event}', value: 1, labels: { cause } });
					await abortableDelay(retryDelay(delay, e instanceof KairoError ? e.retryAfterMs : undefined, opts.runId, attempt), signal);
				}
				attempt++;
				delay = Math.min(delay * 2, 1000);
			}
		} finally {
			if (signal.aborted) outcome = 'cancelled';
			this.metrics?.emit({
				name: MetricName.runStartConfirmation,
				kind: 'histogram',
				unit: 's',
				value: (performance.now() - started) / 1000,
				labels: { outcome },
			});
			this.activeStarts--;
			if (this.activeStarts === 0) {
				for (const resolve of this.startIdleWaiters.splice(0)) resolve();
			}
		}
	}

	private waitForStarts(signal?: AbortSignal): Promise<void> {
		if (this.activeStarts === 0) return Promise.resolve();
		return new Promise<void>((resolve, reject) => {
			const done = () => {
				signal?.removeEventListener('abort', stopped);
				resolve();
			};
			const stopped = () => {
				this.startIdleWaiters = this.startIdleWaiters.filter((r) => r !== done);
				reject(signal?.reason ?? new Error('stopped waiting for starts'));
			};
			this.startIdleWaiters.push(done);
			if (signal?.aborted) stopped();
			else signal?.addEventListener('abort', stopped, { once: true });
		});
	}

	private info(r: RunInfo): RunInfo {
		const { created_at, updated_at, ...rest } = r;
		return {
			...rest,
			...(created_at !== undefined ? { createdAt: created_at } : {}),
			...(updated_at !== undefined ? { updatedAt: updated_at } : {}),
		};
	}

	async get(runId: string, opts?: { input?: boolean }) {
		return this.info(await this.client.get(runId, opts));
	}

	async wait(runId: string, signal?: AbortSignal) {
		return this.info(await this.client.wait(runId, signal));
	}

	async waitQuiescent(runId: string, signal?: AbortSignal) {
		const r = this.info(await this.client.waitQuiescent(runId, signal));
		this.noteNext(typeof r.next_at === 'number' ? r.next_at : undefined);
		return r;
	}

	private noteNext(at: number | undefined): void {
		if (at && (this.nextAt === null || at < this.nextAt)) this.nextAt = at;
	}

	async claimWorkflow(workflows: Record<string, string[]>, runId?: string, timeoutMs = 0): Promise<DriveClaim | undefined> {
		if (this.draining) return undefined;
		const out = await this.client.claimWorkflow({ owner: this.owner, leaseMs: this.leaseMs, workflows, runId, timeoutMs });
		this.noteNext(out.next_at);
		if (out.claim && this.draining) {
			await this.releaseWorkflow(out.claim, true);
			return undefined;
		}
		if (out.claim) this.claims.set(out.claim.run.run_id, out.claim);
		return out.claim;
	}

	async releaseWorkflow(claim: DriveClaim, retry = false): Promise<void> {
		this.claims.delete(claim.run.run_id);
		try {
			await this.client.releaseWorkflow(claim.run.run_id, this.owner, claim.token, retry);
		} catch (e) {
			if (!(e instanceof KairoError && e.status === 409)) throw e;
		}
	}

	private async renew(): Promise<void> {
		for (const [id, claim] of [...this.claims]) {
			try {
				await this.client.renewWorkflow(id, this.owner, claim.token, this.leaseMs);
			} catch {
				this.claims.delete(id);
			}
		}
	}

	async nextWake(): Promise<number | null> {
		return this.nextAt;
	}

	clearDue(now: number): void {
		if (this.nextAt !== null && this.nextAt <= now) this.nextAt = null;
	}

	signal(runId: string, name: string, payload: unknown) {
		return this.client.signal(runId, name, payload);
	}

	async children(runId: string) {
		return (await this.client.children(runId)).map((r) => this.info(r));
	}

	async list(opts: { workflow?: string; status?: string; since?: number; until?: number; after?: string; limit?: number } = {}) {
		return (await this.client.list(opts)).map((r) => this.info(r));
	}

	async finish(runId: string, payload: unknown): Promise<void> {
		try {
			if (finished(await this.client.get(runId))) return;
			await this.client.finish(runId, payload);
		} catch (e) {
			if (e instanceof KairoError && e.status === 404) {
				try {
					if (finished(await this.client.get(runId))) return;
				} catch {}
			}
			if (e instanceof KairoError && e.status === 409) throw new ChildrenUnsettledError(e.message);
			throw e;
		}
	}

	async cancel(runId: string): Promise<void> {
		try {
			await this.client.cancel(runId);
		} catch (e) {
			if (e instanceof KairoError && e.status === 409) throw new ChildrenUnsettledError(e.message);
			throw e;
		}
	}

	async resolve(runId: string, output: unknown, error?: string): Promise<void> {
		const r = await this.client.get(runId);
		if (finished(r)) return; // an HTTP retry after an ambiguous response
		const review = r.reviews?.[0];
		if (!review) throw new KairoError(409, `run ${runId} has no step stopped for review`);
		await this.client.resolve(runId, review.act, output, error);
		await this.client.wait(runId); // terminal event is durable when this answers
	}

	async close(): Promise<void> {
		this.closing = true;
		this.startAbort.abort(new Error('stopped while confirming a run start'));
		await Promise.allSettled([...this.claims.values()].map((claim) => this.releaseWorkflow(claim, true)));
		clearInterval(this.renewTimer);
		const w = this.worker;
		this.worker = undefined;
		w?.stop();
	}

	async drain(signal?: AbortSignal): Promise<void> {
		this.draining = true;
		this.startAbort.abort(new Error('draining while confirming a run start'));
		await Promise.all([this.worker?.drain(signal), this.waitForStarts(signal)]);
	}

	get inflight(): number {
		return this.worker?.inflightTasks ?? 0;
	}
}

function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
	if (signal.aborted) return Promise.reject(signal.reason ?? new Error('aborted'));
	return new Promise((resolve, reject) => {
		const timer = setTimeout(done, ms);
		function done() {
			signal.removeEventListener('abort', stopped);
			resolve();
		}
		function stopped() {
			clearTimeout(timer);
			reject(signal.reason ?? new Error('aborted'));
		}
		signal.addEventListener('abort', stopped, { once: true });
	});
}

function retryDelay(base: number, minimum: number | undefined, runId: string, attempt: number): number {
	let hash = 2166136261;
	for (let i = 0; i < runId.length; i++) hash = Math.imul(hash ^ runId.charCodeAt(i), 16777619) >>> 0;
	hash = Math.imul(hash ^ attempt, 16777619) >>> 0;
	const jittered = Math.floor((base * (75 + (hash % 51))) / 100);
	return Math.max(jittered, minimum ?? 0);
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

	children(runId: string) {
		return this.runtime.children(runId);
	}

	list(opts: { workflow?: string; status?: string; since?: number; until?: number; after?: string; limit?: number } = {}) {
		return this.runtime.list(opts);
	}

	cancel(runId: string) {
		return this.runtime.cancel(runId);
	}

	resolve(runId: string, output: unknown, error?: string) {
		return this.runtime.resolve(runId, output, error);
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
