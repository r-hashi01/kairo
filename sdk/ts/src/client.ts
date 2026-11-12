// The graph API (ADR 0049): a thin client of kairod's HTTP API (/v1/...).

/** How a node acts on the world (ir.Effect); undeclared is real. */
export type EffectName = 'real' | 'unprotected';

/** Durability of a run (engine.Tier). */
export type TierName = 'none' | 'memory' | 'file';

/** A node spec (ir.NodeSpec), as kairod's /v1/nodes takes it. */
export interface NodeSpec {
	action: string;
	effect?: EffectName;
	idempotent_retry?: boolean;
	max_attempts?: number;
	timeout?: string;
	destination?: string;
	destination_from?: string;
	[key: string]: unknown;
}

/** A run as kairod reports it (engine.RunInfo). */
export interface Review {
	act: number;
	step_id: string;
}

export interface RunInfo {
	run_id: string;
	plan: string;
	tenant: string;
	status: string; // running | waiting | completed | failed | cancelled | blocked
	output?: unknown;
	error?: string;
	reviews?: Review[];
	/** Only the finished-run marker is left: the output is gone (ADR 0027). */
	trimmed?: boolean;
	/** The embedded runtime (ADR 0059): the run that made this one, a workflow run's workflow and meta, when it started and last changed (unix ms). */
	parent?: string;
	workflow?: string;
	/** A workflow run's version (ADR 0060). */
	version?: string;
	meta?: Record<string, unknown>;
	createdAt?: number;
	updatedAt?: number;
	created_at?: number;
	updated_at?: number;
	[key: string]: unknown;
}

export interface RunOptions {
	/** The run id: an idempotency key (ADR 0023). */
	runId?: string;
	tenant?: string;
	tier?: TierName;
	/** Initial values of the plan's run variables. */
	vars?: Record<string, unknown>;
	/** Keep the output after the run finishes, across restarts, for the idempotency period (ADR 0050). */
	keepOutput?: boolean;
	parent?: string;
	workflow?: string;
	meta?: Record<string, unknown>;
	drive?: boolean;
	initialSignal?: { name: string; payload: unknown; signal_id: string; only_if_waiting?: boolean };
	/** Cancels the HTTP request while its durable start is still unconfirmed. */
	signal?: AbortSignal;
}

export class KairoError extends Error {
	readonly status: number;
	readonly retryAfterMs?: number;
	constructor(status: number, message: string, retryAfterMs?: number) {
		super(message);
		this.name = 'KairoError';
		this.status = status;
		this.retryAfterMs = retryAfterMs;
	}
}

export class KairoTransportError extends Error {
	constructor(message: string, opts?: ErrorOptions) {
		super(message, opts);
		this.name = 'KairoTransportError';
	}
}

const DONE = new Set(['completed', 'failed', 'cancelled']);

/** Whether a run has finished. */
export function finished(r: RunInfo): boolean {
	return DONE.has(r.status);
}

export class Client {
	readonly url: string;

	constructor(url = 'http://127.0.0.1:8420') {
		this.url = url.replace(/\/$/, '');
	}

	private async call(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<[number, any]> {
		let res: Response;
		let text: string;
		try {
			res = await fetch(this.url + path, {
				method,
				headers: body === undefined ? undefined : { 'content-type': 'application/json' },
				body: body === undefined ? undefined : JSON.stringify(body),
				signal,
			});
			text = await res.text();
		} catch (e) {
			if (signal?.aborted) throw e;
			throw new KairoTransportError(`${method} ${path}: response not received`, { cause: e });
		}
		const data = text ? JSON.parse(text) : null;
		if (res.status >= 400 && res.status !== 504) {
			throw new KairoError(
				res.status,
				(data && data.error) || `${method} ${path}: ${res.status}`,
				parseRetryAfter(res.headers.get('retry-after')),
			);
		}
		return [res.status, data];
	}

	/** Registers node specs (kairod keeps them in its data directory). */
	async registerNodes(specs: NodeSpec[]): Promise<void> {
		await this.call('POST', '/v1/nodes', specs);
	}

	/** Registers a plan (a workflow definition, ir.Definition). */
	async registerPlan(definition: unknown): Promise<{ name: string; hash: string }> {
		return (await this.call('POST', '/v1/plans', definition))[1];
	}

	/**
	 * Starts a run of plan and answers once its start is durable. With a run
	 * id already running or recently finished, nothing new starts
	 * (`existing`).
	 */
	async run(plan: string, input: unknown, opts: RunOptions = {}): Promise<{ run_id: string; existing: boolean }> {
		const body: Record<string, unknown> = { plan, input, tenant: opts.tenant ?? 'default' };
		if (opts.runId) body.run_id = opts.runId;
		if (opts.tier) body.tier = opts.tier;
		if (opts.vars) body.vars = opts.vars;
		if (opts.keepOutput) body.keep_output = true;
		if (opts.parent) body.parent = opts.parent;
		if (opts.workflow) body.workflow = opts.workflow;
		if (opts.meta) body.meta = opts.meta;
		if (opts.drive) body.drive = true;
		if (opts.initialSignal) body.initial_signal = opts.initialSignal;
		const [status, data] = await this.call('POST', '/v1/runs', body, opts.signal);
		if (status === 504) throw new KairoError(504, `run ${data.run_id}: start not confirmed`);
		return data;
	}

	/** With input: the run's input too, as it was submitted (a run not finished, ADR 0060). */
	async get(runId: string, opts: { input?: boolean } = {}): Promise<RunInfo> {
		return (await this.call('GET', `/v1/runs/${encodeURIComponent(runId)}${opts.input ? '?input=true' : ''}`))[1];
	}

	/** Waits until the run has finished (as long as signal allows). */
	async wait(runId: string, signal?: AbortSignal): Promise<RunInfo> {
		for (;;) {
			const [status, data] = await this.call('GET', `/v1/runs/${encodeURIComponent(runId)}/wait?timeout=60s`, undefined, signal);
			if (status === 200) return data;
		}
	}

	/** Waits until a run is terminal or has no executor work in flight. */
	async waitQuiescent(runId: string, signal?: AbortSignal): Promise<RunInfo> {
		for (;;) {
			const [status, data] = await this.call('GET', `/v1/runs/${encodeURIComponent(runId)}/quiescent?timeout=60s`, undefined, signal);
			if (status === 200) return data;
		}
	}

	async claimWorkflow(opts: {
		owner: string;
		leaseMs: number;
		workflows: Record<string, string[]>;
		runId?: string;
		timeoutMs?: number;
	}): Promise<{ claim?: { run: RunInfo; token: string; until: number }; next_at?: number }> {
		return (
			await this.call('POST', '/v1/workflow-drives/claim', {
				owner: opts.owner,
				lease_ms: opts.leaseMs,
				workflows: opts.workflows,
				limit: 1,
				timeout_ms: opts.timeoutMs ?? 0,
				...(opts.runId ? { run_id: opts.runId } : {}),
			})
		)[1];
	}

	async renewWorkflow(runId: string, owner: string, token: string, leaseMs: number): Promise<void> {
		await this.call('POST', `/v1/workflow-drives/${encodeURIComponent(runId)}/renew`, { owner, token, lease_ms: leaseMs });
	}

	async releaseWorkflow(runId: string, owner: string, token: string, retry = false): Promise<void> {
		await this.call('POST', `/v1/workflow-drives/${encodeURIComponent(runId)}/release`, { owner, token, retry });
	}

	/** Delivers a signal to a run (a wait node named name receives it). */
	async signal(runId: string, name: string, payload: unknown = null): Promise<void> {
		await this.call('POST', `/v1/runs/${encodeURIComponent(runId)}/signals/${encodeURIComponent(name)}`, payload);
	}

	async applySignal(
		runId: string,
		name: string,
		payload: unknown,
		signalId: string,
		onlyIfWaiting: boolean,
	): Promise<{ applied: boolean; duplicate?: boolean; early?: boolean }> {
		return (
			await this.call('POST', `/v1/runs/${encodeURIComponent(runId)}/signals/${encodeURIComponent(name)}/apply`, {
				payload: payload ?? null,
				signal_id: signalId,
				only_if_waiting: onlyIfWaiting,
			})
		)[1];
	}

	async cancel(runId: string): Promise<void> {
		await this.call('POST', `/v1/runs/${encodeURIComponent(runId)}/cancel`);
	}

	async children(runId: string): Promise<RunInfo[]> {
		return (await this.call('GET', `/v1/runs/${encodeURIComponent(runId)}/children`))[1];
	}

	async list(opts: { workflow?: string; status?: string; since?: number; until?: number; after?: string; limit?: number } = {}): Promise<RunInfo[]> {
		const q = new URLSearchParams();
		for (const [name, value] of Object.entries(opts)) if (value !== undefined && value !== '') q.set(name, String(value));
		return (await this.call('GET', `/v1/runs${q.size ? `?${q}` : ''}`))[1];
	}

	async finish(runId: string, payload: unknown): Promise<void> {
		await this.call('POST', `/v1/runs/${encodeURIComponent(runId)}/finish`, payload);
	}

	/** Settles one step stopped for review; accepted asynchronously by kairod. */
	async resolve(runId: string, act: number, output: unknown, error?: string): Promise<void> {
		await this.call('POST', `/v1/runs/${encodeURIComponent(runId)}/resolve`, {
			act,
			output: output ?? null,
			...(error ? { error } : {}),
		});
	}
}

function parseRetryAfter(value: string | null): number | undefined {
	if (value === null) return undefined;
	const seconds = Number(value);
	if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
	const at = Date.parse(value);
	if (!Number.isFinite(at)) return undefined;
	return Math.max(0, at - Date.now());
}
