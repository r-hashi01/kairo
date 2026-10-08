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
	[key: string]: unknown;
}

/** A run as kairod reports it (engine.RunInfo). */
export interface RunInfo {
	run_id: string;
	plan: string;
	tenant: string;
	status: string; // running | waiting | completed | failed | cancelled | blocked
	output?: unknown;
	error?: string;
	/** Only the finished-run marker is left: the output is gone (ADR 0027). */
	trimmed?: boolean;
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
}

export class KairoError extends Error {
	readonly status: number;
	constructor(status: number, message: string) {
		super(message);
		this.name = 'KairoError';
		this.status = status;
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
		const res = await fetch(this.url + path, {
			method,
			headers: body === undefined ? undefined : { 'content-type': 'application/json' },
			body: body === undefined ? undefined : JSON.stringify(body),
			signal,
		});
		const text = await res.text();
		const data = text ? JSON.parse(text) : null;
		if (res.status >= 400 && res.status !== 504) {
			throw new KairoError(res.status, (data && data.error) || `${method} ${path}: ${res.status}`);
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
		const [status, data] = await this.call('POST', '/v1/runs', body);
		if (status === 504) throw new KairoError(504, `run ${data.run_id}: start not confirmed`);
		return data;
	}

	async get(runId: string): Promise<RunInfo> {
		return (await this.call('GET', `/v1/runs/${encodeURIComponent(runId)}`))[1];
	}

	/** Waits until the run has finished (as long as signal allows). */
	async wait(runId: string, signal?: AbortSignal): Promise<RunInfo> {
		for (;;) {
			const [status, data] = await this.call('GET', `/v1/runs/${encodeURIComponent(runId)}/wait?timeout=60s`, undefined, signal);
			if (status === 200) return data;
		}
	}

	/** Delivers a signal to a run (a wait node named name receives it). */
	async signal(runId: string, name: string, payload: unknown = null): Promise<void> {
		await this.call('POST', `/v1/runs/${encodeURIComponent(runId)}/signals/${encodeURIComponent(name)}`, payload);
	}

	async cancel(runId: string): Promise<void> {
		await this.call('POST', `/v1/runs/${encodeURIComponent(runId)}/cancel`);
	}
}
