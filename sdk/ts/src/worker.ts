// A worker: pulls tasks from the runtime and runs a handler for each.

import net from 'node:net';
import os from 'node:os';

import {
	CAPABILITY_ASYNC_ACTION_CALLBACK,
	CAPABILITY_GRACEFUL_DRAIN,
	FrameReader,
	MsgType,
	encodeFrame,
	resultBody,
	type Result,
	type Task,
} from './protocol.ts';

/** What a handler gets besides the task. */
export interface TaskContext {
	/** Aborted when the step is abandoned (timeout, run cancelled; ADR 0026). */
	signal: AbortSignal;
	/** Streams output of the step to watchers. */
	emit(data: string | Buffer): void;
}

export type Handler = (task: Task, ctx: TaskContext) => Promise<Result>;

export class DrainUnsupportedError extends Error {
	constructor() {
		super('kairod does not support graceful worker drain');
		this.name = 'DrainUnsupportedError';
	}
}

const CAPABILITY_NEGOTIATION_TIMEOUT = 1000;

export interface WorkerOptions {
	name: string;
	actions: string[];
	handler: Handler;
	/** Tasks at once (default: from the machine, ADR 0039). */
	concurrency?: number;
	/** The runtime's worker token, if it has one. */
	token?: string;
	/** What a task mostly uses: "io" (the default) or "cpu". */
	resource?: 'io' | 'cpu';
	/** URL-backed actions that can return 202 through a daemon callback. */
	callbackActions?: string[];
	/**
	 * Called when a run this worker was sent a task of has finished, after
	 * its tasks (ADR 0044): to let go of what the worker keeps per run. Not
	 * called for runs whose tasks came before a reconnection.
	 */
	onRunEnd?: (runId: string) => void;
}

/** What one IO task is assumed to hold in memory. */
const IO_TASK_BYTES = 2 << 20;

/**
 * How many tasks a worker takes at once when not told (ADR 0039): CPU tasks
 * one per CPU; IO tasks as many as a quarter of the memory allows.
 */
export function defaultConcurrency(resource: 'io' | 'cpu' = 'io'): number {
	const cpus = os.availableParallelism();
	if (resource === 'cpu') return cpus;
	const byMemory = Math.max(Math.floor(os.totalmem() / 4 / IO_TASK_BYTES), 1);
	return Math.max(Math.min(byMemory, 4096), cpus);
}

/** An address: a UNIX socket path, or {host, port}. */
export type Address = string | { host: string; port: number };

export class Worker {
	readonly concurrency: number;
	private readonly cancels = new Map<number, AbortController>();
	private socket?: net.Socket;
	private sendFrame?: (type: number, body: unknown) => void;
	private draining = false;
	private drainAck = false;
	private negotiationDone = false;
	private drainCap = false;
	private inflight = 0;
	private pendingAcks = 0;
	private drainFailure?: Error;
	private readonly drainWaiters = new Set<{ resolve: () => void; reject: (e: Error) => void }>();
	private readonly negotiationWaiters = new Set<() => void>();

	private readonly opts: WorkerOptions;

	constructor(opts: WorkerOptions) {
		this.opts = opts;
		this.concurrency = Math.max(opts.concurrency || defaultConcurrency(opts.resource), 1);
	}

	/** Connects, says hello and serves until the connection closes. */
	run(addr: Address): Promise<void> {
		return new Promise((resolve, reject) => {
			const socket = typeof addr === 'string' ? net.connect(addr) : net.connect(addr.port, addr.host);
			this.socket = socket;
			const reader = new FrameReader();
			const send = (type: number, body: unknown) => {
				if (!socket.destroyed) socket.write(encodeFrame(type, body));
			};
			this.sendFrame = send;
			this.drainFailure = undefined;
			this.negotiationDone = false;
			this.drainCap = false;
			socket.on('connect', () => {
				const hello: Record<string, unknown> = {
					worker: this.opts.name,
					actions: this.opts.actions,
					credit: this.concurrency,
				};
				if (this.opts.token) hello.token = this.opts.token;
				if (this.opts.onRunEnd) hello.run_end = true;
				const capabilities = [CAPABILITY_GRACEFUL_DRAIN];
				if (this.opts.callbackActions?.length) {
					capabilities.push(CAPABILITY_ASYNC_ACTION_CALLBACK);
					hello.callback_actions = this.opts.callbackActions;
				}
				hello.capabilities = capabilities;
				send(MsgType.Hello, hello);
			});
			socket.on('data', (data: Buffer) => {
				let frames: Array<[number, unknown]>;
				try {
					frames = reader.push(data);
				} catch (e) {
					socket.destroy(e as Error);
					return;
				}
				for (const [type, body] of frames) {
					if (type !== MsgType.Hello) this.finishNegotiation(false);
					if (type === MsgType.Hello) {
						const capabilities = Array.isArray((body as { capabilities?: unknown }).capabilities)
							? (body as { capabilities: unknown[] }).capabilities
							: [];
						this.finishNegotiation(capabilities.includes(CAPABILITY_GRACEFUL_DRAIN));
					}
					else if (type === MsgType.Task) {
						this.inflight++;
						this.serve(body as Task, send);
					}
					else if (type === MsgType.Cancel) this.cancels.get((body as { seq: number }).seq)?.abort();
					else if (type === MsgType.Credit) {
						const n = Number((body as { n?: number }).n ?? 0);
						if (this.draining && n === 0) this.drainAck = true;
						else if (this.draining && n > 0) this.pendingAcks = Math.max(this.pendingAcks - n, 0);
						this.checkDrain();
					}
					else if (type === MsgType.RunEnd) {
						try {
							this.opts.onRunEnd?.((body as { run_id: string }).run_id);
						} catch {
							// The worker's own bookkeeping: the connection goes on.
						}
					}
					// Unknown types are skipped.
				}
			});
			socket.on('error', reject);
			socket.on('close', () => {
				this.finishNegotiation(false);
				for (const c of this.cancels.values()) c.abort();
				this.cancels.clear();
				this.sendFrame = undefined;
				if (this.draining && !(this.drainAck && this.inflight === 0 && this.pendingAcks === 0)) {
					this.drainFailure = new Error('worker connection closed while draining');
					this.checkDrain();
				}
				resolve();
			});
		});
	}

	/** Closes the connection. */
	stop(): void {
		this.socket?.end();
	}

	get inflightTasks(): number {
		return this.inflight;
	}

	/**
	 * Stops new delivery and waits for all accepted results (ADR 0073).
	 * Throws DrainUnsupportedError when kairod did not negotiate it (ADR 0075).
	 */
	async drain(signal?: AbortSignal): Promise<void> {
		if (!this.sendFrame) return Promise.reject(new Error('worker is not connected'));
		if (!this.negotiationDone) await this.waitForNegotiation(signal);
		if (!this.drainCap) return Promise.reject(new DrainUnsupportedError());
		if (!this.draining) {
			this.draining = true;
			this.sendFrame(MsgType.Credit, { n: -1 });
		}
		if (this.drainAck && this.inflight === 0 && this.pendingAcks === 0) return Promise.resolve();
		return new Promise((resolve, reject) => {
			const waiter = { resolve, reject };
			const stop = () => {
				this.drainWaiters.delete(waiter);
				reject(signal?.reason instanceof Error ? signal.reason : new Error('worker drain stopped'));
			};
			if (signal?.aborted) return stop();
			signal?.addEventListener('abort', stop, { once: true });
			this.drainWaiters.add(waiter);
			const done = waiter.resolve;
			waiter.resolve = () => {
				signal?.removeEventListener('abort', stop);
				done();
			};
			const failed = waiter.reject;
			waiter.reject = (e) => {
				signal?.removeEventListener('abort', stop);
				failed(e);
			};
			this.checkDrain();
		});
	}

	private finishNegotiation(capable: boolean): void {
		if (this.negotiationDone) return;
		this.negotiationDone = true;
		this.drainCap = capable;
		for (const wake of this.negotiationWaiters) wake();
		this.negotiationWaiters.clear();
	}

	private waitForNegotiation(signal?: AbortSignal): Promise<void> {
		return new Promise((resolve, reject) => {
			let timer: ReturnType<typeof setTimeout>;
			const done = () => {
				clearTimeout(timer);
				signal?.removeEventListener('abort', stopped);
				this.negotiationWaiters.delete(done);
				resolve();
			};
			const stopped = () => {
				clearTimeout(timer);
				this.negotiationWaiters.delete(done);
				reject(signal?.reason instanceof Error ? signal.reason : new Error('worker drain stopped'));
			};
			if (signal?.aborted) return stopped();
			this.negotiationWaiters.add(done);
			signal?.addEventListener('abort', stopped, { once: true });
			timer = setTimeout(done, CAPABILITY_NEGOTIATION_TIMEOUT);
			if (this.negotiationDone) done();
		});
	}

	private checkDrain(): void {
		if (this.drainFailure) {
			for (const waiter of this.drainWaiters) waiter.reject(this.drainFailure);
			this.drainWaiters.clear();
			return;
		}
		if (!this.drainAck || this.inflight !== 0 || this.pendingAcks !== 0) return;
		for (const waiter of this.drainWaiters) waiter.resolve();
		this.drainWaiters.clear();
	}

	private serve(task: Task, send: (type: number, body: unknown) => void): void {
		const controller = new AbortController();
		this.cancels.set(task.seq, controller);
		const ctx: TaskContext = {
			signal: controller.signal,
			emit: (data) =>
				send(MsgType.Chunk, {
					seq: task.seq,
					data: (typeof data === 'string' ? Buffer.from(data) : data).toString('base64'),
				}),
		};
		void (async () => {
			let result: Result;
			try {
				result = await this.opts.handler(task, ctx);
			} catch (e) {
				// A handler that throws: the step failed, definitely.
				result = { error: e instanceof Error ? e.message : String(e) };
			}
			this.cancels.delete(task.seq);
			if (this.draining) this.pendingAcks++;
			send(MsgType.Result, resultBody(task.seq, result));
			this.inflight--;
			this.checkDrain();
		})();
	}
}

/** The plan node of a task's step: its step id without the iteration path. */
export function nodeOf(stepId: string): string {
	const i = stepId.indexOf('[');
	return i < 0 ? stepId : stepId.slice(0, i);
}

/** The loop round of a task's step ("body[2]": 2; 0 outside loops). */
export function iterationOf(stepId: string): number {
	const m = /\[(\d+)\]$/.exec(stepId);
	return m ? Number(m[1]) : 0;
}
