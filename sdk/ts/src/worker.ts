// A worker: pulls tasks from the runtime and runs a handler for each.

import net from 'node:net';
import os from 'node:os';

import { FrameReader, MsgType, encodeFrame, resultBody, type Result, type Task } from './protocol.ts';

/** What a handler gets besides the task. */
export interface TaskContext {
	/** Aborted when the step is abandoned (timeout, run cancelled; ADR 0026). */
	signal: AbortSignal;
	/** Streams output of the step to watchers. */
	emit(data: string | Buffer): void;
}

export type Handler = (task: Task, ctx: TaskContext) => Promise<Result>;

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
			socket.on('connect', () => {
				const hello: Record<string, unknown> = {
					worker: this.opts.name,
					actions: this.opts.actions,
					credit: this.concurrency,
				};
				if (this.opts.token) hello.token = this.opts.token;
				if (this.opts.onRunEnd) hello.run_end = true;
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
					if (type === MsgType.Task) this.serve(body as Task, send);
					else if (type === MsgType.Cancel) this.cancels.get((body as { seq: number }).seq)?.abort();
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
				for (const c of this.cancels.values()) c.abort();
				this.cancels.clear();
				resolve();
			});
		});
	}

	/** Closes the connection. */
	stop(): void {
		this.socket?.end();
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
			send(MsgType.Result, resultBody(task.seq, result));
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
