// The pure core of kairo as a WASM module (cmd/kairo-wasm, ADR 0051): plans
// compiled once, then events applied to a run's state given as bytes. No
// I/O happens here; the embedded runtime reads and writes the state.

import { readFileSync } from 'node:fs';
import { WASI } from 'node:wasi';

/** The ABI version this SDK speaks (wasmcore.ABIVersion). */
export const ABI_VERSION = 1;

/** An event of a run (wasmcore.Event). */
export interface CoreEvent {
	kind: 'start' | 'step_ok' | 'step_err' | 'timer' | 'signal' | 'intent' | 'recover' | 'resolve' | 'cancel' | 'step_wait';
	at: number;
	act?: number;
	attempt?: number;
	timer?: number;
	name?: string;
	data?: unknown;
	error?: string;
	error_type?: string;
	retryable?: boolean;
	unknown?: boolean;
	deadline?: number;
	vars?: unknown;
	meta?: unknown;
}

/** A command of a run (wasmcore.Command). */
export interface CoreCommand {
	kind: 'dispatch' | 'timer' | 'cancel_timer' | 'abort' | 'review' | 'done';
	act?: number;
	node?: string;
	action?: string;
	effect?: string;
	attempt?: number;
	timer?: number;
	at?: number;
	step_id?: string;
	idempotency_key?: string;
	input?: unknown;
}

/** What applying an event answers besides the new state (wasmcore.Result). */
export interface CoreResult {
	ignored?: boolean;
	commands: CoreCommand[];
	traces?: Array<Record<string, unknown>>;
	status: string;
	output?: unknown;
	error?: string;
	quiescent: boolean;
}

export interface Compiled {
	plan: number;
	name: string;
	hash: string;
	has_real: boolean;
	effects: Record<string, string>;
	/** The actions declared idempotent_retry (ADR 0060). */
	idempotent?: Record<string, boolean>;
}

/** What a run's state tells its host (wasmcore.Inspection, ADR 0060). */
export interface Inspection {
	/** A real attempt is out with its intent durable (it may have taken effect; its outcome is not in). */
	intent_durable: boolean;
	/** The activations stopped for review, in order. */
	review?: number[];
	/** The activations with a real attempt out and its intent durable, in order. A resolve event settles either. */
	intents?: number[];
}

export class CoreError extends Error {}

export class Core {
	private readonly x: any;
	private readonly enc = new TextEncoder();
	private readonly dec = new TextDecoder();

	private constructor(instance: WebAssembly.Instance) {
		this.x = instance.exports;
		const v = this.x.kairo_abi_version();
		if (v !== ABI_VERSION) throw new CoreError(`kairo.wasm speaks ABI ${v}, this SDK ${ABI_VERSION}`);
	}

	/** Loads the WASM module from path (or its bytes). */
	static async load(wasm: string | Uint8Array): Promise<Core> {
		const bytes = typeof wasm === 'string' ? readFileSync(wasm) : wasm;
		const wasi = new WASI({ version: 'preview1' });
		const instance = await WebAssembly.instantiate(await WebAssembly.compile(new Uint8Array(bytes)), wasi.getImportObject() as WebAssembly.Imports);
		wasi.initialize(instance);
		return new Core(instance);
	}

	private put(b: Uint8Array): [number, number] {
		if (b.length === 0) return [0, 0];
		const p = this.x.kairo_alloc(b.length);
		new Uint8Array(this.x.memory.buffer, p, b.length).set(b);
		return [p, b.length];
	}

	private result(n: number): Uint8Array {
		const b = new Uint8Array(this.x.memory.buffer, this.x.kairo_result(), Math.abs(n)).slice();
		if (n < 0) throw new CoreError(this.dec.decode(b));
		return b;
	}

	private call(f: string, ...bufs: Uint8Array[]): Uint8Array {
		const args: number[] = [];
		const ptrs: number[] = [];
		for (const b of bufs) {
			const [p, n] = this.put(b);
			args.push(p, n);
			if (p) ptrs.push(p);
		}
		try {
			return this.result(this.x[f](...args));
		} finally {
			for (const p of ptrs) this.x.kairo_free(p);
		}
	}

	/** Registers node specs (ir.NodeSpec). */
	register(specs: unknown[]): void {
		this.call('kairo_register', this.enc.encode(JSON.stringify(specs)));
	}

	/** Compiles a workflow definition (ir.Definition). */
	compile(definition: unknown): Compiled {
		return JSON.parse(this.dec.decode(this.call('kairo_compile', this.enc.encode(JSON.stringify(definition)))));
	}

	/** Reads a run's state (ADR 0060). */
	inspect(state: Uint8Array): Inspection {
		return JSON.parse(this.dec.decode(this.call('kairo_inspect', state)));
	}

	/** Applies event to a run's state (empty for a new run). */
	apply(plan: number, runId: string, state: Uint8Array, event: CoreEvent, traced = false): [Uint8Array, CoreResult] {
		const r = this.enc.encode(runId);
		const e = this.enc.encode(JSON.stringify(event));
		const [rp, rn] = this.put(r);
		const [sp, sn] = this.put(state);
		const [ep, en] = this.put(e);
		try {
			const b = this.result(this.x.kairo_apply(plan, rp, rn, sp, sn, ep, en, traced ? 1 : 0));
			const n = new DataView(b.buffer).getUint32(0, true);
			return [b.slice(4, 4 + n), JSON.parse(this.dec.decode(b.subarray(4 + n)))];
		} finally {
			for (const p of [rp, sp, ep]) if (p) this.x.kairo_free(p);
		}
	}
}
