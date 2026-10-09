// A kairo for an application's tests (the Go SDK's kairotest.Env): in
// memory, in suspend mode, on a clock the test moves.

import { EmbeddedBackend } from './backend.ts';
import type { RunInfo } from './client.ts';
import { ObservationKind, type Observation } from './observe.ts';
import { SQLiteStore } from './store.ts';
import { Kairo, type KairoOptions } from './workflow.ts';

export interface TestEnvOptions {
	/** Where the clock starts (unix ms or a Date; default 2026-01-01T00:00:00Z). */
	at?: number | Date;
	/** kairo.wasm: a path or its bytes (default: the one in this package). */
	wasm?: string | Uint8Array;
	/**
	 * The Kairo's options (concurrency, observe, logger, ...). backend and
	 * mode are the env's: an embedded runtime on SQLite in memory, on the
	 * env's clock, its timers fired by advance only, in suspend mode.
	 */
	kairo?: Omit<KairoOptions, 'backend' | 'mode'>;
}

/** One call a workflow made: an action's, a wait for a signal, or a child workflow. */
export interface Call {
	runId: string;
	/** "call", "wait" or "workflow". */
	kind: 'call' | 'wait' | 'workflow';
	/** The action, the signal or the workflow. */
	name: string;
	/** What it was given. */
	input: unknown;
	/** An action's output, a wait's payload (null if it timed out), a workflow's result. */
	output: unknown;
	/** completed, failed, cancelled, blocked, running. */
	status: string;
	error?: string;
}

/**
 * A kairo for an application's tests. A workflow that waits (a sleep, a
 * signal, a timeout) throws Suspended from run; advance moves the clock and
 * fires what is due, signal sends what it waits for, and result gives its
 * result. Nothing waits in real time.
 *
 *	const env = await createTestEnv();
 *	env.k.defineAction('charge', { effect: 'real', handler: stubCharge });
 *	env.k.workflow('refund', refund);
 *	await env.start();
 *	await assert.rejects(env.k.run('refund', req, { id: 'r-1' }), Suspended); // sleeps a day
 *	await env.advance(24 * 3600_000);
 *	const out = await env.k.result('r-1');
 */
export interface TestEnv {
	k: Kairo;
	/** Starts k (Kairo.start). */
	start(): Promise<void>;
	/** The env's clock (unix ms). */
	now(): number;
	/**
	 * Moves the clock by ms, firing what comes due on the way at the time it
	 * is due (a sleep that ends, then a timeout that starts from there), and
	 * driving the workflows on.
	 */
	advance(ms: number): Promise<void>;
	/** Sends a signal (Kairo.signal). */
	signal(id: string, name: string, payload?: unknown): Promise<void>;
	/** The calls workflow id made, in the order it made them. */
	calls(id: string): Promise<Call[]>;
	close(): Promise<void>;
}

/** Opens a TestEnv. Declare actions and workflows on env.k, then env.start(); close it when the test ends. */
export async function createTestEnv(opts: TestEnvOptions = {}): Promise<TestEnv> {
	const at = opts.at ?? Date.UTC(2026, 0, 1);
	let clock = at instanceof Date ? at.getTime() : at;
	const now = () => clock;
	// Run id: the order it started in, and the run that made it. A fake
	// clock gives runs made at once the same creation time: the order is
	// taken from the observations instead.
	const started = new Map<string, { n: number; parent?: string }>();
	const observe = opts.kairo?.observe;
	const backend = await EmbeddedBackend.open({ store: new SQLiteStore(':memory:'), wasm: opts.wasm, now, manualTimers: true });
	const k = new Kairo({
		...opts.kairo,
		backend,
		mode: 'suspend',
		observe: (o: Observation) => {
			if (o.kind === ObservationKind.runStarted && o.runId && !started.has(o.runId)) started.set(o.runId, { n: started.size, parent: o.parent });
			observe?.(o);
		},
	});
	return {
		k,
		start: () => k.start(),
		now,
		async advance(ms: number) {
			const target = clock + ms;
			for (let i = 0; i < 10_000; i++) {
				const next = await k.tick();
				if (next === null || next > target) {
					clock = target;
					return;
				}
				if (next > clock) clock = next; // to the next thing due
			}
			throw new Error('kairo test env: still due after 10000 ticks: a timer that fires again at once?');
		},
		signal: (id, name, payload = null) => k.signal(id, name, payload),
		async calls(id: string) {
			const kids = [...started].filter(([, s]) => s.parent === id).sort(([, a], [, b]) => a.n - b.n);
			const out: Call[] = [];
			for (const [runId] of kids) out.push(toCall(await k.backend.get(runId)));
			return out;
		},
		close: () => k.close(),
	};
}

function toCall(r: RunInfo): Call {
	const c = { runId: r.run_id, status: r.status, ...(r.error ? { error: r.error } : {}) };
	const input = r.input as { in?: unknown; input?: unknown } | null | undefined;
	if (r.plan.startsWith('kairo.call/')) return { ...c, kind: 'call', name: r.plan.slice('kairo.call/'.length), input: input?.in, output: r.output };
	if (r.plan.startsWith('kairo.wait/')) {
		// Without its timeout (ADR 0059).
		const name = r.plan.slice('kairo.wait/'.length).replace(/@\d+$/, '');
		return { ...c, kind: 'wait', name, input: input?.in, output: (r.output as { payload?: unknown } | undefined)?.payload };
	}
	const out = (r.output as { payload?: { value?: unknown } } | undefined)?.payload;
	return { ...c, kind: 'workflow', name: r.workflow ?? '', input: input?.input, output: out?.value };
}
