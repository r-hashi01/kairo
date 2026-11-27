// A run's progress (ADR 0088): its tree, and each run's recorded events,
// in public types. Only what was recorded, as recorded: nothing inferred
// (an attempt's start is not recorded unless it was a real step's intent),
// no clocks adjusted. Reading changes nothing.

import type { RunInfo } from './client.ts';
import type { CoreEvent } from './core.ts';

/**
 * What a recorded event says happened:
 *
 * - started: the run started.
 * - dispatched: a real step's attempt passed its limits and went to run (its intent).
 * - succeeded / failed: an attempt's outcome.
 * - waiting: an attempt reported it waits until a deadline.
 * - timer: a timer fired (a retry's backoff, a wait's timeout, a sleep).
 * - signal: a signal was received.
 * - recovered: a step whose process stopped before it was dispatched was given out again.
 * - resolved: a step stopped for review was settled.
 * - cancelled: the run was cancelled.
 * - other: an event of a kind this SDK does not name.
 */
export type RunEventKind =
	| 'started'
	| 'dispatched'
	| 'succeeded'
	| 'failed'
	| 'waiting'
	| 'timer'
	| 'signal'
	| 'recovered'
	| 'resolved'
	| 'cancelled'
	| 'other';

/** A recorded event of a run (ADR 0088). */
export interface RunEvent {
	kind: RunEventKind;
	/** When it was applied (unix ms), by the clock of the process that applied it. */
	at: number;
	/** The attempt (dispatched, succeeded, failed, waiting). */
	attempt?: number;
	/** A failure's message (failed, resolved as failed, cancelled). */
	error?: string;
	/** A failure's type as recorded; "process_lost": the process running the attempt stopped. */
	errorType?: string;
	/** The failure may be tried again. */
	retryable?: boolean;
	/** The attempt's outcome is not known: it may have taken effect. */
	unknown?: boolean;
	/** The retry was asked to wait this long (ms). */
	retryAfterMs?: number;
	/** A signal's name. */
	signal?: string;
	/** Until when an attempt waits (unix ms; waiting). */
	deadline?: number;
}

/** What a run of a code workflow's tree is. */
export type ProgressRunKind = 'workflow' | 'call' | 'wait' | 'run';

/** A run of the tree, as children gives it, with its events (ADR 0088). */
export interface ProgressRun extends RunInfo {
	kind: ProgressRunKind;
	/** The workflow's, action's or wait's name; a plain run's plan. */
	name: string;
	events: RunEvent[];
}

/** A run's tree and its recorded events (Kairo.progress, ADR 0088). */
export interface RunProgress {
	root: string;
	/** The root first, then the runs under it in creation order. */
	runs: ProgressRun[];
}

const KINDS: Record<string, RunEventKind> = {
	start: 'started',
	intent: 'dispatched',
	step_ok: 'succeeded',
	step_err: 'failed',
	step_wait: 'waiting',
	timer: 'timer',
	signal: 'signal',
	recover: 'recovered',
	resolve: 'resolved',
	cancel: 'cancelled',
};

/** The public form of a recorded event. */
export function runEvent(e: CoreEvent): RunEvent {
	const kind = KINDS[e.kind] ?? 'other';
	const out: RunEvent = { kind, at: e.at };
	if (e.attempt !== undefined && ['dispatched', 'succeeded', 'failed', 'waiting'].includes(kind)) out.attempt = e.attempt;
	if (e.error) out.error = e.error;
	if (e.error_type) out.errorType = e.error_type;
	if (e.retryable) out.retryable = true;
	if (e.unknown) out.unknown = true;
	if (e.retry_after_ms) out.retryAfterMs = e.retry_after_ms;
	if (kind === 'signal' && e.name) out.signal = e.name;
	if (kind === 'waiting' && e.deadline) out.deadline = e.deadline;
	return out;
}

const PLAN_CALL = 'kairo.call/';
const PLAN_WAIT = 'kairo.wait/';
const PLAN_WORKFLOW = 'kairo.workflow';

/** What run r is in a code workflow's tree, and its name. */
export function progressKind(r: RunInfo): { kind: ProgressRunKind; name: string } {
	if (r.plan === PLAN_WORKFLOW) return { kind: 'workflow', name: r.workflow ?? '' };
	if (r.plan.startsWith(PLAN_CALL)) return { kind: 'call', name: r.plan.slice(PLAN_CALL.length) };
	// A wait with a timeout: kairo.wait/<signal>@<ms>.
	if (r.plan.startsWith(PLAN_WAIT)) return { kind: 'wait', name: r.plan.slice(PLAN_WAIT.length).replace(/@(\d+)$/, (m, ms) => (Number(ms) > 0 ? '' : m)) };
	return { kind: 'run', name: r.plan };
}
