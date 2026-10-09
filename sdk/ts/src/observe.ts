// Logging and observing the runtime: where what goes wrong in its
// background work is written, and what is told of runs and steps as they
// happen (to count, time and trace them without kairo depending on a
// metrics or tracing library).

import type { Result } from './protocol.ts';

/** Takes what goes wrong in the runtime's background work (lease renewals, sweeps, callbacks). */
export interface Logger {
	warn(msg: string, attrs?: Record<string, unknown>): void;
	error(msg: string, attrs?: Record<string, unknown>): void;
}

/** The default Logger: console.warn and console.error. */
export const consoleLogger: Logger = {
	warn: (msg, attrs) => (attrs ? console.warn(msg, attrs) : console.warn(msg)),
	error: (msg, attrs) => (attrs ? console.error(msg, attrs) : console.error(msg)),
};

/** Kinds of Observation. */
export const ObservationKind = {
	runStarted: 'run.started',
	runSettled: 'run.settled',
	stepStarted: 'step.started',
	stepFinished: 'step.finished',
} as const;
export type ObservationKind = (typeof ObservationKind)[keyof typeof ObservationKind];

/** How a step's attempt ended (Observation.status of step.finished). */
export const StepStatus = {
	ok: 'ok',
	/** Failed, may be tried again. */
	retryable: 'retryable',
	/** Failed for good. */
	failed: 'failed',
	/** Its outcome is unknown (invariant 5). */
	unknown: 'unknown',
	/** Waits in kairo (a sleep). */
	waiting: 'waiting',
	/** Runs elsewhere; its outcome comes later (ADR 0052). */
	pending: 'pending',
} as const;
export type StepStatus = (typeof StepStatus)[keyof typeof StepStatus];

/** Something that happened in this process's runtime, given to KairoOptions.observe as it happens. */
export interface Observation {
	kind: ObservationKind;
	/** Unix ms. */
	at: number;
	runId: string;
	/** The run that made it (a workflow, of its calls). */
	parent?: string;
	/** kairo.workflow, kairo.call/<action>, kairo.wait/<signal>. */
	plan?: string;
	/** A workflow run's workflow. */
	workflow?: string;
	/** A call's or a step's action. */
	action?: string;
	/**
	 * run.settled: the run's status (completed, failed, cancelled, or
	 * blocked: stopped for review). step.finished: how the attempt ended
	 * (StepStatus).
	 */
	status?: string;
	/** run.settled, step.finished: the error, if any. */
	error?: string;
	stepId?: string;
	attempt?: number;
	/** step.finished: how long the handler ran here (ms). */
	duration?: number;
}

/** Called with each Observation, in the runtime, as it happens: it must not block. */
export type Observer = (o: Observation) => void;

const PLAN_CALL = 'kairo.call/';

/**
 * Gives o to observer, if there is one: its action from a call's plan, and
 * the time now if it has none. An observer that throws is logged, not let
 * stop the runtime.
 */
export function observe(observer: Observer | undefined, logger: Logger, now: () => number, o: Omit<Observation, 'at'> & { at?: number }): void {
	if (!observer) return;
	if (!o.action && o.plan?.startsWith(PLAN_CALL)) o.action = o.plan.slice(PLAN_CALL.length);
	o.at ??= now();
	try {
		observer(o as Observation);
	} catch (e) {
		logger.error('kairo: the observer threw', { kind: o.kind, err: e });
	}
}

/** How a step's attempt ended. */
export function stepStatus(res: Result): StepStatus {
	if (res.pending) return StepStatus.pending;
	if (res.unknown) return StepStatus.unknown;
	if (res.error !== undefined) return res.retryable ? StepStatus.retryable : StepStatus.failed;
	if (res.wait) return StepStatus.waiting;
	return StepStatus.ok;
}
