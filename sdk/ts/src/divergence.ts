// A workflow's replay that leaves its record is stopped before it makes a
// call with an effect (ADR 0084). Calls are matched with the record by
// what they are, not where (ADR 0049): a function that is not deterministic
// makes another call on replay, and one that acts outside would act again.

import type { RunInfo } from './client.ts';
import { finished } from './client.ts';

/**
 * A workflow's replay left its record: it was about to make a call with an
 * effect while a recorded call with an effect was never reached (ADR
 * 0084). Nothing was made; the workflow is not failed, and goes on once
 * code that replays as it first ran drives it.
 */
export class DivergedError extends Error {
	override name = 'DivergedError';
	/** The workflow's run id. */
	readonly workflow: string;
	/** The call it was about to make. */
	readonly call: string;
	/** Recorded calls it did not reach first. */
	readonly unreached: string[];
	constructor(workflow: string, call: string, unreached: string[]) {
		super(`workflow ${workflow} left its record: it was about to make ${call}, but has not reached ${unreached.join(', ')} (ADR 0084)`);
		this.workflow = workflow;
		this.call = call;
		this.unreached = unreached;
	}
}

type Kind = 'effect' | 'call' | 'wait';
type Recorded = { effect: boolean; finished: boolean };

/**
 * One drive of a workflow. A new call with an effect is held while a
 * recorded call with an effect is not reached; it is judged once the
 * function can go no further without it: the calls still out are the held
 * ones, and those that cannot lead to the record (recorded and unfinished,
 * or new waits). Calls in JavaScript go on in microtasks, so that point is
 * looked for after they have run (setImmediate).
 */
export class Divergence {
	private readonly reached = new Set<string>();
	private readonly out = new Map<string, Kind>();
	private recorded: Map<string, Recorded> | undefined;
	private loading: Promise<void> | undefined;
	private held: Array<{ resolve: () => void; reject: (e: Error) => void }> = [];
	private scheduled = false;
	private err: DivergedError | undefined;
	private heldCall = '';

	private readonly workflow: string;
	private readonly children: () => Promise<RunInfo[]>;
	private readonly hasEffect: (r: RunInfo) => boolean;

	constructor(workflow: string, children: () => Promise<RunInfo[]>, hasEffect: (r: RunInfo) => boolean) {
		this.workflow = workflow;
		this.children = children;
		this.hasEffect = hasEffect;
	}

	/** Throws the drive's divergence, once found, even if the workflow caught it. */
	check(): void {
		if (this.err) throw this.err;
	}

	/** The call id is being made; one with an effect waits until it is judged. */
	async enter(id: string, kind: Kind): Promise<void> {
		this.check();
		this.reached.add(id);
		this.out.set(id, kind);
		this.wake();
		if (kind !== 'effect') return;
		if (!this.recorded) await (this.loading ??= this.load());
		this.check();
		if (this.recorded!.has(id) || this.unreached().length === 0) {
			this.recorded!.set(id, this.recorded!.get(id) ?? { effect: true, finished: false });
			return;
		}
		this.out.delete(id);
		this.heldCall ||= id;
		const judged = new Promise<void>((resolve, reject) => this.held.push({ resolve, reject }));
		this.wake();
		await judged;
		this.recorded!.set(id, { effect: true, finished: false });
		this.out.set(id, kind);
	}

	/** The call id answered (or failed). */
	leave(id: string): void {
		this.out.delete(id);
		this.wake();
	}

	private async load(): Promise<void> {
		const recorded = new Map<string, Recorded>();
		for (const r of await this.children()) recorded.set(r.run_id, { effect: this.hasEffect(r), finished: finished(r) });
		this.recorded = recorded;
	}

	private unreached(): string[] {
		const out: string[] = [];
		for (const [id, r] of this.recorded ?? []) if (r.effect && !this.reached.has(id)) out.push(id);
		return out.sort();
	}

	private wake(): void {
		if (this.held.length === 0 || this.scheduled) return;
		this.scheduled = true;
		setImmediate(() => {
			this.scheduled = false;
			this.judge();
		});
	}

	private judge(): void {
		if (this.held.length === 0 || !this.recorded) return;
		const unreached = this.unreached();
		if (unreached.length > 0) {
			for (const [id, kind] of this.out) {
				const r = this.recorded.get(id);
				// One answered from the record, or a new call that runs, may
				// still lead to the record: wait for it.
				if (r ? r.finished : kind !== 'wait') return;
			}
			this.err = new DivergedError(this.workflow, this.heldCall, unreached);
		}
		const held = this.held;
		this.held = [];
		this.heldCall = '';
		for (const h of held) {
			if (this.err) h.reject(this.err);
			else h.resolve();
		}
	}
}
