/** Non-durable, bounded-cardinality operational measurements (ADR 0076). */

export const MetricName = {
	runStartUnconfirmed: 'kairo_run_start_unconfirmed_total',
	runStartRetries: 'kairo_run_start_confirmation_retries_total',
	runStartConfirmation: 'kairo_run_start_confirmation_seconds',
	drain: 'kairo_drain_total',
	drainDuration: 'kairo_drain_seconds',
	drainStartingTasks: 'kairo_drain_starting_tasks',
	drainStartingDrives: 'kairo_drain_starting_drives',
} as const;

export interface Metric {
	name: (typeof MetricName)[keyof typeof MetricName] | string;
	kind: 'counter' | 'gauge' | 'histogram';
	unit: string;
	value: number;
	labels?: Readonly<Record<string, string | number>>;
}

export type MetricSink = (metric: Metric) => void;

/** A bounded handoff. emit returns before calling the sink. */
export class MetricEmitter {
	private readonly queue: Metric[] = [];
	private readonly sink: MetricSink;
	private readonly capacity: number;
	private scheduled = false;
	dropped = 0;
	failed = 0;

	constructor(sink: MetricSink, capacity = 1024) {
		this.sink = sink;
		this.capacity = capacity;
	}

	emit(metric: Metric): void {
		if (this.queue.length >= this.capacity) {
			this.dropped++;
			return;
		}
		this.queue.push(metric);
		if (!this.scheduled) {
			this.scheduled = true;
			queueMicrotask(() => this.flush());
		}
	}

	private flush(): void {
		this.scheduled = false;
		for (const metric of this.queue.splice(0)) {
			try {
				this.sink(metric);
			} catch {
				this.failed++;
			}
		}
	}
}
