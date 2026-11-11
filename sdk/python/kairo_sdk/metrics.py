"""Non-durable, bounded-cardinality operational measurements (ADR 0076)."""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass, field


RUN_START_UNCONFIRMED = "kairo_run_start_unconfirmed_total"
RUN_START_RETRIES = "kairo_run_start_confirmation_retries_total"
RUN_START_CONFIRMATION = "kairo_run_start_confirmation_seconds"
DRAIN = "kairo_drain_total"
DRAIN_DURATION = "kairo_drain_seconds"
DRAIN_STARTING_TASKS = "kairo_drain_starting_tasks"
DRAIN_STARTING_DRIVES = "kairo_drain_starting_drives"


@dataclass(frozen=True, slots=True)
class Metric:
    name: str
    kind: str
    unit: str
    value: float
    labels: dict[str, str | int] = field(default_factory=dict)


MetricSink = Callable[[Metric], None]


class MetricEmitter:
    """A process-level bounded handoff; emit never calls or waits for sink."""

    def __init__(self, sink: MetricSink, capacity: int = 1024) -> None:
        self._sink = sink
        self._queue: queue.Queue[Metric | object] = queue.Queue(maxsize=capacity)
        self._sentinel = object()
        self._closed = False
        self.dropped = 0
        self.failed = 0
        threading.Thread(target=self._run, daemon=True, name="kairo-metrics").start()

    def emit(self, metric: Metric) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(metric)
        except queue.Full:
            self.dropped += 1

    def _run(self) -> None:
        while True:
            metric = self._queue.get()
            if metric is self._sentinel:
                self._queue.task_done()
                return
            try:
                self._sink(metric)  # type: ignore[arg-type]
            except Exception:
                self.failed += 1
            finally:
                self._queue.task_done()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._queue.put_nowait(self._sentinel)
        except queue.Full:
            self._queue.get_nowait()
            self._queue.task_done()
            self.dropped += 1
            self._queue.put_nowait(self._sentinel)
