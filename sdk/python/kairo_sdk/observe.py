"""Logging and observing the runtime.

What goes wrong in the runtime's background work is written to a
``logging.Logger`` (``logging.getLogger("kairo_sdk")`` by default). What
happens to runs and steps is given, as it happens, to an observer: to count,
time and trace them (metrics, OpenTelemetry) without kairo depending on
either.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from .protocol import Result

#: The default logger of the runtime's background work.
LOGGER = logging.getLogger("kairo_sdk")

# Kinds of Observation.
RUN_STARTED = "run.started"
RUN_SETTLED = "run.settled"
STEP_STARTED = "step.started"
STEP_FINISHED = "step.finished"

# How a step's attempt ended (Observation.status of STEP_FINISHED).
STEP_OK = "ok"
STEP_RETRYABLE = "retryable"  # failed, may be tried again
STEP_FAILED = "failed"  # failed for good
STEP_UNKNOWN = "unknown"  # its outcome is unknown (invariant 5)
STEP_WAITING = "waiting"  # waits in kairo (a sleep)
STEP_PENDING = "pending"  # runs elsewhere; its outcome comes later (ADR 0052)


@dataclass
class Observation:
    """Something that happened in this process's runtime, given to the
    observer (``Kairo(observe=...)``) as it happens.

    status: for RUN_SETTLED, the run's status (completed, failed, cancelled,
    or blocked: stopped for review); for STEP_FINISHED, how the attempt
    ended (STEP_OK, STEP_RETRYABLE, STEP_FAILED, STEP_UNKNOWN, STEP_WAITING,
    STEP_PENDING). duration: how long the handler ran here (seconds; None
    for an outcome that came on the callback)."""

    kind: str  # RUN_STARTED, RUN_SETTLED, STEP_STARTED, STEP_FINISHED
    at: int = 0  # unix ms
    run_id: str = ""
    parent: str = ""  # the run that made it (a workflow, of its calls)
    plan: str = ""  # kairo.workflow, kairo.call/<action>, kairo.wait/<signal>
    workflow: str = ""  # a workflow run's workflow
    action: str = ""  # a call's or a step's action
    status: str = ""
    error: str = ""
    step_id: str = ""
    attempt: int = 0
    duration: float | None = None


#: Called with each Observation, in the runtime, as it happens: it must not block.
Observer = Callable[[Observation], None]

_PLAN_CALL = "kairo.call/"


def observe(observer: Observer | None, logger: logging.Logger, now: Callable[[], int], o: Observation) -> None:
    """Gives o to observer, if there is one: its action from a call's plan,
    and the time now if it has none. An observer that raises is logged, not
    let stop the runtime."""
    if observer is None:
        return
    if not o.action and o.plan.startswith(_PLAN_CALL):
        o.action = o.plan[len(_PLAN_CALL) :]
    if not o.at:
        o.at = now()
    try:
        observer(o)
    except Exception:
        logger.exception("kairo: the observer raised (%s)", o.kind)


def step_status(res: Result) -> str:
    """How a step's attempt ended."""
    if res.pending is not None:
        return STEP_PENDING
    if res.unknown:
        return STEP_UNKNOWN
    if res.error:
        return STEP_RETRYABLE if res.retryable else STEP_FAILED
    if res.wait is not None:
        return STEP_WAITING
    return STEP_OK
