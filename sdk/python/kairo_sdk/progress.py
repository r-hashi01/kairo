"""A run's progress (ADR 0088): its tree, and each run's recorded events, in
public types. Only what was recorded, as recorded: nothing inferred (an
attempt's start is not recorded unless it was a real step's intent), no
clocks adjusted. Reading changes nothing.

The same as the TypeScript SDK's (sdk/ts/src/progress.ts)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

#: What a recorded event says happened:
#:
#: - started: the run started.
#: - dispatched: a real step's attempt passed its limits and went to run (its intent).
#: - succeeded / failed: an attempt's outcome.
#: - waiting: an attempt reported it waits until a deadline.
#: - timer: a timer fired (a retry's backoff, a wait's timeout, a sleep).
#: - signal: a signal was received.
#: - recovered: a step whose process stopped before it was dispatched was given out again.
#: - resolved: a step stopped for review was settled.
#: - cancelled: the run was cancelled.
#: - other: an event of a kind this SDK does not name.
RunEventKind = Literal[
    "started", "dispatched", "succeeded", "failed", "waiting", "timer", "signal", "recovered", "resolved", "cancelled", "other"
]

#: What a run of a code workflow's tree is.
ProgressRunKind = Literal["workflow", "call", "wait", "run"]


@dataclass(frozen=True)
class RunEvent:
    """A recorded event of a run (ADR 0088)."""

    kind: RunEventKind
    at: int  # when it was applied (unix ms), by the clock of the process that applied it
    attempt: int | None = None  # the attempt (dispatched, succeeded, failed, waiting)
    error: str | None = None  # a failure's message (failed, resolved as failed, cancelled)
    error_type: str | None = None  # a failure's type as recorded; "process_lost": the process running the attempt stopped
    retryable: bool = False  # the failure may be tried again
    unknown: bool = False  # the attempt's outcome is not known: it may have taken effect
    retry_after_ms: int | None = None  # the retry was asked to wait this long
    signal: str | None = None  # a signal's name (signal)
    deadline: int | None = None  # until when an attempt waits (unix ms; waiting)


@dataclass(frozen=True)
class RunProgress:
    """A run's tree and its recorded events (Kairo.progress, ADR 0088).

    runs: the root first, then the runs under it in creation order. Each is
    a run as children gives it, with its "kind" (ProgressRunKind), its
    "name" (the workflow's, action's or wait's name; a plain run's plan) and
    its "events" (RunEvent, in the order recorded)."""

    root: str
    runs: list[dict[str, Any]]


_KINDS: dict[str, RunEventKind] = {
    "start": "started",
    "intent": "dispatched",
    "step_ok": "succeeded",
    "step_err": "failed",
    "step_wait": "waiting",
    "timer": "timer",
    "signal": "signal",
    "recover": "recovered",
    "resolve": "resolved",
    "cancel": "cancelled",
}

_ATTEMPTED = ("dispatched", "succeeded", "failed", "waiting")


def run_event(e: dict[str, Any]) -> RunEvent:
    """The public form of a recorded event (the core's)."""
    kind = _KINDS.get(e.get("kind", ""), "other")
    attempt = e.get("attempt")
    return RunEvent(
        kind=kind,
        at=int(e.get("at") or 0),
        attempt=int(attempt) if attempt is not None and kind in _ATTEMPTED else None,
        error=e.get("error") or None,
        error_type=e.get("error_type") or None,
        retryable=bool(e.get("retryable")),
        unknown=bool(e.get("unknown")),
        retry_after_ms=int(e["retry_after_ms"]) if e.get("retry_after_ms") else None,
        signal=(e.get("name") or None) if kind == "signal" else None,
        deadline=int(e["deadline"]) if kind == "waiting" and e.get("deadline") else None,
    )


_PLAN_CALL = "kairo.call/"
_PLAN_WAIT = "kairo.wait/"
_PLAN_WORKFLOW = "kairo.workflow"


def progress_kind(info: dict[str, Any]) -> tuple[ProgressRunKind, str]:
    """What run info is in a code workflow's tree, and its name."""
    plan: str = info["plan"]
    if plan == _PLAN_WORKFLOW:
        return "workflow", info.get("workflow") or ""
    if plan.startswith(_PLAN_CALL):
        return "call", plan[len(_PLAN_CALL):]
    if plan.startswith(_PLAN_WAIT):
        # A wait with a timeout: kairo.wait/<signal>@<ms>.
        return "wait", re.sub(r"@([0-9]+)$", lambda m: "" if int(m.group(1)) > 0 else m.group(0), plan[len(_PLAN_WAIT):])
    return "run", plan


def progress_run(info: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    """A run of the tree as RunProgress gives it."""
    kind, name = progress_kind(info)
    return {**info, "kind": kind, "name": name, "events": [run_event(e) for e in events]}
