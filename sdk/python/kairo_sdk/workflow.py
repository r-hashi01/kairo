"""Workflows written as code (ADR 0049).

A workflow is an ``async`` function that runs in this process; each call it
makes (an action, a wait, a clock read) is a run of its own in kairo, whose
id comes from the call: running the function again with the same workflow
id returns the calls that finished, and runs nothing twice.

The calls run on kairod (HttpBackend, the default) or on the runtime
embedded in this process with a database (EmbeddedBackend, ADR 0051).
With either backend, mode="suspend" serves hosts that do not stay up
(serverless): a workflow goes as far as it can now, and tick() and signal()
drive it on.

The process that drives a workflow holds a lease on it (ADR 0059, 0069):
if the process stops, another one that has the workflow takes it up once
the lease expires. The same behaviour as the TypeScript and Go SDKs
(sdk/ts/src/workflow.ts, sdk/go).
"""

from __future__ import annotations

import asyncio
import collections
import datetime
import decimal
import hashlib
import hmac
import inspect
import json
import logging
import math
import random
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .backend import Backend, HttpBackend
from .client import KairoError, finished
from .embedded import EffectWeakenedError, Embedded, new_token
from .http import SignatureError, check_url, post, sign, verify
from .metrics import (
    DRAIN,
    DRAIN_DURATION,
    DRAIN_STARTING_DRIVES,
    DRAIN_STARTING_TASKS,
    Metric,
    MetricEmitter,
    MetricSink,
)
from .observe import LOGGER, STEP_FINISHED, STEP_STARTED, Observation, Observer, observe, step_status
from .protocol import Result
from .store import ChildrenUnsettledError, LeaseRow, ParentUnavailableError, WorkflowRetryError, is_workflow_retry
from .worker import Task, TaskContext

PLAN_CALL = "kairo.call/"
PLAN_WAIT = "kairo.wait/"
PLAN_WORKFLOW = "kairo.workflow"
NOW, RANDOM, SLEEP = "kairo.now", "kairo.random", "kairo.sleep"
BUILTIN = (NOW, RANDOM, SLEEP)


class ResultLostError(Exception):
    """The workflow ran into a call whose result kairo no longer keeps."""


class Cancelled(Exception):
    """The workflow was cancelled."""


class RunSettledError(Exception):
    """Cancel did not change the workflow because it completed or failed."""


class Suspended(Exception):
    """The workflow waits (a timer, a signal): tick() or signal() drives it on."""


class StoppedError(Exception):
    """The caller stopped waiting (its task was cancelled, or it timed out),
    or the process is closing. The workflow is not cancelled: it goes on
    (ADR 0059)."""


class NotWaitingError(Exception):
    """send_signal(only_if_waiting=True) found no current wait (ADR 0064)."""


class TimedOutError(Exception):
    """A wait's timeout came before its signal (wait_for's timeout, ADR 0059)."""


@dataclass(frozen=True)
class SignalReceipt:
    """The wait that atomically accepted a signal (ADR 0064)."""

    run_id: str
    index: int  # zero-based occurrence of wait_for with this signal name
    early: bool  # this send made the wait before the workflow reached it


class RetryableError(Exception):
    """Raised by an action's handler: a failure that may be retried (it did
    not take effect)."""


class RetryAfterError(RetryableError):
    """A definite retryable refusal that asks the destination to wait."""

    def __init__(self, message: str, retry_after: float) -> None:
        super().__init__(message)
        self.retry_after = max(0.0, retry_after)


@dataclass(frozen=True)
class DestinationLimits:
    concurrency: int | None = None
    requests_per_minute: float | None = None
    weight_per_minute: float | None = None


class UnknownOutcomeError(Exception):
    """Raised by an action's handler: its outcome is unknown (it may have
    taken effect). Never taken for success (invariant 5): a real step stops
    for review, an unprotected one is retried."""


class CallError(RuntimeError):
    """A call that did not complete: it failed (its action's failure, after
    its retries) or was cancelled (then it is a Cancelled too).

    run_id: the call's run. action: the action called ("" for a wait).
    status: "failed" or "cancelled". message: the failure as recorded."""

    def __init__(self, run_id: str, action: str, status: str, message: str = "") -> None:
        what = f"{run_id} ({action})" if action else run_id
        super().__init__(f"call {what} {status}: {message}" if message else f"call {what} {status}")
        self.run_id = run_id
        self.action = action
        self.status = status
        self.message = message


class _CancelledCall(CallError, Cancelled):
    """A call that was cancelled: caught as CallError or as Cancelled."""


def _call_error(run_id: str, plan: str, status: str, message: str = "") -> CallError:
    # The action a call's plan calls ("" for a wait).
    action = plan[len(PLAN_CALL) :] if plan.startswith(PLAN_CALL) else ""
    if status == "cancelled":
        return _CancelledCall(run_id, action, status, message)
    return CallError(run_id, action, status, message)


class WorkflowError(RuntimeError):
    """A workflow that failed: its function raised (__cause__: what it
    raised, when it failed in this process). message: what is recorded."""

    def __init__(self, id: str, message: str) -> None:
        super().__init__(f"workflow {id} failed: {message}")
        self.id = id
        self.message = message


def _now_ms() -> int:
    return int(time.time() * 1000)


def _failure(e: BaseException) -> Result:
    """The result of a handler that raised."""
    error = f"{type(e).__name__}: {e}"
    if isinstance(e, UnknownOutcomeError):
        return Result(error=error, unknown=True, retryable=True, error_type=type(e).__name__)
    retry_after = round(e.retry_after * 1000) if isinstance(e, RetryAfterError) else 0
    return Result(error=error, retryable=isinstance(e, RetryableError), retry_after_ms=retry_after,
                  rate_limited=retry_after > 0, error_type=type(e).__name__)


def _to_wire(r: Result) -> tuple[dict[str, Any], bool]:
    """A result as an action over HTTP(S) answers it (ADR 0052), and whether
    it is unknown: an unknown outcome has no 200 body."""
    usage = {"weight": r.weight} if r.weight else {}
    if r.unknown:
        return {"error": r.error, **usage, **({"error_type": r.error_type} if r.error_type else {})}, True
    if r.error:
        w: dict[str, Any] = {"error": r.error}
        if r.retryable:
            w["retryable"] = True
        if r.retryable and r.retry_after_ms > 0:
            w["retry_after_ms"] = r.retry_after_ms
        w.update(usage)
        if r.error_type:
            w["error_type"] = r.error_type
        return w, False
    return {"output": r.output, **usage}, False


class _NotHere(KairoError):
    """A workflow run whose version this process does not have (another
    process's to drive, ADR 0060)."""

    def __init__(self, name: str, version: str) -> None:
        super().__init__(404, f"workflow {name} version {json.dumps(version)} is not registered here")


class _WorkflowDef:
    """A workflow's versions (ADR 0060): the current one starts new runs; the
    others only finish the runs they started."""

    def __init__(self) -> None:
        self.versions: dict[str, Callable[..., Awaitable[Any]]] = {}
        self.current = ""
        self.currents = 0


def _workflow_input(name: str, version: str, input: Any, external_parent: bool = False) -> dict[str, Any]:
    """A workflow run's input: the workflow, its version (when it has one)
    and the input it was given."""
    w: dict[str, Any] = {"workflow": name, "input": input}
    if version:
        w["version"] = version
    if external_parent:
        w["external_parent"] = True
    return w


def _externally_attached(input: Any) -> bool:
    return isinstance(input, dict) and input.get("external_parent") is True


def _workflow_of(input: Any) -> tuple[str, str, Any] | None:
    """Reads a workflow run's input: its workflow, version and input."""
    if not isinstance(input, dict) or not isinstance(input.get("workflow"), str) or not input["workflow"]:
        return None
    v = input.get("version")
    return input["workflow"], v if isinstance(v, str) else "", input.get("input")


class _DrivenElsewhere(Exception):
    """Another process holds the workflow's drive lease (ADR 0059)."""


class _StepStopped(Exception):
    """A step stopped waiting for a slot (ADR 0059)."""


@dataclass
class _Action:
    handler: Callable[..., Any]
    effect: str
    timeout: str | None
    destination: str | None
    url: str | None = None
    async_: bool = False
    limit: int | None = None
    rate: float | None = None
    weight_rate: float | None = None
    max_attempts: int | None = None
    backoff: str | None = None


def _canonical(v: Any) -> str:
    """JSON with object keys sorted: equal values, equal text, byte for byte
    what the TypeScript SDK's canonical (JSON.stringify of each leaf, keys in
    JavaScript's sort order) and the Go SDK's make of the same value, so that
    call ids agree across SDKs (ADR 0049, 0058)."""
    out: list[str] = []
    _write_canonical(out, v)
    return "".join(out)


def _recorded_signal(output: Any, payload: Any, signal_id: str, only_if_waiting: bool) -> bool | None:
    if not isinstance(output, dict) or not isinstance(output.get("_kairo_signal"), dict):
        return None
    meta = output["_kairo_signal"]
    if meta.get("id") != signal_id:
        return None
    if _canonical(output.get("payload")) != _canonical(payload) or bool(meta.get("only_if_waiting")) != only_if_waiting:
        raise ValueError(f"logical signal id {signal_id!r} conflicts with its recorded outcome")
    return bool(meta.get("early"))


_SURROGATE = re.compile("[\ud800-\udfff]")


def _write_canonical(out: list[str], v: Any) -> None:
    if v is None:
        out.append("null")
    elif v is True:
        out.append("true")
    elif v is False:
        out.append("false")
    elif isinstance(v, int):
        # A JavaScript number: exact up to 2**53, rounded beyond.
        if abs(v) <= 2**53:
            out.append(str(v))
        else:
            try:
                out.append(_js_number(float(v)))
            except OverflowError:
                out.append("null")
    elif isinstance(v, float):
        out.append(_js_number(v))
    elif isinstance(v, str):
        out.append(_js_string(v))
    elif isinstance(v, (list, tuple)):
        out.append("[")
        for i, e in enumerate(v):
            if i:
                out.append(",")
            _write_canonical(out, e)
        out.append("]")
    elif isinstance(v, dict):
        keys = {_js_key(k): x for k, x in v.items()}
        out.append("{")
        # JavaScript's default sort: by UTF-16 code units.
        for i, k in enumerate(sorted(keys, key=lambda k: k.encode("utf-16-be", "surrogatepass"))):
            if i:
                out.append(",")
            out.append(_js_string(k))
            out.append(":")
            _write_canonical(out, keys[k])
        out.append("}")
    else:
        raise TypeError(f"canonical: {type(v).__name__} is not JSON")


def _js_key(k: Any) -> str:
    """A dict key as json.dumps makes it a string."""
    if isinstance(k, str):
        return k
    if k is None or isinstance(k, (bool, int, float)):
        return _canonical(k)
    raise TypeError(f"canonical: a key of {type(k).__name__}")


def _js_string(s: str) -> str:
    """s as JSON.stringify writes it: ", \\ and control characters escaped
    (as json.dumps does), and lone surrogates as \\uXXXX."""
    return _SURROGATE.sub(lambda m: f"\\u{ord(m.group()):04x}", json.dumps(s, ensure_ascii=False))


def _js_number(x: float) -> str:
    """x as JavaScript writes a number (Number::toString): the shortest
    digits that round-trip (Python's repr has the same), placed as
    JavaScript places them; NaN and the infinities are null, -0 is 0."""
    if math.isnan(x) or math.isinf(x):
        return "null"
    if x == 0:
        return "0"
    sign = "-" if x < 0 else ""
    t = decimal.Decimal(repr(abs(x))).normalize().as_tuple()
    s = "".join(map(str, t.digits))
    k = len(s)
    n = int(t.exponent) + k  # x = 0.s * 10**n
    if k <= n <= 21:
        return sign + s + "0" * (n - k)
    if 0 < n <= 21:
        return sign + s[:n] + "." + s[n:]
    if -6 < n <= 0:
        return sign + "0." + "0" * -n + s
    e = n - 1
    return sign + (s if k == 1 else s[0] + "." + s[1:]) + "e" + ("+" if e >= 0 else "-") + str(abs(e))


def _call_id(workflow: str, kind: str, input: Any, n: int) -> str:
    h = hashlib.sha256((kind + "\0" + _canonical(input)).encode()).hexdigest()[:24]
    return f"{workflow}/{h}.{n}"


def _wait_root(name: str, timeout_ms: int = 0) -> dict[str, Any]:
    """The plan of a wait for signal name, with a timeout (ms; 0: none)."""
    root: dict[str, Any] = {"kind": "wait", "id": "w", "signal": name}
    if timeout_ms > 0:
        root["timeout"] = f"{timeout_ms}ms"
    return root


def _wait_plan(name: str) -> dict[str, Any] | None:
    """A wait's plan from its name (kairo.wait/<signal>[@<ms>]): one made by another process (ADR 0059)."""
    if not name.startswith(PLAN_WAIT):
        return None
    rest = name[len(PLAN_WAIT) :]
    signal, at, ms = rest.rpartition("@")
    if at and ms.isdigit() and int(ms) > 0:
        return {"name": name, "root": _wait_root(signal, int(ms))}
    return {"name": name, "root": _wait_root(rest)}


def _set(f: asyncio.Future[Any]) -> None:
    if not f.done():
        f.set_result(None)


def _ms(t: int | float | datetime.datetime | None) -> int | None:
    if isinstance(t, datetime.datetime):
        return round(t.timestamp() * 1000)
    return None if t is None else int(t)


class Kairo:
    """Actions served here, and workflows driven here."""

    def __init__(
        self,
        url: str = "http://127.0.0.1:8420",
        *,
        worker: str | tuple[str, int] | None = None,
        token: str = "",
        tenant: str = "default",
        idempotency_ttl: float = 24 * 3600,
        concurrency: int | None = None,
        destination_limits: Callable[[str], DestinationLimits] | None = None,
        request_timeout: float = 90.0,
        backend: Backend | None = None,
        mode: str = "wait",
        secret: str | None = None,
        callback_url: str | None = None,
        ca: str | None = None,
        allow_insecure: bool = False,
        wait_until: Callable[[Awaitable[Any]], None] | None = None,
        tick_secret: str | None = None,
        wake: Callable[[int], Awaitable[None]] | None = None,
        logger: logging.Logger | None = None,
        observe: Observer | None = None,
        metrics: MetricSink | None = None,
    ) -> None:
        """concurrency: steps at once in this process, on kairod's worker or
        in the embedded runtime (ADR 0059; kairo.now, kairo.random and
        kairo.sleep are not counted). Steps over it wait, leased, for a slot.
        Default: no limit.

        Actions over HTTP(S) (ADR 0052): secret signs the calls and the
        callbacks; callback_url is where this process takes outcomes (the
        ASGI app's /callback); ca, certificates to trust (PEM) besides the
        system's; allow_insecure allows plain http to other machines;
        wait_until keeps the host running async actions after the answer.

        Suspend mode (ADR 0053): tick_secret is the bearer token a scheduler
        calls the ASGI app's /tick with (without it, /tick is not served);
        wake is told, before run, tick, signal and callbacks return, when
        something is next to do (unix ms): schedule a one-off tick then,
        instead of polling.

        logger takes what goes wrong in the runtime's background work: lease
        renewals, sweeps, timers, steps' outcomes, workflows driven in the
        background, callbacks (default: logging.getLogger("kairo_sdk")).
        observe, if given, is given each Observation as it happens: runs that
        start and settle, steps' attempts that start and finish (to count,
        time and trace them). It is called in the runtime, as it works (on
        the event loop; with kairod, on the worker's threads, and only for
        the steps it runs here): it must not block."""
        self._metrics = MetricEmitter(metrics) if metrics is not None else None
        self.backend: Any = backend or HttpBackend(
            url,
            worker=worker,
            token=token,
            tenant=tenant,
            concurrency=concurrency,
            request_timeout=request_timeout,
            metrics=self._metrics,
        )
        if isinstance(self.backend, HttpBackend) and self._metrics is not None:
            self.backend.metrics = self._metrics
        self._ttl = idempotency_ttl
        self._concurrency = concurrency
        self._destination_limits = destination_limits
        self._actions: dict[str, _Action] = {}
        self._workflows: dict[str, _WorkflowDef] = {}
        self._planned: set[str] = set()
        # The stops of the workflows driven here: set when the process closes.
        self._stops: set[asyncio.Future[None]] = set()
        self._stops_by_id: dict[str, asyncio.Future[None]] = {}
        self._cancel_requests: dict[str, asyncio.Future[bool]] = {}
        self._tree_cancellations: dict[str, int] = {}
        # Closing: calls interrupted now go on in kairo, to be resumed (not cancelled).
        self._closing = False
        # Draining (drain): no new call starts here.
        self._draining = False
        if mode not in ("wait", "suspend"):
            raise ValueError(f"mode {mode}: wait or suspend")
        self.suspend = mode == "suspend"
        if self.suspend and not (hasattr(self.backend, "idle") or isinstance(self.backend, HttpBackend)):
            raise ValueError("suspend mode needs the embedded backend or kairod")
        # Suspend mode: workflows being driven on here, and those to drive again after.
        self._driving_ids: set[str] = set()
        self._redriving: set[str] = set()
        self._again: set[str] = set()
        self._redrives: set[asyncio.Future[Any]] = set()
        self._stop_hook: Callable[[], None] | None = None
        self._secret = secret
        self._callback_url = callback_url
        self._ca = ca
        self._allow_insecure = allow_insecure
        self._wait_until = wait_until
        self._tick_secret = tick_secret
        self._wake = wake
        self._background: set[asyncio.Future[Any]] = set()
        self._tick_lock = asyncio.Lock()
        # The embedded runtime, when that is the backend (drive leases, ADR 0059).
        rt = getattr(self.backend, "runtime", None)
        self._rt: Embedded | None = rt if isinstance(rt, Embedded) else None
        if self._rt is not None:
            if logger is not None:
                self._rt.logger = logger
            if observe is not None:
                self._rt.observer = observe
        self._logger = logger or (self._rt.logger if self._rt is not None else LOGGER)
        # The clock (unix ms): the embedded runtime's (its now), or the system's.
        self._now: Callable[[], int] = self._rt.now if self._rt is not None else _now_ms
        # With kairod, the steps its worker runs here are observed here (the
        # embedded runtime observes its own).
        self._observer = observe if self._rt is None else None
        # Wait mode: workflows driven in the background here (by id), and their drives.
        self._owned: set[str] = set()
        self._drives: set[asyncio.Future[Any]] = set()
        self._drives_idle = asyncio.Event()
        self._drives_idle.set()
        # Ends with the process (close): what waits in it stops.
        self._life: asyncio.Future[None] | None = None
        # Wait mode: the sweep, once a lease period, of what stopped processes left (ADR 0059).
        self._sweeper: asyncio.Future[None] | None = None
        # Limits (ADR 0059): steps at once in this process (embedded), and by destination.
        self._slots: _Slots | None = None
        self._limits: dict[str, _Limiter] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def metric_stats(self) -> tuple[int, int]:
        """Measurements dropped before delivery, and sink failures."""
        if self._metrics is None:
            return 0, 0
        return self._metrics.dropped, self._metrics.failed

    def action(
        self,
        name: str,
        *,
        effect: str = "real",
        timeout: str | None = None,
        destination: str | None = None,
        url: str | None = None,
        async_: bool = False,
        allow_insecure: bool | None = None,
        limit: int | None = None,
        rate: float | None = None,
        weight_rate: float | None = None,
        max_attempts: int | None = None,
        backoff: str | None = None,
    ):
        """Declares an action: "real" (the default) acts outside and never runs
        twice; "unprotected" may run again. The handler takes the input and a
        TaskContext; it may be a plain or an async function.

        destination: the rate-limit key (default: the action). limit: at most
        this many of its steps run in this process at once; rate: at most
        this many start in this process a minute, spaced evenly (ADR 0059).
        Actions of one destination share limit and rate: the strictest.

        max_attempts: a step of the action is tried at most this many times
        when it fails retryably (RetryableError; default 3, or 1 for a real
        action: such a step is retried only when it did not take effect).
        backoff: the wait before a step's second attempt, doubled for each
        one after (e.g. "10ms"; default "200ms").

        url (ADR 0052): the action's steps are called over HTTP(S) there, and
        the handler runs there (asgi_app). async_: where the URL serves it,
        it answers at once (202) and sends the outcome to the callback."""
        if name.startswith("kairo."):
            raise ValueError(f'action {name}: names beginning with "kairo." are kairo\'s')
        if url:
            check_url(url, self._allow_insecure if allow_insecure is None else allow_insecure)

        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._actions[name] = _Action(fn, effect, timeout, destination, url, async_, limit, rate, weight_rate, max_attempts, backoff)
            return fn

        return register

    def workflow(self, name: str, *, version: str = "", draining: bool = False):
        """Declares a workflow: ``async def fn(ctx, input)``; or one of its
        versions (ADR 0060). A run is driven by the version it started with,
        to its end: a process drives only the runs whose version it has.
        draining: this version only finishes the runs that started with it;
        new runs start with the current one (the one not draining)."""

        def register(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
            d = self._workflows.setdefault(name, _WorkflowDef())
            if not draining:
                d.current = version
                d.currents += 1
            d.versions[version] = fn
            return fn

        return register

    def _fn_for(self, name: str, version: str) -> Callable[..., Awaitable[Any]] | None:
        """Workflow name's function at version (None: not here)."""
        d = self._workflows.get(name)
        return d.versions.get(version) if d is not None else None

    def _current_version(self, name: str) -> str | None:
        """The version new runs of workflow name start with (None: no such workflow)."""
        d = self._workflows.get(name)
        return d.current if d is not None and d.currents > 0 else None

    async def start(self) -> None:
        """Registers the actions and their plans, and starts running the actions' steps."""
        for name, d in self._workflows.items():
            if d.currents != 1:
                raise ValueError(f"workflow {name}: {d.currents} current versions (one, the others draining)")
        self._loop = asyncio.get_running_loop()
        self._life_future()
        specs: list[dict[str, Any]] = []
        for name, a in self._actions.items():
            s: dict[str, Any] = {"action": name, "effect": a.effect, "destination_from": "$kairo_destination"}
            if a.timeout:
                s["timeout"] = a.timeout
            if a.destination:
                s["destination"] = a.destination
            if a.max_attempts:
                s["max_attempts"] = a.max_attempts
            if a.backoff:
                s["backoff"] = a.backoff
            specs.append(s)
        for name in BUILTIN:
            specs.append({"action": name, "effect": "unprotected"})
        if any(a.url for a in self._actions.values()):
            if not self._secret:
                raise ValueError("actions with a url need secret")
            if isinstance(self.backend, HttpBackend):
                self.backend.callback_actions = [name for name, a in self._actions.items() if a.url]
            else:
                if not self._callback_url:
                    raise ValueError("actions with a url need callback_url with the embedded backend")
                check_url(self._callback_url, self._allow_insecure)
                if not hasattr(self.backend, "complete"):
                    raise ValueError("actions with a url need the embedded backend or kairod")
        for name, a in self._actions.items():
            if not a.limit and not a.rate and not a.weight_rate:
                continue
            # Actions of one destination share its limits: the strictest.
            key = a.destination or name
            self._limits.setdefault(key, _Limiter()).declare(DestinationLimits(a.limit, a.rate, a.weight_rate))
        rt = self._rt
        # kairod's worker limits its own tasks.
        if rt is not None and self._concurrency:
            self._slots = _Slots(self._concurrency)
        if rt is not None and (self._slots or self._limits or self._destination_limits):
            # Steps wait for their slots on the event loop, before their thread.
            rt.admit = self._admit_step
        await self.backend.start(specs, self._serve_observed if self._observer is not None else self._serve)
        for s in specs:
            await self._plan(PLAN_CALL + s["action"], {"kind": "step", "id": "call", "action": s["action"],
                                                         "input": {"in": "$input.in", "$kairo_destination": "$input.destination"}})
        await self._plan_workflow()
        if rt is not None:
            rt.lost_drive = self._lost_drive
            rt.wake_parent = self._redrive
            rt.plan_for = _wait_plan
            rt.lease_parents = self.suspend
        if self.suspend and rt is not None:
            # A call that settles drives its workflow on (its id is the
            # workflow's id, "/", the call's key).
            def settled(r: dict[str, Any]) -> None:
                rid = r["run_id"]
                i = rid.rfind("/")
                if i > 0:
                    self._redrive(rid[:i])

            self._stop_hook = self.backend.on_settled(settled)
        elif rt is not None:
            # Wait mode: a resident process takes up, once a lease period, what
            # processes that stopped left (ADR 0059): their workflows, steps and
            # timers. One task for the process, not one for each run.
            self._sweeper = asyncio.ensure_future(self._sweep(rt))

    async def _sweep(self, rt: Embedded) -> None:
        life = self._life_future()
        while not self._closing:
            try:
                if not self._draining:  # draining: left to other processes
                    await rt.tick()
                await rt.recheck()
            except Exception as e:
                # Again next time.
                if not self._closing:
                    self._logger.warning("kairo: sweeping: %s", e)
            if self._closing:
                return
            await asyncio.wait({life}, timeout=rt.lease / 1000)

    def _life_future(self) -> asyncio.Future[None]:
        if self._life is None:
            self._life = asyncio.get_running_loop().create_future()
            if self._closing:
                _set(self._life)
        return self._life

    async def drain(self) -> None:
        started = time.monotonic() if self._metrics is not None else 0.0
        if self._metrics is not None:
            self._metrics.emit(
                Metric(DRAIN_STARTING_TASKS, "histogram", "{task}", float(getattr(self.backend, "inflight", 0)))
            )
            self._metrics.emit(Metric(DRAIN_STARTING_DRIVES, "histogram", "{drive}", float(len(self._stops))))
        outcome = "failed"
        try:
            await self._drain()
            outcome = "completed"
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except Exception as error:
            if type(error).__name__ == "DrainUnsupportedError":
                outcome = "unsupported"
            raise
        finally:
            if self._metrics is not None:
                labels = {"outcome": outcome}
                self._metrics.emit(Metric(DRAIN, "counter", "{event}", 1, labels))
                self._metrics.emit(Metric(DRAIN_DURATION, "histogram", "s", time.monotonic() - started, labels))

    async def _drain(self) -> None:
        """Stops this process at the edges of its steps, before close: no new
        step starts here (a workflow driven here stops at its next call, to go
        on where it is driven next; timers and sweeps are left to a tick here
        or elsewhere), and the steps already running run to their end and
        have their outcomes recorded. Returns once they have; a caller that
        stops waiting first (its task cancelled, or timed out: asyncio.timeout)
        leaves them running, and close then stops what still runs, as a
        process that stops (a real step then waits for review in the next
        process). After drain, close."""
        rt = self._rt
        self._draining = True
        if rt is not None:
            rt.drain()
        for stop in list(self._stops):
            _set(stop)  # stops at its next call; its drive lease is handed over
        if rt is not None:
            await rt.wait_idle()
        else:
            self._logger.info(
                "kairo: draining kairod worker: worker_tasks=%d workflow_drives=%d",
                getattr(self.backend, "inflight", 0),
                len(self._stops),
            )
            try:
                await self.backend.drain()
                await self._drives_idle.wait()
            except asyncio.CancelledError:
                self._logger.warning("kairo: kairod worker drain stopped")
                raise
            self._logger.info("kairo: kairod worker drained")

    async def close(self) -> None:
        """Stops serving actions and driving workflows, as a process that stops
        does: the workflows it drove stay unfinished in kairo, to be resumed."""
        self._closing = True
        if self._stop_hook is not None:
            self._stop_hook()
        if self._life is not None:
            _set(self._life)
        for stop in list(self._stops):
            _set(stop)
        # The drives here end; their leases are left expired, for another
        # process to take up (ADR 0059).
        waits = [*self._drives, *self._redrives] + ([self._sweeper] if self._sweeper is not None else [])
        await asyncio.gather(*waits, return_exceptions=True)
        await self.backend.close()
        if self._metrics is not None:
            self._metrics.close()

    async def tick(self) -> int | None:
        """Suspend mode: takes up due timers and steps whose process stopped,
        drives on the workflows whose calls settle, and returns once that is
        done (call it from a scheduler). Returns when something is next to
        do (unix ms), or None."""
        async with self._tick_lock:
            return await self._tick()

    async def _tick(self) -> int | None:
        if isinstance(self.backend, HttpBackend):
            self.backend.clear_due(self._now())
            while True:
                claim = await self.backend.claim(self._supported_workflows())
                if claim is None:
                    break
                run = claim["run"]
                w = _workflow_of(run.get("input"))
                if w is None:
                    await self.backend.release(claim)
                    continue
                try:
                    await self._run_as(w[0], w[2], run["run_id"], run.get("parent"))
                except (Suspended, _NotHere):
                    await self.backend.release(claim)
                except BaseException as e:
                    await self.backend.release(claim, is_workflow_retry(e))
                    if not is_workflow_retry(e):
                        raise
                else:
                    await self.backend.release(claim)
        else:
            await self.backend.tick()
        await self._settle()
        return await self._wake_up()

    def _supported_workflows(self) -> dict[str, list[str]]:
        return {name: sorted(d.versions) for name, d in self._workflows.items()}

    async def _wake_up(self) -> int | None:
        """Suspend mode: tells wake when something is next to do (ADR 0053); returns it."""
        if not self.suspend or not hasattr(self.backend, "next_wake"):
            return None
        at = await self.backend.next_wake()
        if at is not None and self._wake is not None:
            await self._wake(at)
        return at

    async def _settle(self) -> None:
        """Returns once the work started here and the workflows driven on are done."""
        if not hasattr(self.backend, "idle"):
            return
        while True:
            await self.backend.idle()
            # Finished ones are dropped here, whether or not their done
            # callbacks ran yet.
            self._redrives.difference_update([f for f in self._redrives if f.done()])
            if not self._redrives:
                await self.backend.idle()
                self._redrives.difference_update([f for f in self._redrives if f.done()])
                if not self._redrives:
                    return
            await asyncio.gather(*list(self._redrives), return_exceptions=True)

    def _redrive(self, id: str) -> None:
        """Suspend mode: drives workflow id on (again, if it is being driven now)."""
        if id in self._redriving:
            self._again.add(id)
            return
        if id in self._driving_ids:
            return

        self._redriving.add(id)

        async def drive() -> None:
            try:
                while True:
                    self._again.discard(id)
                    try:
                        info = await self.backend.get(id)
                    except KairoError:
                        return
                    w = _workflow_of(info.get("input"))
                    if info.get("plan") != PLAN_WORKFLOW or finished(info) or w is None:
                        return
                    try:
                        await self._drive(id, w[0], w[2], info.get("parent"), 0)
                    except Exception:
                        # Suspended again, failed (recorded), cancelled, driven
                        # elsewhere, or of a version not here.
                        pass
                    if id not in self._again:
                        return
            finally:
                self._redriving.discard(id)

        fut = asyncio.ensure_future(drive())
        self._redrives.add(fut)
        fut.add_done_callback(self._redrives.discard)

    async def _plan(self, name: str, root: dict[str, Any], vars: dict[str, Any] | None = None) -> None:
        if name in self._planned:
            return
        d: dict[str, Any] = {"name": name, "root": root}
        if vars:
            d["vars"] = vars
        await self.backend.register_plan(d)
        self._planned.add(name)

    async def _plan_workflow(self) -> None:
        await self._plan(PLAN_WORKFLOW, {"kind": "wait", "id": "done", "signal": "done"}, {"started_at": {"type": "integer", "value": 0}})

    async def _admit(self, action: str, destination: str, ctx: TaskContext) -> Callable[[Result], None]:
        """Waits, on the event loop, for a step of action to be let run (ADR
        0059): its destination's slot and turn, then the process's slot.
        Returns what gives the slots back (on the event loop). Raises
        _StepStopped if the step is cancelled first."""
        a = self._actions.get(action)
        lim: _Limiter | None = None
        slots = self._slots if a is not None and action not in BUILTIN else None
        held: list[_Slots] = []

        def release_slots() -> None:
            while held:
                held.pop().give()

        if a is not None and action not in BUILTIN:
            limits = DestinationLimits(a.limit, a.rate, a.weight_rate)
            if self._destination_limits is not None:
                dynamic = self._destination_limits(destination)
                limits = DestinationLimits(
                    _stricter(limits.concurrency, dynamic.concurrency),
                    _stricter(limits.requests_per_minute, dynamic.requests_per_minute),
                    _stricter(limits.weight_per_minute, dynamic.weight_per_minute),
                )
            lim = self._limits.setdefault(destination, _Limiter())
            lim.declare(DestinationLimits(a.limit, a.rate, a.weight_rate))
        if lim is None and slots is None:
            return lambda _result: release_slots()
        stop: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        loop = asyncio.get_running_loop()

        def stopped() -> None:
            try:
                loop.call_soon_threadsafe(_set, stop)
            except RuntimeError:
                pass  # the loop is closed: nothing waits on it any more

        off = ctx.cancelled.on_set(stopped) if hasattr(ctx.cancelled, "on_set") else (lambda: None)
        admitted = False
        try:
            if lim is not None:
                await lim.admit(limits, stop)
                admitted = True
            if slots is not None:
                await slots.take(stop)
                held.append(slots)
            def finish(result: Result) -> None:
                release_slots()
                if lim is not None:
                    retry_after = result.retry_after_ms if result.retryable and not result.unknown else 0
                    lim.finish(result.weight or result.tokens, retry_after)
                    self._retire_limiter(destination, lim)
            return finish
        except BaseException:
            release_slots()
            if admitted and lim is not None:
                lim.finish(0, 0)
            raise
        finally:
            off()

    def _retire_limiter(self, destination: str, limiter: _Limiter) -> None:
        after = limiter.idle_after()
        if after is None:
            return
        if after <= 0:
            if self._limits.get(destination) is limiter and limiter.idle_after() == 0:
                self._limits.pop(destination, None)
            return
        asyncio.get_running_loop().call_later(after + 0.001, self._retire_limiter, destination, limiter)

    async def _admit_step(self, task: Task, ctx: TaskContext) -> Callable[[], None] | Result:
        """The embedded runtime's admission of a dispatched step (ADR 0059)."""
        try:
            return await self._admit(task.action, task.destination or task.action, ctx)
        except _StepStopped:
            return _stopped_waiting(task.action)

    def _serve_observed(self, task: Task, ctx: TaskContext) -> Result:
        """_serve, observed (kairod's worker; on its thread)."""
        step = {"run_id": task.run_id, "action": task.action, "step_id": task.step_id, "attempt": task.attempt}
        observe(self._observer, self._logger, self._now, Observation(STEP_STARTED, **step))
        began = time.monotonic()
        res = self._serve(task, ctx)
        duration = time.monotonic() - began
        observe(self._observer, self._logger, self._now, Observation(STEP_FINISHED, **step, status=step_status(res), error=res.error, duration=duration))
        return res

    def _serve(self, task: Task, ctx: TaskContext) -> Result:
        input = (task.input or {}).get("in") if isinstance(task.input, dict) else None
        if task.action == NOW:
            return Result(output=self._now())
        if task.action == RANDOM:
            return Result(output=random.random())
        if task.action == SLEEP:
            # Waits in kairo, not here (ADR 0045).
            return Result(wait={"until": int(self._now() + float(input["ms"])), "output": None})
        a = self._actions.get(task.action)
        if a is None:
            return Result(error=f"no action {task.action} here")
        # On kairod's worker (a thread of its own), over a limit, the step
        # waits here for a slot (ADR 0059); the embedded runtime admits it
        # before its thread.
        finish: Callable[[Result], None] | None = None
        if self._rt is None and (self._limits or self._destination_limits) and self._loop is not None:
            try:
                finish = asyncio.run_coroutine_threadsafe(
                    self._admit(task.action, task.destination or a.destination or task.action, ctx), self._loop
                ).result()
            except _StepStopped:
                return _stopped_waiting(task.action)
        result: Result
        try:
            if a.url:
                result = self._call_remote(task, a, input)
            else:
                out = a.handler(input, ctx)
                if inspect.isawaitable(out):
                    out = asyncio.run(_await(out))
                result = Result(output=out)
        except Exception as e:  # the handler's failure, of the kind it says
            result = _failure(e)
        if not result.weight and ctx._weight:
            result.weight = ctx._weight
        if finish is not None:
            self._loop.call_soon_threadsafe(finish, result)  # type: ignore[union-attr]
        return result

    def _call_remote(self, task: Task, a: _Action, input: Any) -> Result:
        """Calls an action over HTTP(S) (ADR 0052); on the step's thread."""
        assert a.url and self._secret
        callback = task.callback.get("url") if task.callback else None
        if callback is None and self._rt is not None:
            callback = self._callback_url
        body = json.dumps(
            {
                "run_id": task.run_id,
                "step_id": task.step_id,
                "act": task.act,
                "attempt": task.attempt,
                "action": task.action,
                "input": input,
                "idempotency_key": task.idempotency_key,
                "destination": task.destination,
                "callback": callback,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        lease_ms = _duration_ms(a.timeout) or 15 * 60 * 1000
        try:
            r = post(
                a.url,
                body,
                ca=self._ca,
                timeout=min(lease_ms, 60_000) / 1000,
                headers={"Idempotency-Key": task.idempotency_key, "Kairo-Signature": sign(self._secret, body)},
            )
        except OSError as e:
            # It may have run: unknown (invariant 5).
            return Result(error=f"calling {a.url}: {e}", unknown=True, retryable=True, error_type="http")
        if r.status == 202:
            if not callback:
                return Result(
                    error="the async action did not receive a callback",
                    unknown=True,
                    retryable=True,
                    error_type="unsupported",
                )
            return Result(pending={"owner": f"remote:{a.url}", "lease_ms": lease_ms})
        if 200 <= r.status < 300:
            try:
                return _from_wire(json.loads(r.body))
            except (ValueError, TypeError):
                return Result(error=f"{a.url}: the answer is not JSON", unknown=True, retryable=True, error_type="http")
        if 400 <= r.status < 500:
            return Result(error=f"{a.url}: {r.status} {r.body[:200].decode(errors='replace')}", error_type=f"http_{r.status}")
        return Result(error=f"{a.url}: {r.status}", unknown=True, retryable=True, error_type=f"http_{r.status}")

    def asgi_app(self) -> Callable[..., Awaitable[None]]:
        """Serves actions called over HTTP(S) and takes their outcomes (ADR
        0052), as an ASGI app: POST <base>/action runs an action's handler
        here; POST <base>/callback applies an outcome to its run. Both are
        signed with secret. GET or POST <base>/tick, with "Authorization:
        Bearer <tick_secret>", is a scheduler's tick (ADR 0053): it answers
        {"next": <unix ms> | null}."""

        async def app(scope: dict[str, Any], receive: Callable[[], Awaitable[dict[str, Any]]], send: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
            if scope["type"] != "http":
                return
            body = b""
            while True:
                m = await receive()
                body += m.get("body", b"")
                if not m.get("more_body"):
                    break
            headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
            try:
                if scope["path"].endswith("/tick"):
                    status, out = await self._handle_tick(scope["method"], headers.get("authorization"))
                else:
                    status, out = await self._handle(scope["method"], scope["path"], headers.get("kairo-signature"), body)
            except Exception:
                if not self._closing:
                    raise
                # Closed while it was answered (its store too): as before it.
                status, out = 503, {"error": "closing"}
            data = json.dumps(out).encode()
            await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", b"application/json")]})
            await send({"type": "http.response.body", "body": data})

        return app

    async def _handle_tick(self, method: str, authorization: str | None) -> tuple[int, Any]:
        if self._closing:
            return 503, {"error": "closing"}
        if not self._tick_secret or not self.suspend:
            return 404, {"error": "no such path"}
        if method not in ("GET", "POST"):
            return 405, {"error": "GET or POST"}
        if not hmac.compare_digest((authorization or "").encode(), f"Bearer {self._tick_secret}".encode()):
            return 401, {"error": "bad token"}
        return 200, {"next": await self.tick()}

    async def _handle(self, method: str, path: str, signature: str | None, body: bytes) -> tuple[int, Any]:
        if self._closing:
            # Closed (its store too): the caller tries again, or another
            # process takes the step up when its lease expires.
            return 503, {"error": "closing"}
        if method != "POST":
            return 405, {"error": "POST only"}
        if not self._secret:
            return 500, {"error": "no secret configured"}
        try:
            verify(self._secret, body, signature)
        except SignatureError as e:
            return 401, {"error": str(e)}
        m = json.loads(body)
        if path.endswith("/callback"):
            if not hasattr(self.backend, "complete"):
                return 501, {"error": "no embedded runtime here"}
            await self.backend.complete(m["run_id"], m["act"], m["attempt"], _from_wire(m["result"]))
            if self.suspend:
                await self._settle()
                await self._wake_up()
            return 200, {"ok": True}
        if not path.endswith("/action"):
            return 404, {"error": "no such path"}
        a = self._actions.get(m.get("action", ""))
        if a is None:
            return 404, {"error": f"no action {m.get('action')} here"}

        async def run() -> tuple[dict[str, Any], bool]:
            ctx = TaskContext(lambda _d: None)
            try:
                out = await asyncio.to_thread(a.handler, m.get("input"), ctx)
                if inspect.isawaitable(out):
                    out = await out
                return _to_wire(Result(output=out, weight=ctx._weight))
            except Exception as e:
                result = _failure(e)
                result.weight = ctx._weight
                return _to_wire(result)

        if not a.async_:
            # An unknown outcome: 502, which the caller takes as unknown.
            result, unknown = await run()
            return (502 if unknown else 200), result

        # Answer now; run after, and send the outcome to the callback.
        async def work() -> None:
            result, unknown = await run()
            if unknown:
                return  # no callback: the lease expires and the step is taken up
            cb = json.dumps({"run_id": m["run_id"], "act": m["act"], "attempt": m["attempt"], "result": result}).encode()
            try:
                check_url(m["callback"], self._allow_insecure)
                await asyncio.to_thread(post, m["callback"], cb, ca=self._ca, headers={"Kairo-Signature": sign(self._secret or "", cb)})
            except (OSError, ValueError) as e:
                # A lost callback: the lease expires and the step is taken up.
                self._logger.warning("kairo: action %s: its callback: %s", m.get("action"), e)

        fut = asyncio.ensure_future(work())
        self._background.add(fut)
        fut.add_done_callback(self._background.discard)
        if self._wait_until is not None:
            self._wait_until(fut)
        return 202, {"accepted": True}

    async def run(
        self,
        name: str,
        input: Any = None,
        *,
        id: str | None = None,
        meta: dict[str, Any] | None = None,
        parent: str | None = None,
    ) -> Any:
        """Runs workflow name as execution id, or resumes it: calls that
        finished return their recorded results. Returns its result.

        In wait mode, with the embedded runtime, the workflow is driven in
        the background, by this process or by whichever process drives it
        now (ADR 0059). In wait mode, a caller that stops waiting (its task
        cancelled, or timed out: asyncio.timeout, asyncio.wait_for) gets
        StoppedError, as does one waiting when the process closes; the
        workflow is not cancelled and goes on. In suspend mode it is driven
        here until it waits: Suspended. meta is kept with a new workflow
        (its info's "meta", list)."""
        id = id or str(uuid.uuid4())
        try:
            token, actual_parent = await self._begin(name, input, id, meta, parent)
            if self.suspend:
                try:
                    return await self._drive(id, name, input, actual_parent, token)
                except _DrivenElsewhere as e:
                    raise Suspended(str(e)) from None
            # kairod: driven here, as the caller waits.
            if self._rt is None:
                return await self._drive(id, name, input, actual_parent, 0)
            self._drive_background(id, name, input, actual_parent, token)
            return await self.result(id)
        except asyncio.CancelledError:
            if self.suspend:
                raise
            raise StoppedError(f"workflow {id}: stopped waiting for it") from None
        finally:
            await self._wake_up()

    async def submit(
        self,
        name: str,
        input: Any = None,
        *,
        id: str | None = None,
        meta: dict[str, Any] | None = None,
        parent: str | None = None,
    ) -> str:
        """Starts workflow name and returns its id once the start is recorded,
        without waiting for it (ADR 0059): result(id) gives its result, from
        any process. In wait mode it is driven in the background; in suspend
        mode it is driven here until it waits (what it comes to is recorded:
        nothing is raised). The id is an idempotency key, as in run."""
        id = id or str(uuid.uuid4())
        token, actual_parent = await self._begin(name, input, id, meta, parent)
        if not self.suspend:
            self._drive_background(id, name, input, actual_parent, token)
            return id
        try:
            await self._drive(id, name, input, actual_parent, token)
        except Exception:
            pass
        await self._wake_up()
        return id

    async def result(self, id: str) -> Any:
        """Workflow id's result once it has finished. In wait mode it waits;
        a caller that stops waiting (its task cancelled, or timed out), or
        the process closing, gets StoppedError (the workflow goes on). In
        suspend mode it does not wait: Suspended while the workflow has not
        finished."""
        if self.suspend:
            info = await self.backend.get(id)
            if not finished(info):
                raise Suspended(f"workflow {id} has not finished")
            return _done(id, info)
        life = self._life_future()
        waiter = asyncio.ensure_future(self.backend.wait(id))
        try:
            await asyncio.wait({waiter, life}, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            waiter.cancel()
            raise StoppedError(f"workflow {id}: stopped waiting for it") from None
        if not waiter.done():
            waiter.cancel()
            raise StoppedError(f"workflow {id}: stopped waiting for it (the process closes)")
        return _done(id, waiter.result())

    async def list(
        self,
        *,
        workflow: str | None = None,
        status: str | None = None,
        since: int | datetime.datetime | None = None,
        until: int | datetime.datetime | None = None,
        after: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Workflows started here or elsewhere (root runs: not child
        workflows, not calls), in the order they were created (ADR 0059): of
        workflow, in status, created in [since, until) (unix ms or datetime),
        after the run after, at most limit. Each with its "meta",
        "created_at" and "updated_at" (unix ms)."""
        return await self.backend.list(workflow=workflow, status=status, since=_ms(since), until=_ms(until), after=after, limit=limit)

    async def children(self, id: str) -> list[dict[str, Any]]:
        """Runs made directly by id: calls, waits and child workflows."""
        return await self.backend.children(id)

    async def _begin(
        self, name: str, input: Any, id: str, meta: dict[str, Any] | None, requested_parent: str | None = None
    ) -> tuple[int, str | None]:
        """Makes workflow name's run as id (or finds it). A new one is leased to
        this process to drive, in the same transaction: its token (0: it
        existed, or no leases here)."""
        version = self._current_version(name)
        if version is None:
            raise KeyError(f"no workflow {name}")
        await self._plan_workflow()
        token = new_token() if self._rt is not None or isinstance(self.backend, HttpBackend) else 0
        started = await self.backend.run(
            PLAN_WORKFLOW,
            _workflow_input(name, version, input, requested_parent is not None),
            run_id=id,
            vars={"started_at": self._now()},
            parent=requested_parent,
            guard_parent=requested_parent is not None,
            workflow=name,
            meta=meta,
            drive=token,
        )
        if not started.get("existing"):
            return token, requested_parent
        info = await self.backend.get(id)
        actual_parent = info.get("parent")
        if requested_parent is not None and requested_parent != actual_parent:
            raise ParentUnavailableError(
                f"parent {requested_parent}: run {id} already belongs to parent {actual_parent!r}"
            )
        if actual_parent is not None and not finished(info):
            try:
                p = await self.backend.get(actual_parent)
            except KairoError as e:
                if e.status == 404:
                    raise ParentUnavailableError(f"parent {actual_parent}: it does not exist") from None
                raise
            if p.get("plan") != PLAN_WORKFLOW or not p.get("workflow"):
                raise ParentUnavailableError(f"parent {actual_parent}: it is not a workflow")
            if finished(p):
                raise ParentUnavailableError(f"parent {actual_parent}: it has finished")
        return 0, actual_parent

    async def _drive(self, id: str, name: str, input: Any, parent: str | None, token: int) -> Any:
        """Runs workflow id's function here, holding its drive lease (ADR
        0059): token is the lease set with its start, or 0 to claim it now.
        What it leaves: nothing when it finished (the lease ends with it), no
        lease when it waits (suspend mode: what it waits for drives it on),
        an expired lease when it stopped otherwise (a tick takes it up)."""
        rt = self._rt
        remote = self.backend if isinstance(self.backend, HttpBackend) else None
        claim: dict[str, Any] | None = None
        if remote is not None:
            info = await self.backend.get(id, input=True)
            if finished(info):
                return _done(id, info)
            claim = await remote.claim(self._supported_workflows(), id, 0 if self.suspend else 60_000)
            if claim is None:
                raise _DrivenElsewhere(f"workflow {id} is driven by another process")
        if rt is not None and token == 0:
            info = await self.backend.get(id)
            if finished(info):
                return _done(id, info)
            w = _workflow_of(info.get("input"))
            if w is not None and self._fn_for(name, w[1]) is None:
                raise _NotHere(name, w[1])  # not claimed: another process's
            token = new_token()
            if not await rt.claim_drive(id, token):
                raise _DrivenElsewhere(f"workflow {id} is driven by another process")
        if rt is not None:
            rt.driving(True)
        try:
            return await self._run_as(name, input, id, parent)
        except BaseException as e:
            if (self._draining or self._closing) and remote is not None and claim is not None:
                await remote.release(claim, True)
                claim = None
                raise StoppedError(f"workflow {id}: not driven here any more") from e
            if is_workflow_retry(e):
                if remote is not None:
                    await remote.release(claim, True)
                    claim = None
                    self._logger.warning("kairo: workflow %s: retrying after a transient error: %s", id, e)
                    if self.suspend:
                        raise Suspended(f"workflow {id}: suspended after a transient error") from e
                    raise StoppedError(f"workflow {id}: stopped for a transient error") from e
                retry_at = self._now() + rt.lease
                try:
                    retry_at = await rt.retry_drive(id, token)
                except Exception as ee:
                    self._logger.warning("kairo: workflow %s: handing it over for retry: %s", id, ee)
                self._logger.warning("kairo: workflow %s: retrying after a transient error at %s: %s", id, retry_at, e)
                if self.suspend:
                    raise Suspended(f"workflow {id}: suspended after a transient error") from e
                raise
            if rt is not None:
                try:
                    await rt.end_drive(id, token, not isinstance(e, Suspended))
                except Exception as ee:
                    self._logger.warning("kairo: workflow %s: ending its drive lease: %s", id, ee)
            if remote is not None and claim is not None:
                await remote.release(claim, isinstance(e, StoppedError))
                claim = None
            raise
        finally:
            if rt is not None:
                rt.driving(False)
            if remote is not None and claim is not None:
                await remote.release(claim)

    def _drive_background(self, id: str, name: str, input: Any, parent: str | None, token: int) -> None:
        """Wait mode: drives workflow id in the background, unless this process drives it already."""
        if id in self._owned or self._closing:
            return
        self._owned.add(id)

        async def go() -> None:
            try:
                await self._drive(id, name, input, parent, token)
            except (_DrivenElsewhere, StoppedError, Cancelled, _NotHere):
                pass  # stopped, cancelled, driven elsewhere, or of a version not here: nothing to do here
            except Exception as e:
                if not is_workflow_retry(e):
                    self._logger.warning("kairo: workflow %s: %s", id, e)
            finally:
                self._owned.discard(id)

        fut = asyncio.ensure_future(go())
        self._drives.add(fut)
        fut.add_done_callback(self._drives.discard)

    async def _lost_drive(self, lease: LeaseRow) -> None:
        """Takes up workflow lease.run, whose driver stopped: its drive lease expired (ADR 0059)."""
        rt = self._rt
        assert rt is not None
        try:
            info: dict[str, Any] | None = await self.backend.get(lease.run)
        except KairoError as e:
            if e.status != 404:
                raise
            info = None
        if info is None or info.get("plan") != PLAN_WORKFLOW or finished(info):
            # Nothing left to drive.
            await rt.end_drive(lease.run, lease.attempt, False, lease.owner)
            return
        w = _workflow_of(info.get("input"))
        if w is None:
            return
        if _externally_attached(info.get("input")):
            # An external re-submission is this workflow's replay reach.
            await rt.end_drive(lease.run, lease.attempt, False, lease.owner)
            return
        if self._fn_for(w[0], w[1]) is None:
            raise _NotHere(w[0], w[1])  # another process's
        if self.suspend:
            self._redrive(lease.run)
            return
        self._drive_background(lease.run, w[0], w[2], info.get("parent"), 0)

    async def signal(self, id: str, name: str, payload: Any = None) -> None:
        """Sends a signal to the first wait for it in workflow id that has not
        received one. A wait the workflow has not reached yet receives it when
        it does (ADR 0059, 0071): signals of one name go to its waits in order.
        LookupError: no such workflow."""
        await self.send_signal(id, name, payload)

    async def send_signal(
        self,
        id: str,
        name: str,
        payload: Any = None,
        *,
        only_if_waiting: bool = False,
        signal_id: str | None = None,
    ) -> SignalReceipt:
        """Atomically sends a signal and reports which wait accepted it.
        only_if_waiting refuses to keep an early signal (ADR 0064, 0071)."""
        rt = self._rt
        if rt is None and not hasattr(self.backend, "apply_signal"):
            raise RuntimeError("kairod does not support durable signal receipts")
        if signal_id is None:
            signal_id = str(uuid.uuid4())
        elif not signal_id or len(signal_id.encode()) > 256:
            raise ValueError("signal_id must be non-empty and at most 256 bytes")
        await self._plan(PLAN_WAIT + name, _wait_root(name))
        n = 0
        receipt: SignalReceipt | None = None
        while True:
            run_id = _call_id(id, PLAN_WAIT + name, None, n)
            try:
                row = await self.backend.get(run_id)
            except KairoError as e:
                if e.status != 404:
                    raise
                row = None
            if row is None:
                # This no-op transaction is the linearization point for
                # "not present now". A wait created first accepts it here.
                applied = (
                    {"applied": await rt.signal_applied(run_id, name, payload), "early": False}
                    if rt is not None
                    else await self.backend.apply_signal(
                        run_id, name, payload, signal_id=signal_id, only_if_waiting=only_if_waiting
                    )
                )
                if applied.get("applied"):
                    receipt = SignalReceipt(run_id, n, bool(applied.get("early")))
                    break
                if only_if_waiting:
                    try:
                        workflow = await self.backend.get(id)
                    except KairoError as e:
                        if e.status == 404:
                            raise LookupError(f"no workflow {id}") from None
                        raise
                    if workflow.get("plan") != PLAN_WORKFLOW:
                        raise LookupError(f"no workflow {id}")
                    raise NotWaitingError(f"workflow {id} is not waiting for {name}")
                if not await self._signal_ahead(id, name, run_id, payload, signal_id):
                    continue  # a concurrent wait or signal made this number
                receipt = SignalReceipt(run_id, n, True)
                break
            if finished(row):
                early = _recorded_signal(row.get("output"), payload, signal_id, only_if_waiting)
                if early is not None:
                    receipt = SignalReceipt(run_id, n, early)
                    break
                n += 1
                continue
            applied = (
                {"applied": await rt.signal_applied(run_id, name, payload), "early": False}
                if rt is not None
                else await self.backend.apply_signal(
                    run_id, name, payload, signal_id=signal_id, only_if_waiting=only_if_waiting
                )
            )
            if applied.get("applied"):
                receipt = SignalReceipt(run_id, n, bool(applied.get("early")))
                break
            n += 1  # a concurrent signal, timeout or cancellation won
        if self.suspend:
            if self._rt is None:
                await self.backend.wait(receipt.run_id)
                await self.tick()
            else:
                await self._settle()
                await self._wake_up()
        assert receipt is not None
        return receipt

    async def _signal_ahead(self, id: str, name: str, run_id: str, payload: Any, signal_id: str) -> bool:
        """Makes wait run_id of workflow id, as the workflow will when it
        reaches it, with the signal applied in the same transaction."""
        try:
            wf = await self.backend.get(id)
        except KairoError as e:
            if e.status == 404:
                raise LookupError(f"no workflow {id}") from None
            raise
        if wf.get("plan") != PLAN_WORKFLOW:
            raise LookupError(f"no workflow {id}")
        if finished(wf):
            raise LookupError(f"workflow {id} has finished")
        kw = (
            {"then": [{"kind": "signal", "name": name, "data": payload}]}
            if self._rt is not None
            else {"initial_signal": {"name": name, "payload": payload, "signal_id": signal_id}}
        )
        r = await self.backend.run(PLAN_WAIT + name, {"in": None}, run_id=run_id, parent=id, **kw)
        return not r.get("existing")

    async def cancel(self, id: str) -> None:
        """Cancels workflow id: the calls it is waiting for are cancelled with it
        (suspend mode: it stops when it is driven next). Waits for the durable
        outcome; an already cancelled workflow is an idempotent success, while
        one already completed or failed raises RunSettledError (ADR 0061)."""
        # Stop this process's replay first, so a child cancellation cannot
        # make the function record a failure before the durable parent one.
        stop = self._stops_by_id.get(id)
        request = self._cancel_requests.get(id)
        if request is None and stop is not None:
            request = asyncio.get_running_loop().create_future()
            self._cancel_requests[id] = request
        if stop is not None:
            _set(stop)
        settled = False
        try:
            while True:
                await self._settle_descendants(id)
                try:
                    await self.backend.cancel(id)
                    break
                except ChildrenUnsettledError:
                    pass
            await self._settle_descendants(id)
            info = await self.backend.wait(id)
            settled = True
            if info.get("status") != "cancelled":
                raise RunSettledError(f"workflow {id} is {info.get('status')}")
        finally:
            if request is not None and not request.done():
                request.set_result(settled)

    async def resolve(self, call_id: str, output: Any = None) -> None:
        """Settles call call_id, stopped for review (blocked: a real step whose
        outcome was unknown) or with a real attempt out that cannot go on (its
        action is no longer real, ADR 0060), with the output it had: it did
        take effect. A normal review can also be settled over kairod."""
        await self._settle_call(call_id, output, None)

    async def resolve_failed(self, call_id: str, message: str = "resolved as failed") -> None:
        """Settles call call_id as resolve does, as not done: it failed with message."""
        await self._settle_call(call_id, None, message or "resolved as failed")

    async def _settle_call(self, call_id: str, output: Any, error: str | None) -> None:
        resolve = getattr(self.backend, "resolve", None)
        if resolve is None:
            raise RuntimeError("resolve is not supported by this backend")
        await resolve(call_id, output, error)
        if self.suspend:
            if isinstance(self.backend, HttpBackend):
                await self.tick()
            else:
                await self._settle()
                await self._wake_up()

    async def _run_as(self, name: str, input: Any, id: str, parent: str | None = None, up: Context | None = None) -> Any:
        """Drives workflow id here. parent: the workflow that made it, if any;
        up, its context when it runs inside it (a child workflow)."""
        if self._rt is not None and parent is not None:
            self._rt.reach(id, True)
        try:
            return await self._run_as_reached(name, input, id, parent, up)
        finally:
            if self._rt is not None and parent is not None:
                self._rt.reach(id, False)

    async def _run_child_as(self, name: str, input: Any, id: str, parent: str, up: Context) -> Any:
        if not isinstance(self.backend, HttpBackend):
            return await self._run_as(name, input, id, parent, up)
        current = self._current_version(name)
        if current is None:
            raise KeyError(f"no workflow {name}")
        await self._plan_workflow()
        await self.backend.run(
            PLAN_WORKFLOW,
            _workflow_input(name, current, input),
            run_id=id,
            vars={"started_at": self._now()},
            parent=parent,
            drive=new_token(),
        )
        return await self._drive(id, name, input, parent, 0)

    async def _run_as_reached(self, name: str, input: Any, id: str, parent: str | None = None, up: Context | None = None) -> Any:
        current = self._current_version(name)
        if current is None:
            raise KeyError(f"no workflow {name}")
        await self._plan_workflow()
        # A new run starts with the current version; one that exists goes on
        # with the version it started with (ADR 0060).
        started = await self.backend.run(
            PLAN_WORKFLOW,
            _workflow_input(name, current, input),
            run_id=id,
            vars={"started_at": self._now()},
            parent=parent,
            workflow=name,
        )
        info = await self.backend.get(id, input=True)
        if finished(info):
            return _done(id, info)
        # A kairod before ADR 0060's GetInput reports no input: there, the
        # current version.
        w = _workflow_of(info.get("input")) if "input" in info else None
        version = w[1] if w is not None else ("" if "input" in info else current)
        fn = self._fn_for(name, version)
        if fn is None:
            raise _NotHere(name, version)
        # (The embedded runtime keeps the calls while the workflow runs, ADR 0054.)
        if started.get("existing") and not getattr(self.backend, "keeps_calls", False):
            at = int((info.get("vars") or {}).get("started_at") or 0)
            if at and self._now() - at > self._ttl * 1000:
                raise ResultLostError(f"workflow {id} started more than {self._ttl}s ago: its calls' records may be gone")
        # Set when the workflow stops being driven here: cancelled in kairo
        # (its run, or a workflow above it), or the process closes. Only when
        # cancelled are its calls cancelled; otherwise the workflow goes on
        # later.
        stop: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._stops.add(stop)
        self._drives_idle.clear()
        self._stops_by_id[id] = stop
        if self._closing:
            _set(stop)
        unlink: Callable[[], None] | None = None
        if up is not None:
            if up._stop.done():
                _set(stop)
            else:

                def follow(_: Any) -> None:
                    _set(stop)

                up._stop.add_done_callback(follow)
                unlink = lambda: up._stop.remove_done_callback(follow)  # noqa: E731
        me = {"cancelled": False}

        def cancelled() -> bool:
            return (
                me["cancelled"]
                or id in self._cancel_requests
                or self._tree_cancellations.get(id, 0) > 0
                or (up is not None and up.cancelled())
            )

        watcher: asyncio.Future[None] | None = None
        if not self.suspend:
            # The workflow's own run ends when it is cancelled: stop the calls.
            # (Suspended, a cancelled workflow stops when it is driven next.)
            async def watch() -> None:
                try:
                    r = await self.backend.wait(id)
                except Exception:
                    return
                if r.get("status") == "cancelled":
                    me["cancelled"] = True
                    _set(stop)

            watcher = asyncio.ensure_future(watch())
        self._driving_ids.add(id)
        ctx = Context(self, id, stop, cancelled)
        # A function that raises, at once or later, fails the workflow; one
        # that does not stop when the workflow is cancelled or the process
        # closes is left behind.
        body = asyncio.ensure_future(_call(fn, ctx, input))
        body.add_done_callback(_quiet)
        try:
            try:
                await asyncio.wait({body, stop}, return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                # The caller stopped waiting: not driven here any more.
                _set(stop)
            if not body.done():
                body.cancel()
                request = self._cancel_requests.get(id)
                if request is not None:
                    if await request:
                        return await self._workflow_outcome(id)
                    raise StoppedError(f"workflow {id}: not driven here any more")
                if cancelled():
                    # Cancelled with the workflow above it: so is its run.
                    if not me["cancelled"]:
                        try:
                            await self.backend.cancel(id)
                        except Exception:
                            pass
                    raise Cancelled(f"workflow {id} cancelled")
                raise StoppedError(f"workflow {id}: not driven here any more")
            failed = False
            cause: BaseException | None = None
            value: Any = None
            try:
                value = body.result()
            except Suspended:
                raise  # goes on later
            except _NotHere:
                # A child workflow whose version is not here: this one is left
                # to another process, not failed (ADR 0060).
                raise
            except EffectWeakenedError:
                raise
            except (Exception, asyncio.CancelledError) as e:
                if is_workflow_retry(e):
                    raise
                if stop.done():
                    request = self._cancel_requests.get(id)
                    if request is not None:
                        if await request:
                            return await self._workflow_outcome(id, e)
                        raise StoppedError(f"workflow {id}: not driven here any more") from e
                    # Stopped (the process closes, the caller stopped waiting)
                    # or cancelled, at the same time as the function ended:
                    # with that, not with the error it ended with (a child
                    # stopped by the same close raises StoppedError into it).
                    if cancelled():
                        if not me["cancelled"]:
                            try:
                                await self.backend.cancel(id)
                            except Exception:
                                pass
                        raise Cancelled(f"workflow {id} cancelled") from e
                    raise StoppedError(f"workflow {id}: not driven here any more") from e
                # (A CancelledError here is not this driver's: the function's own.)
                failed, cause = True, e
            if failed:
                assert cause is not None
                error = str(cause) or type(cause).__name__
                _set(stop)
                await self._finish_workflow(id, {"ok": False, "error": error})
                return await self._workflow_outcome(id, cause)
            _set(stop)
            await self._finish_workflow(id, {"ok": True, "value": value})
            return await self._workflow_outcome(id)
        finally:
            self._stops.discard(stop)
            if not self._stops:
                self._drives_idle.set()
            if self._stops_by_id.get(id) is stop:
                del self._stops_by_id[id]
            self._cancel_requests.pop(id, None)
            self._driving_ids.discard(id)
            if unlink is not None:
                unlink()
            if watcher is not None:
                watcher.cancel()
            _set(stop)

    async def _finish_workflow(self, id: str, payload: Any) -> None:
        """Atomically finishes after the last descendant scan (ADR 0065)."""
        while True:
            await self._settle_descendants(id)
            try:
                await self.backend.finish(id, payload)
                return
            except ChildrenUnsettledError:
                pass

    async def _workflow_outcome(
        self,
        id: str,
        cause: BaseException | None = None,
    ) -> Any:
        """Returns the durable outcome, even when cancellation beat the local
        workflow function (ADR 0061)."""
        try:
            info = await self.backend.wait(id)
        except asyncio.CancelledError:
            raise StoppedError(f"workflow {id}: not driven here any more") from None
        return _done(id, info, cause)

    async def _settle_descendants(self, id: str) -> None:
        """Cancels unfinished descendants deepest first before their parent settles."""
        while True:
            rows: list[dict[str, Any]] = []

            async def walk(parent: str) -> None:
                for child in await self.backend.children(parent):
                    await walk(child["run_id"])
                    rows.append(child)

            await walk(id)
            marked = [row["run_id"] for row in rows if not finished(row)]
            for child in marked:
                self._tree_cancellations[child] = self._tree_cancellations.get(child, 0) + 1
                stop = self._stops_by_id.get(child)
                if stop is not None:
                    _set(stop)
            unfinished = 0
            try:
                for row in rows:
                    if finished(row):
                        continue
                    unfinished += 1
                    try:
                        await self.backend.cancel(row["run_id"])
                    except ChildrenUnsettledError:
                        continue
                    await self.backend.wait(row["run_id"])
            finally:
                for child in marked:
                    n = self._tree_cancellations.get(child, 1) - 1
                    if n == 0:
                        self._tree_cancellations.pop(child, None)
                    else:
                        self._tree_cancellations[child] = n
            if not unfinished:
                return

    async def _call_run(self, plan: str, root: dict[str, Any] | None, input: Any, run_id: str,
                        parent: str | None = None, destination: str = "") -> Any:
        if self._rt is not None:
            self._rt.reach(run_id, True)
        try:
            return await self._call_run_reached(plan, root, input, run_id, parent, destination)
        finally:
            if self._rt is not None:
                self._rt.reach(run_id, False)

    async def _call_run_reached(self, plan: str, root: dict[str, Any] | None, input: Any, run_id: str,
                                parent: str | None = None, destination: str = "") -> Any:
        if self._closing:
            raise StoppedError(f"call {run_id}: stopped before it started")
        if self._draining:
            # Draining: no new call starts here (its step would be
            # dispatched); one already made is waited for as before.
            try:
                await self.backend.get(run_id)
            except KairoError as e:
                if e.status != 404:
                    raise
                raise StoppedError(f"call {run_id}: stopped (draining)") from None
        if root is not None:
            await self._plan(plan, root)
        # Kept and removed with the workflow that makes it (ADR 0054).
        run_input = {"in": input, **({"destination": destination} if destination else {})}
        await self.backend.run(plan, run_input, run_id=run_id, parent=parent)
        if self._rt is not None:
            await self._rt.resume_reached(run_id)
        if self.suspend:
            # Whatever this process can do for the call is done once it is
            # idle; a call still going then waits for a timer or a signal.
            if isinstance(self.backend, HttpBackend):
                r = await self.backend.wait_quiescent(run_id)
            else:
                await self.backend.idle()
                r = await self.backend.get(run_id)
            if not finished(r) and r.get("status") != "blocked":
                raise Suspended(f"call {run_id} waits")
        else:
            r = await self.backend.wait(run_id)
        if r.get("trimmed"):
            raise ResultLostError(f"call {run_id} finished, but kairo no longer keeps its result")
        if r.get("status") != "completed":
            raise _call_error(run_id, plan, str(r.get("status")), r.get("error") or "")
        return r.get("output")


def tick_handler(make: Callable[[], Awaitable[Kairo]]) -> Callable[..., Awaitable[dict[str, Any]]]:
    """A function's entry for a scheduler that calls it, not over HTTP (e.g.
    AWS Lambda from EventBridge Scheduler, ADR 0053): each call makes a
    Kairo (started, suspend mode), ticks, and closes it. Returns when
    something is next to do. (Lambda: ``asyncio.run(handler())``.)"""

    async def handler(*_: Any) -> dict[str, Any]:
        k = await make()
        try:
            return {"next": await k.tick()}
        finally:
            await k.close()

    return handler


async def _await(a: Awaitable[Any]) -> Any:
    return await a


def _from_wire(w: Any) -> Result:
    """A result as actions over HTTP(S) send it (ADR 0052)."""
    if not isinstance(w, dict):
        raise TypeError("not a result")
    weight = w.get("weight", 0)
    if not isinstance(weight, int) or isinstance(weight, bool) or weight < 0:
        weight = 0
    retryable = bool(w.get("retryable"))
    retry_after = w.get("retry_after_ms", 0)
    if not retryable or not isinstance(retry_after, int) or isinstance(retry_after, bool) or retry_after < 0:
        retry_after = 0
    if w.get("error") is not None:
        return Result(error=str(w["error"]), retryable=retryable, weight=weight, retry_after_ms=retry_after,
                      rate_limited=retry_after > 0, error_type=w.get("error_type") or "")
    if w.get("wait"):
        return Result(wait={"until": int(w["wait"]["until"]), "output": w["wait"].get("output")}, weight=weight)
    return Result(output=w.get("output"), weight=weight)


def _duration_ms(d: str | None) -> int | None:
    """Milliseconds of a duration such as "300ms", "5s", "5m", "1h"."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h)", d or "")
    if not m:
        return None
    return int(float(m.group(1)) * {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}[m.group(2)])


def _done(id: str, info: dict[str, Any], cause: BaseException | None = None) -> Any:
    if info.get("trimmed"):
        raise ResultLostError(f"workflow {id} finished, but kairo no longer keeps its result")
    if info.get("status") == "cancelled":
        raise Cancelled(f"workflow {id} cancelled")
    out = (info.get("output") or {}).get("payload")
    if not out:
        error = WorkflowError(id, f"{info.get('status')}: {info.get('error', '')}")
        raise error
    if not out.get("ok"):
        message = str(out.get("error"))
        error = WorkflowError(id, message)
        if cause is not None and (str(cause) or type(cause).__name__) == message:
            raise error from cause
        raise error
    return out.get("value")


async def _call(fn: Callable[..., Any], ctx: Context, input: Any) -> Any:
    """Runs a workflow function: a raise at once fails it as a raise later does."""
    out = fn(ctx, input)
    if inspect.isawaitable(out):
        out = await out
    return out


def _quiet(f: asyncio.Future[Any]) -> None:
    """A left-behind function's outcome is not looked at: not a warning either."""
    if not f.cancelled():
        f.exception()


def _stopped_waiting(action: str) -> Result:
    return Result(error=f"step of {action} stopped waiting for a slot", retryable=True, error_type="cancelled")


class _Slots:
    """Slots taken and given back, on the event loop; a slot given back goes
    to the step waiting longest (no polling)."""

    def __init__(self, size: int) -> None:
        self.size = size
        self._free = size
        self._queue: collections.deque[asyncio.Future[None]] = collections.deque()

    async def take(self, stop: asyncio.Future[None]) -> None:
        """Takes a slot, waiting for one; raises _StepStopped when stop is set first."""
        if self._free > 0 and not self._queue:
            self._free -= 1
            return
        go: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._queue.append(go)
        try:
            await asyncio.wait({go, stop}, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            self._drop(go)
            raise
        if not go.done():
            self._drop(go)
            raise _StepStopped

    def _drop(self, go: asyncio.Future[None]) -> None:
        """A waiter that gave up: out of the queue, or its slot given on."""
        if go.done():
            self.give()
        else:
            go.cancel()
            try:
                self._queue.remove(go)
            except ValueError:
                pass

    def give(self) -> None:
        while self._queue:
            go = self._queue.popleft()
            if not go.done():
                go.set_result(None)
                return
        self._free += 1


def _stricter(a: float | int | None, b: float | int | None) -> Any:
    if not a or a <= 0:
        return b if b and b > 0 else None
    if not b or b <= 0:
        return a
    return min(a, b)


class _Limiter:
    """Process-local limits of one runtime destination (ADR 0059, 0077)."""

    def __init__(self) -> None:
        self.declared = DestinationLimits()
        self.active = 0
        self.concurrency = 0
        self.every = 0.0
        self.next = 0.0
        self.weight_rate = 0.0
        self.weight_capacity = 0.0
        self.weight = 0.0
        self.weight_at = 0.0
        self.pause_until = 0.0
        self.changed: asyncio.Future[None] | None = None

    def declare(self, v: DestinationLimits) -> None:
        self.declared = DestinationLimits(
            _stricter(self.declared.concurrency, v.concurrency),
            _stricter(self.declared.requests_per_minute, v.requests_per_minute),
            _stricter(self.declared.weight_per_minute, v.weight_per_minute),
        )

    def configure(self, v: DestinationLimits) -> None:
        v = DestinationLimits(
            _stricter(self.declared.concurrency, v.concurrency),
            _stricter(self.declared.requests_per_minute, v.requests_per_minute),
            _stricter(self.declared.weight_per_minute, v.weight_per_minute),
        )
        now = time.time()
        self._refill(now)
        self.concurrency = max(0, int(v.concurrency or 0))
        self.every = 60 / v.requests_per_minute if v.requests_per_minute and v.requests_per_minute > 0 else 0.0
        if not self.every:
            self.next = 0.0
        if not v.weight_per_minute or v.weight_per_minute <= 0:
            self.weight_rate = self.weight_capacity = self.weight = 0.0
            self.weight_at = now
            return
        capacity = max(1.0, v.weight_per_minute / 6)
        self.weight = capacity if not self.weight_rate else min(self.weight, capacity)
        self.weight_rate = v.weight_per_minute / 60
        self.weight_capacity = capacity
        self.weight_at = now

    def _refill(self, now: float) -> None:
        if self.weight_rate <= 0:
            return
        if not self.weight_at:
            self.weight_at = now
        self.weight = min(self.weight_capacity, self.weight + max(0.0, now - self.weight_at) * self.weight_rate)
        self.weight_at = now

    def _wake(self) -> None:
        if self.changed is not None and not self.changed.done():
            self.changed.set_result(None)
        self.changed = None

    async def admit(self, limits: DestinationLimits, stop: asyncio.Future[None]) -> None:
        while True:
            self.configure(limits)
            now = time.time()
            self._refill(now)
            at = max(now, self.pause_until, self.next)
            if self.weight_rate > 0 and self.weight < 0:
                at = max(at, now + (-self.weight / self.weight_rate))
            full = self.concurrency > 0 and self.active >= self.concurrency
            if not full and at <= now:
                self.active += 1
                if self.every > 0:
                    self.next = now + self.every
                return
            if self.changed is None:
                self.changed = asyncio.get_running_loop().create_future()
            done, _ = await asyncio.wait(
                {self.changed, stop}, timeout=None if full else max(0.0, at - now), return_when=asyncio.FIRST_COMPLETED
            )
            if stop in done:
                raise _StepStopped

    def finish(self, weight: int, retry_after_ms: int) -> None:
        now = time.time()
        self._refill(now)
        if isinstance(weight, int) and weight > 0 and self.weight_rate > 0:
            self.weight -= weight
        if retry_after_ms > 0:
            self.pause_until = max(self.pause_until, now + retry_after_ms / 1000)
        if self.active > 0:
            self.active -= 1
        self._wake()

    def idle_after(self) -> float | None:
        now = time.time()
        self._refill(now)
        if self.active > 0:
            return None
        after = max(0.0, self.pause_until - now, self.next - now)
        if self.weight_rate > 0 and self.weight < self.weight_capacity:
            after = max(after, (self.weight_capacity - self.weight) / self.weight_rate)
        return after


class Context:
    """What a workflow function calls."""

    def __init__(
        self, k: Kairo, id: str, stop: asyncio.Future[None] | None = None, cancelled: Callable[[], bool] | None = None
    ) -> None:
        self.id = id
        self._k = k
        self._seen: dict[str, int] = {}
        # Set when the workflow stops being driven here (cancelled, or the process closes).
        self._stop = stop if stop is not None else asyncio.get_running_loop().create_future()
        # Whether the workflow (or one above it) was cancelled in kairo: only then are its calls cancelled.
        self.cancelled: Callable[[], bool] = cancelled or (lambda: False)

    def _next(self, kind: str, input: Any) -> str:
        key = kind + "\0" + _canonical(input)
        n = self._seen.get(key, 0)
        self._seen[key] = n + 1
        return _call_id(self.id, kind, input, n)

    async def _run(self, plan: str, root: dict[str, Any] | None, input: Any, run_id: str, destination: str = "") -> Any:
        if self._stop.done():
            raise StoppedError(f"call {run_id}: stopped before it started")
        try:
            return await self._k._call_run(plan, root, input, run_id, self.id, destination)
        except asyncio.CancelledError:
            # The workflow was cancelled in kairo: so is the call. If not, the
            # workflow is only no longer driven here: the call goes on in
            # kairo, and the workflow resumes it later.
            if self.cancelled():
                try:
                    await self._k.backend.cancel(run_id)
                except Exception:
                    pass
            raise

    async def call(self, action: str, input: Any = None, *, destination: str = "") -> Any:
        """Runs action with input (once, however often the workflow runs again)."""
        kind = PLAN_CALL + action + (("\0destination:" + destination) if destination else "")
        return await self._run(PLAN_CALL + action, None, input, self._next(kind, input), destination)

    async def wait_for(self, name: str, timeout: float | None = None) -> Any:
        """Waits for signal name (see Kairo.signal); returns its payload. With
        timeout (seconds, as sleep), raises TimedOutError if no signal comes
        in time; the timeout is kept in kairo (ADR 0059)."""
        # The timeout is the plan's, not the call's: the id is the same with
        # or without one, so a signal sent ahead finds it (ADR 0059).
        run_id = self._next(PLAN_WAIT + name, None)
        ms = round((timeout or 0) * 1000)
        plan = f"{PLAN_WAIT}{name}@{ms}" if ms > 0 else PLAN_WAIT + name
        out = await self._run(plan, _wait_root(name, ms), None, run_id)
        if out.get("timed_out"):
            raise TimedOutError(f"wait for {name} timed out")
        return out["payload"]

    async def sleep(self, seconds: float) -> None:
        """Waits, in kairo (the process may stop meanwhile)."""
        await self.call(SLEEP, {"ms": int(seconds * 1000)})

    async def now(self) -> int:
        """The time (unix ms), the same each time the workflow runs again."""
        return await self.call(NOW)

    async def random(self) -> float:
        """A random number in [0, 1), the same each time the workflow runs again."""
        return await self.call(RANDOM)

    async def workflow(self, name: str, input: Any = None) -> Any:
        """Runs workflow name as a child of this one."""
        return await self._k._run_child_as(name, input, self._next("kairo.workflow/" + name, input), self.id, self)
