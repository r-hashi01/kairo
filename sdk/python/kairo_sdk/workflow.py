"""Workflows written as code (ADR 0049).

A workflow is an ``async`` function that runs in this process; each call it
makes (an action, a wait, a clock read) is a run of its own in kairo, whose
id comes from the call: running the function again with the same workflow
id returns the calls that finished, and runs nothing twice.

The calls run on kairod (HttpBackend, the default) or on the runtime
embedded in this process with a database (EmbeddedBackend, ADR 0051). With
the embedded runtime, mode="suspend" serves hosts that do not stay up
(serverless): a workflow goes as far as it can now, and tick() and signal()
drive it on.

With the embedded runtime, the process that drives a workflow holds a
lease on it (ADR 0059): if the process stops, another one that has the
workflow takes it up once the lease expires. The same behaviour as the
TypeScript and Go SDKs (sdk/ts/src/workflow.ts, sdk/go).
"""

from __future__ import annotations

import asyncio
import collections
import datetime
import hashlib
import hmac
import inspect
import json
import random
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .backend import Backend, HttpBackend
from .client import KairoError, finished
from .embedded import Embedded, new_token
from .http import SignatureError, check_url, post, sign, verify
from .protocol import Result
from .store import LeaseRow
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


class Suspended(Exception):
    """The workflow waits (a timer, a signal): tick() or signal() drives it on."""


class StoppedError(Exception):
    """The caller stopped waiting (its task was cancelled, or it timed out),
    or the process is closing. The workflow is not cancelled: it goes on
    (ADR 0059)."""


class TimedOutError(Exception):
    """A wait's timeout came before its signal (wait_for's timeout, ADR 0059)."""


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


def _canonical(v: Any) -> str:
    """JSON with object keys sorted: equal values, equal text."""
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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
        backend: Backend | None = None,
        mode: str = "wait",
        secret: str | None = None,
        callback_url: str | None = None,
        ca: str | None = None,
        allow_insecure: bool = False,
        wait_until: Callable[[Awaitable[Any]], None] | None = None,
        tick_secret: str | None = None,
        wake: Callable[[int], Awaitable[None]] | None = None,
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
        instead of polling."""
        self.backend: Any = backend or HttpBackend(url, worker=worker, token=token, tenant=tenant, concurrency=concurrency)
        self._ttl = idempotency_ttl
        self._concurrency = concurrency
        self._actions: dict[str, _Action] = {}
        self._workflows: dict[str, Callable[..., Awaitable[Any]]] = {}
        self._planned: set[str] = set()
        # The stops of the workflows driven here: set when the process closes.
        self._stops: set[asyncio.Future[None]] = set()
        # Closing: calls interrupted now go on in kairo, to be resumed (not cancelled).
        self._closing = False
        if mode not in ("wait", "suspend"):
            raise ValueError(f"mode {mode}: wait or suspend")
        self.suspend = mode == "suspend"
        if self.suspend and not hasattr(self.backend, "idle"):
            raise ValueError("suspend mode needs the embedded backend")
        # Suspend mode: workflows being driven on here, and those to drive again after.
        self._driving_ids: set[str] = set()
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
        # The embedded runtime, when that is the backend (drive leases, ADR 0059).
        rt = getattr(self.backend, "runtime", None)
        self._rt: Embedded | None = rt if isinstance(rt, Embedded) else None
        # Wait mode: workflows driven in the background here (by id), and their drives.
        self._owned: set[str] = set()
        self._drives: set[asyncio.Future[Any]] = set()
        # Ends with the process (close): what waits in it stops.
        self._life: asyncio.Future[None] | None = None
        # Wait mode: the sweep, once a lease period, of what stopped processes left (ADR 0059).
        self._sweeper: asyncio.Future[None] | None = None
        # Limits (ADR 0059): steps at once in this process (embedded), and by destination.
        self._slots: _Slots | None = None
        self._limits: dict[str, _Limiter] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

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
    ):
        """Declares an action: "real" (the default) acts outside and never runs
        twice; "unprotected" may run again. The handler takes the input and a
        TaskContext; it may be a plain or an async function.

        destination: the rate-limit key (default: the action). limit: at most
        this many of its steps run in this process at once; rate: at most
        this many start in this process a minute, spaced evenly (ADR 0059).
        Actions of one destination share limit and rate: the strictest.

        url (ADR 0052): the action's steps are called over HTTP(S) there, and
        the handler runs there (asgi_app). async_: where the URL serves it,
        it answers at once (202) and sends the outcome to the callback."""
        if name.startswith("kairo."):
            raise ValueError(f'action {name}: names beginning with "kairo." are kairo\'s')
        if url:
            check_url(url, self._allow_insecure if allow_insecure is None else allow_insecure)

        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._actions[name] = _Action(fn, effect, timeout, destination, url, async_, limit, rate)
            return fn

        return register

    def workflow(self, name: str):
        """Declares a workflow: ``async def fn(ctx, input)``."""

        def register(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
            self._workflows[name] = fn
            return fn

        return register

    async def start(self) -> None:
        """Registers the actions and their plans, and starts running the actions' steps."""
        self._loop = asyncio.get_running_loop()
        self._life_future()
        specs: list[dict[str, Any]] = []
        for name, a in self._actions.items():
            s: dict[str, Any] = {"action": name, "effect": a.effect}
            if a.timeout:
                s["timeout"] = a.timeout
            if a.destination:
                s["destination"] = a.destination
            specs.append(s)
        for name in BUILTIN:
            specs.append({"action": name, "effect": "unprotected"})
        if any(a.url for a in self._actions.values()):
            # Actions over HTTP(S) (ADR 0052) take their outcomes on the callback.
            if not self._secret or not self._callback_url:
                raise ValueError("actions with a url need secret and callback_url")
            check_url(self._callback_url, self._allow_insecure)
            if not hasattr(self.backend, "complete"):
                raise ValueError("actions with a url need the embedded backend")
        for name, a in self._actions.items():
            if not a.limit and not a.rate:
                continue
            # Actions of one destination share its limits: the strictest.
            key = a.destination or name
            self._limits.setdefault(key, _Limiter()).tighten(a.limit or 0, a.rate or 0)
        rt = self._rt
        # kairod's worker limits its own tasks.
        if rt is not None and self._concurrency:
            self._slots = _Slots(self._concurrency)
        if rt is not None and (self._slots or self._limits):
            # Steps wait for their slots on the event loop, before their thread.
            rt.admit = self._admit_step
        await self.backend.start(specs, self._serve)
        for s in specs:
            await self._plan(PLAN_CALL + s["action"], {"kind": "step", "id": "call", "action": s["action"], "input": {"in": "$input.in"}})
        await self._plan_workflow()
        if rt is not None:
            rt.lost_drive = self._lost_drive
            rt.plan_for = _wait_plan
            rt.lease_parents = self.suspend
        if self.suspend:
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
                await rt.tick()
                await rt.recheck()
            except Exception:
                pass  # next time
            if self._closing:
                return
            await asyncio.wait({life}, timeout=rt.lease / 1000)

    def _life_future(self) -> asyncio.Future[None]:
        if self._life is None:
            self._life = asyncio.get_running_loop().create_future()
            if self._closing:
                _set(self._life)
        return self._life

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

    async def tick(self) -> int | None:
        """Suspend mode: takes up due timers and steps whose process stopped,
        drives on the workflows whose calls settle, and returns once that is
        done (call it from a scheduler). Returns when something is next to
        do (unix ms), or None."""
        await self.backend.tick()
        await self._settle()
        return await self._wake_up()

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
        if id in self._driving_ids:
            self._again.add(id)
            return

        async def drive() -> None:
            while True:
                self._again.discard(id)
                try:
                    info = await self.backend.get(id)
                except KairoError:
                    return
                inp = info.get("input") or {}
                if info.get("plan") != PLAN_WORKFLOW or finished(info) or not inp.get("workflow"):
                    return
                try:
                    await self._drive(id, inp["workflow"], inp.get("input"), info.get("parent"), 0)
                except Exception:
                    pass  # suspended again, failed (recorded), cancelled, or driven elsewhere
                if id not in self._again:
                    return

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

    async def _admit(self, action: str, ctx: TaskContext) -> Callable[[], None]:
        """Waits, on the event loop, for a step of action to be let run (ADR
        0059): its destination's slot and turn, then the process's slot.
        Returns what gives the slots back (on the event loop). Raises
        _StepStopped if the step is cancelled first."""
        a = self._actions.get(action)
        lim = self._limits.get(a.destination or action) if a is not None and action not in BUILTIN else None
        slots = self._slots if a is not None and action not in BUILTIN else None
        held: list[_Slots] = []

        def release() -> None:
            while held:
                held.pop().give()

        if lim is None and slots is None:
            return release
        stop: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        loop = asyncio.get_running_loop()

        def stopped() -> None:
            try:
                loop.call_soon_threadsafe(_set, stop)
            except RuntimeError:
                pass  # the loop is closed: nothing waits on it any more

        off = ctx.cancelled.on_set(stopped) if hasattr(ctx.cancelled, "on_set") else (lambda: None)
        try:
            if lim is not None and lim.slots is not None:
                await lim.slots.take(stop)
                held.append(lim.slots)
            if lim is not None and lim.every > 0:
                now = time.time()
                at = max(lim.next, now)
                lim.next = at + lim.every
                if at > now:
                    done, _ = await asyncio.wait({stop}, timeout=at - now)
                    if done:
                        raise _StepStopped
            if slots is not None:
                await slots.take(stop)
                held.append(slots)
            return release
        except BaseException:
            release()
            raise
        finally:
            off()

    async def _admit_step(self, task: Task, ctx: TaskContext) -> Callable[[], None] | Result:
        """The embedded runtime's admission of a dispatched step (ADR 0059)."""
        try:
            return await self._admit(task.action, ctx)
        except _StepStopped:
            return _stopped_waiting(task.action)

    def _serve(self, task: Task, ctx: TaskContext) -> Result:
        input = (task.input or {}).get("in") if isinstance(task.input, dict) else None
        if task.action == NOW:
            return Result(output=int(time.time() * 1000))
        if task.action == RANDOM:
            return Result(output=random.random())
        if task.action == SLEEP:
            # Waits in kairo, not here (ADR 0045).
            return Result(wait={"until": int(time.time() * 1000 + float(input["ms"])), "output": None})
        a = self._actions.get(task.action)
        if a is None:
            return Result(error=f"no action {task.action} here")
        # On kairod's worker (a thread of its own), over a limit, the step
        # waits here for a slot (ADR 0059); the embedded runtime admits it
        # before its thread.
        release: Callable[[], None] | None = None
        if self._rt is None and self._limits and self._loop is not None:
            try:
                release = asyncio.run_coroutine_threadsafe(self._admit(task.action, ctx), self._loop).result()
            except _StepStopped:
                return _stopped_waiting(task.action)
        try:
            if a.url:
                return self._call_remote(task, a, input)
            out = a.handler(input, ctx)
            if inspect.isawaitable(out):
                out = asyncio.run(_await(out))
            return Result(output=out)
        finally:
            if release is not None:
                self._loop.call_soon_threadsafe(release)  # type: ignore[union-attr]

    def _call_remote(self, task: Task, a: _Action, input: Any) -> Result:
        """Calls an action over HTTP(S) (ADR 0052); on the step's thread."""
        assert a.url and self._secret
        body = json.dumps(
            {
                "run_id": task.run_id,
                "step_id": task.step_id,
                "act": task.act,
                "attempt": task.attempt,
                "action": task.action,
                "input": input,
                "idempotency_key": task.idempotency_key,
                "callback": self._callback_url,
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
            if scope["path"].endswith("/tick"):
                status, out = await self._handle_tick(scope["method"], headers.get("authorization"))
            else:
                status, out = await self._handle(scope["method"], scope["path"], headers.get("kairo-signature"), body)
            data = json.dumps(out).encode()
            await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", b"application/json")]})
            await send({"type": "http.response.body", "body": data})

        return app

    async def _handle_tick(self, method: str, authorization: str | None) -> tuple[int, Any]:
        if not self._tick_secret or not self.suspend:
            return 404, {"error": "no such path"}
        if method not in ("GET", "POST"):
            return 405, {"error": "GET or POST"}
        if not hmac.compare_digest((authorization or "").encode(), f"Bearer {self._tick_secret}".encode()):
            return 401, {"error": "bad token"}
        return 200, {"next": await self.tick()}

    async def _handle(self, method: str, path: str, signature: str | None, body: bytes) -> tuple[int, Any]:
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

        async def run() -> dict[str, Any]:
            try:
                out = await asyncio.to_thread(a.handler, m.get("input"), TaskContext(lambda _d: None))
                if inspect.isawaitable(out):
                    out = await out
                return {"output": out}
            except Exception as e:
                return {"error": f"{type(e).__name__}: {e}", "error_type": type(e).__name__}

        if not a.async_:
            return 200, await run()

        # Answer now; run after, and send the outcome to the callback.
        async def work() -> None:
            result = await run()
            cb = json.dumps({"run_id": m["run_id"], "act": m["act"], "attempt": m["attempt"], "result": result}).encode()
            try:
                check_url(m["callback"], self._allow_insecure)
                await asyncio.to_thread(post, m["callback"], cb, ca=self._ca, headers={"Kairo-Signature": sign(self._secret or "", cb)})
            except (OSError, ValueError):
                pass  # a lost callback: the lease expires and the step is taken up

        fut = asyncio.ensure_future(work())
        self._background.add(fut)
        fut.add_done_callback(self._background.discard)
        if self._wait_until is not None:
            self._wait_until(fut)
        return 202, {"accepted": True}

    async def run(self, name: str, input: Any = None, *, id: str | None = None, meta: dict[str, Any] | None = None) -> Any:
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
            token = await self._begin(name, input, id, meta)
            if self.suspend:
                try:
                    return await self._drive(id, name, input, None, token)
                except _DrivenElsewhere as e:
                    raise Suspended(str(e)) from None
            # kairod: driven here, as the caller waits.
            if self._rt is None:
                return await self._drive(id, name, input, None, 0)
            self._drive_background(id, name, input, None, token)
            return await self.result(id)
        except asyncio.CancelledError:
            if self.suspend:
                raise
            raise StoppedError(f"workflow {id}: stopped waiting for it") from None
        finally:
            await self._wake_up()

    async def submit(self, name: str, input: Any = None, *, id: str | None = None, meta: dict[str, Any] | None = None) -> str:
        """Starts workflow name and returns its id once the start is recorded,
        without waiting for it (ADR 0059): result(id) gives its result, from
        any process. In wait mode it is driven in the background; in suspend
        mode it is driven here until it waits (what it comes to is recorded:
        nothing is raised). The id is an idempotency key, as in run."""
        id = id or str(uuid.uuid4())
        token = await self._begin(name, input, id, meta)
        if not self.suspend:
            self._drive_background(id, name, input, None, token)
            return id
        try:
            await self._drive(id, name, input, None, token)
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
        "created_at" and "updated_at" (unix ms). The embedded runtime only."""
        if self._rt is None:
            raise RuntimeError("list needs the embedded backend")
        return await self._rt.list(workflow=workflow, status=status, since=_ms(since), until=_ms(until), after=after, limit=limit)

    async def _begin(self, name: str, input: Any, id: str, meta: dict[str, Any] | None) -> int:
        """Makes workflow name's run as id (or finds it). A new one is leased to
        this process to drive, in the same transaction: its token (0: it
        existed, or no leases here)."""
        if name not in self._workflows:
            raise KeyError(f"no workflow {name}")
        await self._plan_workflow()
        token = new_token() if self._rt is not None else 0
        started = await self.backend.run(
            PLAN_WORKFLOW,
            {"workflow": name, "input": input},
            run_id=id,
            vars={"started_at": int(time.time() * 1000)},
            workflow=name,
            meta=meta,
            drive=token,
        )
        return 0 if started.get("existing") else token

    async def _drive(self, id: str, name: str, input: Any, parent: str | None, token: int) -> Any:
        """Runs workflow id's function here, holding its drive lease (ADR
        0059): token is the lease set with its start, or 0 to claim it now.
        What it leaves: nothing when it finished (the lease ends with it), no
        lease when it waits (suspend mode: what it waits for drives it on),
        an expired lease when it stopped otherwise (a tick takes it up)."""
        rt = self._rt
        if rt is not None and token == 0:
            info = await self.backend.get(id)
            if finished(info):
                return _done(id, info)
            token = new_token()
            if not await rt.claim_drive(id, token):
                raise _DrivenElsewhere(f"workflow {id} is driven by another process")
        if rt is not None:
            rt.driving(True)
        try:
            return await self._run_as(name, input, id, parent)
        except BaseException as e:
            if rt is not None:
                try:
                    await rt.end_drive(id, token, not isinstance(e, Suspended))
                except Exception:
                    pass
            raise
        finally:
            if rt is not None:
                rt.driving(False)

    def _drive_background(self, id: str, name: str, input: Any, parent: str | None, token: int) -> None:
        """Wait mode: drives workflow id in the background, unless this process drives it already."""
        if id in self._owned or self._closing:
            return
        self._owned.add(id)

        async def go() -> None:
            try:
                await self._drive(id, name, input, parent, token)
            except Exception:
                # Its failure is recorded (result reports it); stopped,
                # cancelled or driven elsewhere: nothing to do here.
                pass
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
        inp = info.get("input") or {}
        if not inp.get("workflow"):
            return
        if inp["workflow"] not in self._workflows:
            raise KairoError(404, f"workflow {inp['workflow']} is not registered here")  # another process's
        if self.suspend:
            self._redrive(lease.run)
            return
        self._drive_background(lease.run, inp["workflow"], inp.get("input"), info.get("parent"), 0)

    async def signal(self, id: str, name: str, payload: Any = None) -> None:
        """Sends a signal to the first wait for it in workflow id that has not
        received one. With the embedded runtime, a wait the workflow has not
        reached yet receives it when it does (ADR 0059): signals of one name
        go to its waits in order. LookupError: no such workflow, or it has
        finished (kairod: it does not wait for the signal)."""
        # The waits' plan, which this process may not have needed yet.
        await self._plan(PLAN_WAIT + name, _wait_root(name))
        n = 0
        while True:
            run_id = _call_id(id, PLAN_WAIT + name, None, n)
            try:
                r = await self.backend.get(run_id)
            except KairoError as e:
                if e.status != 404:
                    raise
                if self._rt is None:
                    raise LookupError(f"workflow {id} does not wait for {name}") from None
                if await self._signal_ahead(id, name, run_id, payload):
                    break
                continue  # the workflow made the wait meanwhile: signal it
            if finished(r):
                n += 1
                continue
            await self.backend.signal(run_id, name, payload)
            break
        if self.suspend:
            await self._settle()
            await self._wake_up()

    async def _signal_ahead(self, id: str, name: str, run_id: str, payload: Any) -> bool:
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
        r = await self.backend.run(
            PLAN_WAIT + name, {"in": None}, run_id=run_id, parent=id, then=[{"kind": "signal", "name": name, "data": payload}]
        )
        return not r.get("existing")

    async def cancel(self, id: str) -> None:
        """Cancels workflow id: the calls it is waiting for are cancelled with it
        (suspend mode: it stops when it is driven next)."""
        await self.backend.cancel(id)

    async def _run_as(self, name: str, input: Any, id: str, parent: str | None = None, up: Context | None = None) -> Any:
        """Drives workflow id here. parent: the workflow that made it, if any;
        up, its context when it runs inside it (a child workflow)."""
        fn = self._workflows.get(name)
        if fn is None:
            raise KeyError(f"no workflow {name}")
        await self._plan_workflow()
        started = await self.backend.run(
            PLAN_WORKFLOW,
            {"workflow": name, "input": input},
            run_id=id,
            vars={"started_at": int(time.time() * 1000)},
            parent=parent,
            workflow=name,
        )
        info = await self.backend.get(id)
        if finished(info):
            return _done(id, info)
        # (The embedded runtime keeps the calls while the workflow runs, ADR 0054.)
        if started.get("existing") and not getattr(self.backend, "keeps_calls", False):
            at = int((info.get("vars") or {}).get("started_at") or 0)
            if at and time.time() * 1000 - at > self._ttl * 1000:
                raise ResultLostError(f"workflow {id} started more than {self._ttl}s ago: its calls' records may be gone")
        # Set when the workflow stops being driven here: cancelled in kairo
        # (its run, or a workflow above it), or the process closes. Only when
        # cancelled are its calls cancelled; otherwise the workflow goes on
        # later.
        stop: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._stops.add(stop)
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
            return me["cancelled"] or (up is not None and up.cancelled())

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
                if cancelled():
                    # Cancelled with the workflow above it: so is its run.
                    if not me["cancelled"]:
                        try:
                            await self.backend.cancel(id)
                        except Exception:
                            pass
                    raise Cancelled(f"workflow {id} cancelled")
                raise StoppedError(f"workflow {id}: not driven here any more")
            try:
                value = body.result()
            except Suspended:
                raise  # goes on later
            except (Exception, asyncio.CancelledError) as e:
                try:
                    await self.backend.signal(id, "done", {"ok": False, "error": str(e) or type(e).__name__})
                except Exception:
                    pass
                if isinstance(e, asyncio.CancelledError):
                    # Not this driver's: the function's own.
                    raise RuntimeError(f"workflow {id}: its function was cancelled") from e
                raise
            await self.backend.signal(id, "done", {"ok": True, "value": value})
            return value
        finally:
            self._stops.discard(stop)
            self._driving_ids.discard(id)
            if unlink is not None:
                unlink()
            if watcher is not None:
                watcher.cancel()
            _set(stop)

    async def _call_run(self, plan: str, root: dict[str, Any] | None, input: Any, run_id: str, parent: str | None = None) -> Any:
        if root is not None:
            await self._plan(plan, root)
        # Kept and removed with the workflow that makes it (ADR 0054).
        await self.backend.run(plan, {"in": input}, run_id=run_id, parent=parent)
        if self.suspend:
            # Whatever this process can do for the call is done once it is
            # idle; a call still going then waits for a timer or a signal.
            await self.backend.idle()
            r = await self.backend.get(run_id)
            if not finished(r) and r.get("status") != "blocked":
                raise Suspended(f"call {run_id} waits")
        else:
            r = await self.backend.wait(run_id)
        if r.get("trimmed"):
            raise ResultLostError(f"call {run_id} finished, but kairo no longer keeps its result")
        if r.get("status") != "completed":
            raise RuntimeError(f"call {run_id} {r.get('status')}: {r.get('error', '')}")
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
    if w.get("error") is not None:
        return Result(error=str(w["error"]), retryable=bool(w.get("retryable")), error_type=w.get("error_type") or "")
    if w.get("wait"):
        return Result(wait={"until": int(w["wait"]["until"]), "output": w["wait"].get("output")})
    return Result(output=w.get("output"))


def _duration_ms(d: str | None) -> int | None:
    """Milliseconds of a duration such as "300ms", "5s", "5m", "1h"."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h)", d or "")
    if not m:
        return None
    return int(float(m.group(1)) * {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}[m.group(2)])


def _done(id: str, info: dict[str, Any]) -> Any:
    if info.get("trimmed"):
        raise ResultLostError(f"workflow {id} finished, but kairo no longer keeps its result")
    if info.get("status") == "cancelled":
        raise Cancelled(f"workflow {id} cancelled")
    out = (info.get("output") or {}).get("payload")
    if not out:
        raise RuntimeError(f"workflow {id} {info.get('status')}: {info.get('error', '')}")
    if not out.get("ok"):
        raise RuntimeError(out.get("error"))
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


class _Limiter:
    """The limits of one destination (ADR 0059): steps at once, and the
    spacing of their starts (seconds, by the wall clock)."""

    def __init__(self) -> None:
        self.slots: _Slots | None = None
        self.every = 0.0
        self.next = 0.0  # when the next step may start (unix seconds)

    def tighten(self, limit: int, per_minute: float) -> None:
        if limit > 0 and (self.slots is None or limit < self.slots.size):
            self.slots = _Slots(limit)
        if per_minute > 0:
            self.every = max(self.every, 60 / per_minute)


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

    async def _run(self, plan: str, root: dict[str, Any] | None, input: Any, run_id: str) -> Any:
        try:
            return await self._k._call_run(plan, root, input, run_id, self.id)
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

    async def call(self, action: str, input: Any = None) -> Any:
        """Runs action with input (once, however often the workflow runs again)."""
        return await self._run(PLAN_CALL + action, None, input, self._next(PLAN_CALL + action, input))

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
        return await self._k._run_as(name, input, self._next("kairo.workflow/" + name, input), self.id, self)
