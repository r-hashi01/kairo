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
"""

from __future__ import annotations

import asyncio
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
from .http import SignatureError, check_url, post, sign, verify
from .protocol import Result
from .worker import Task, TaskContext

PLAN_CALL = "kairo.call/"
PLAN_WAIT = "kairo.wait/"
PLAN_WORKFLOW = "kairo.workflow"
NOW, RANDOM, SLEEP = "kairo.now", "kairo.random", "kairo.sleep"


class ResultLostError(Exception):
    """The workflow ran into a call whose result kairo no longer keeps."""


class Cancelled(Exception):
    """The workflow was cancelled."""


class Suspended(Exception):
    """The workflow waits (a timer, a signal): tick() or signal() drives it on."""


@dataclass
class _Action:
    handler: Callable[..., Any]
    effect: str
    timeout: str | None
    destination: str | None
    url: str | None = None
    async_: bool = False


def _canonical(v: Any) -> str:
    """JSON with object keys sorted: equal values, equal text."""
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _call_id(workflow: str, kind: str, input: Any, n: int) -> str:
    h = hashlib.sha256((kind + "\0" + _canonical(input)).encode()).hexdigest()[:24]
    return f"{workflow}/{h}.{n}"


def _wait_root(name: str) -> dict[str, Any]:
    return {"kind": "wait", "id": "w", "signal": name}


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
        """Actions over HTTP(S) (ADR 0052): secret signs the calls and the
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
        self._actions: dict[str, _Action] = {}
        self._workflows: dict[str, Callable[..., Awaitable[Any]]] = {}
        self._planned: set[str] = set()
        self._driving: set[asyncio.Task[Any]] = set()
        self._closing = False
        if mode not in ("wait", "suspend"):
            raise ValueError(f"mode {mode}: wait or suspend")
        self.suspend = mode == "suspend"
        if self.suspend and not hasattr(self.backend, "idle"):
            raise ValueError("suspend mode needs the embedded backend")
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
    ):
        """Declares an action: "real" (the default) acts outside and never runs
        twice; "unprotected" may run again. The handler takes the input and a
        TaskContext; it may be a plain or an async function.

        url (ADR 0052): the action's steps are called over HTTP(S) there, and
        the handler runs there (asgi_app). async_: where the URL serves it,
        it answers at once (202) and sends the outcome to the callback."""
        if name.startswith("kairo."):
            raise ValueError(f'action {name}: names beginning with "kairo." are kairo\'s')
        if url:
            check_url(url, self._allow_insecure if allow_insecure is None else allow_insecure)

        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._actions[name] = _Action(fn, effect, timeout, destination, url, async_)
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
        specs: list[dict[str, Any]] = []
        for name, a in self._actions.items():
            s: dict[str, Any] = {"action": name, "effect": a.effect}
            if a.timeout:
                s["timeout"] = a.timeout
            if a.destination:
                s["destination"] = a.destination
            specs.append(s)
        for name in (NOW, RANDOM, SLEEP):
            specs.append({"action": name, "effect": "unprotected"})
        if any(a.url for a in self._actions.values()):
            # Actions over HTTP(S) (ADR 0052) take their outcomes on the callback.
            if not self._secret or not self._callback_url:
                raise ValueError("actions with a url need secret and callback_url")
            check_url(self._callback_url, self._allow_insecure)
            if not hasattr(self.backend, "complete"):
                raise ValueError("actions with a url need the embedded backend")
        await self.backend.start(specs, self._serve)
        for s in specs:
            await self._plan(PLAN_CALL + s["action"], {"kind": "step", "id": "call", "action": s["action"], "input": {"in": "$input.in"}})
        await self._plan_workflow()
        if self.suspend:
            # A call that settles drives its workflow on (its id is the
            # workflow's id, "/", the call's key).
            def settled(r: dict[str, Any]) -> None:
                rid = r["run_id"]
                i = rid.rfind("/")
                if i > 0:
                    self._redrive(rid[:i])

            self._stop_hook = self.backend.on_settled(settled)

    async def close(self) -> None:
        """Stops serving actions and driving workflows, as a process that stops
        does: the workflows it drove stay unfinished in kairo, to be resumed."""
        self._closing = True
        if self._stop_hook is not None:
            self._stop_hook()
        for t in list(self._driving):
            t.cancel()
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
                    await self._run_as(inp["workflow"], inp.get("input"), id)
                except Exception:
                    pass  # suspended again, failed (recorded), or cancelled
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
        if a.url:
            return self._call_remote(task, a, input)
        out = a.handler(input, ctx)
        if inspect.isawaitable(out):
            out = asyncio.run(_await(out))
        return Result(output=out)

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

    async def run(self, name: str, input: Any = None, *, id: str | None = None) -> Any:
        """Runs workflow name as execution id, or resumes it: calls that
        finished return their recorded results. Returns its result (suspend
        mode: raises Suspended when it waits)."""
        try:
            return await self._run_as(name, input, id or str(uuid.uuid4()))
        finally:
            await self._wake_up()

    async def signal(self, id: str, name: str, payload: Any = None) -> None:
        """Sends a signal to the first wait for it in workflow id that has not received one."""
        # The waits' plan, which this process may not have needed yet.
        await self._plan(PLAN_WAIT + name, _wait_root(name))
        n = 0
        while True:
            run_id = _call_id(id, PLAN_WAIT + name, None, n)
            try:
                r = await self.backend.get(run_id)
            except KairoError as e:
                if e.status == 404:
                    raise LookupError(f"workflow {id} does not wait for {name}") from None
                raise
            if finished(r):
                n += 1
                continue
            await self.backend.signal(run_id, name, payload)
            if self.suspend:
                await self._settle()
                await self._wake_up()
            return

    async def cancel(self, id: str) -> None:
        """Cancels workflow id: the calls it is waiting for are cancelled with it
        (suspend mode: it stops when it is driven next)."""
        await self.backend.cancel(id)

    async def _run_as(self, name: str, input: Any, id: str, parent: str | None = None) -> Any:
        fn = self._workflows.get(name)
        if fn is None:
            raise KeyError(f"no workflow {name}")
        await self._plan_workflow()
        started = await self.backend.run(
            PLAN_WORKFLOW, {"workflow": name, "input": input}, run_id=id, vars={"started_at": int(time.time() * 1000)}, parent=parent
        )
        info = await self.backend.get(id)
        if finished(info):
            return _done(id, info)
        # (The embedded runtime keeps the calls while the workflow runs, ADR 0054.)
        if started.get("existing") and not getattr(self.backend, "keeps_calls", False):
            at = int((info.get("vars") or {}).get("started_at") or 0)
            if at and time.time() * 1000 - at > self._ttl * 1000:
                raise ResultLostError(f"workflow {id} started more than {self._ttl}s ago: its calls' records may be gone")
        ctx = Context(self, id)
        body = asyncio.ensure_future(fn(ctx, input))
        self._driving.add(body)
        self._driving_ids.add(id)
        cancelled = False

        async def watch() -> None:
            nonlocal cancelled
            r = await self.backend.wait(id)
            if r.get("status") == "cancelled" and not body.done():
                cancelled = True
                body.cancel()

        # The workflow's own run ends when it is cancelled: stop the calls.
        # (Suspended, a cancelled workflow stops when it is driven next.)
        watcher = None if self.suspend else asyncio.ensure_future(watch())
        try:
            value = await body
        except Suspended:
            raise  # goes on later
        except asyncio.CancelledError:
            if cancelled:
                raise Cancelled(f"workflow {id} cancelled") from None
            raise
        except Exception as e:
            await self.backend.signal(id, "done", {"ok": False, "error": str(e)})
            raise
        finally:
            self._driving.discard(body)
            self._driving_ids.discard(id)
            if watcher is not None:
                watcher.cancel()
        await self.backend.signal(id, "done", {"ok": True, "value": value})
        return value

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


class Context:
    """What a workflow function calls."""

    def __init__(self, k: Kairo, id: str) -> None:
        self.id = id
        self._k = k
        self._seen: dict[str, int] = {}

    def _next(self, kind: str, input: Any) -> str:
        key = kind + "\0" + _canonical(input)
        n = self._seen.get(key, 0)
        self._seen[key] = n + 1
        return _call_id(self.id, kind, input, n)

    async def _run(self, plan: str, root: dict[str, Any] | None, input: Any, run_id: str) -> Any:
        try:
            return await self._k._call_run(plan, root, input, run_id, self.id)
        except asyncio.CancelledError:
            # The workflow was cancelled: so is the call (unless this process
            # is stopping, and the call goes on in kairo).
            if not self._k._closing:
                try:
                    await self._k.backend.cancel(run_id)
                except Exception:
                    pass
            raise

    async def call(self, action: str, input: Any = None) -> Any:
        """Runs action with input (once, however often the workflow runs again)."""
        return await self._run(PLAN_CALL + action, None, input, self._next(PLAN_CALL + action, input))

    async def wait_for(self, name: str) -> Any:
        """Waits for signal name (see Kairo.signal); returns its payload."""
        run_id = self._next(PLAN_WAIT + name, None)
        out = await self._run(PLAN_WAIT + name, _wait_root(name), None, run_id)
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
        return await self._k._run_as(name, input, self._next("kairo.workflow/" + name, input), self.id)
