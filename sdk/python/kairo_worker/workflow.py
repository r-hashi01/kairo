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
import inspect
import json
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .backend import Backend, HttpBackend
from .client import KairoError, finished
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
    ) -> None:
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

    def action(self, name: str, *, effect: str = "real", timeout: str | None = None, destination: str | None = None):
        """Declares an action: "real" (the default) acts outside and never runs
        twice; "unprotected" may run again. The handler takes the input and a
        TaskContext; it may be a plain or an async function."""
        if name.startswith("kairo."):
            raise ValueError(f'action {name}: names beginning with "kairo." are kairo\'s')

        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._actions[name] = _Action(fn, effect, timeout, destination)
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

    async def tick(self) -> None:
        """Suspend mode: takes up due timers and steps whose process stopped,
        drives on the workflows whose calls settle, and returns once that is
        done (call it from a scheduler)."""
        await self.backend.tick()
        await self._settle()

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
        out = a.handler(input, ctx)
        if inspect.isawaitable(out):
            out = asyncio.run(_await(out))
        return Result(output=out)

    async def run(self, name: str, input: Any = None, *, id: str | None = None) -> Any:
        """Runs workflow name as execution id, or resumes it: calls that
        finished return their recorded results. Returns its result (suspend
        mode: raises Suspended when it waits)."""
        return await self._run_as(name, input, id or str(uuid.uuid4()))

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
            return

    async def cancel(self, id: str) -> None:
        """Cancels workflow id: the calls it is waiting for are cancelled with it
        (suspend mode: it stops when it is driven next)."""
        await self.backend.cancel(id)

    async def _run_as(self, name: str, input: Any, id: str) -> Any:
        fn = self._workflows.get(name)
        if fn is None:
            raise KeyError(f"no workflow {name}")
        await self._plan_workflow()
        started = await self.backend.run(
            PLAN_WORKFLOW, {"workflow": name, "input": input}, run_id=id, vars={"started_at": int(time.time() * 1000)}
        )
        info = await self.backend.get(id)
        if finished(info):
            return _done(id, info)
        if started.get("existing"):
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

    async def _call_run(self, plan: str, root: dict[str, Any] | None, input: Any, run_id: str) -> Any:
        if root is not None:
            await self._plan(plan, root)
        await self.backend.run(plan, {"in": input}, run_id=run_id)
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


async def _await(a: Awaitable[Any]) -> Any:
    return await a


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
            return await self._k._call_run(plan, root, input, run_id)
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
        return await self._k._run_as(name, input, self._next("kairo.workflow/" + name, input))
