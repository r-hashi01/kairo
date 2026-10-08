"""The embedded runtime (ADR 0051): kairo without a resident server.

The pure core (WASM) runs in this process; runs live in a database (Store).
Each event is one transaction: lock the run, apply the event, append it,
replace the state, arm or disarm timers. The commands are carried out after
the commit: a real step only once its intent was committed with it
(invariant 4).

A step dispatched here is leased to this process while it runs. A process
that stops leaves its leases to expire; whoever finds an expired lease
applies an unknown outcome to the step, as kairod does when a worker goes
away: an unprotected step runs again, a real one stops for review
(invariant 5). Expired leases and due timers are looked for when asked
(tick), and along the way of work at most once a lease period: no polling.

The same runtime as the TypeScript SDK's (sdk/ts/src/embedded.ts).
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from .client import KairoError
from .core import Core
from .protocol import Result
from .store import Changes, LeaseRow, RunRow, Store, TimerRow
from .worker import Task, TaskContext

#: Runs the action of a dispatched step here (it may block: it runs on a thread).
ActionHandler = Callable[[Task, TaskContext], Result]

DONE = {"completed", "failed", "cancelled"}
SETTLED = DONE | {"blocked"}  # will not go on by itself
OUTCOMES = {"step_ok", "step_err", "step_wait"}


def _ms() -> int:
    return int(time.time() * 1000)


def info(row: RunRow) -> dict[str, Any]:
    """A run as the HTTP API reports it (engine.RunInfo)."""
    r: dict[str, Any] = {"run_id": row.id, "plan": row.plan, "tenant": "default", "status": row.status}
    if row.input is not None:
        r["input"] = json.loads(row.input)
    if row.output is not None:
        r["output"] = json.loads(row.output)
    if row.error:
        r["error"] = row.error
    return r


class Embedded:
    def __init__(
        self,
        store: Store,
        *,
        wasm: Any = None,
        now: Callable[[], int] = _ms,
        lease_ms: int = 30_000,
        owner: str | None = None,
    ) -> None:
        self.core = Core(wasm)
        self.store = store
        self.now = now
        self.lease_ms = lease_ms
        self.owner = owner or str(uuid.uuid4())
        self._named = owner is not None
        self.plans: dict[str, dict[str, Any]] = {}
        self.handler: ActionHandler | None = None
        self._waiters: dict[str, set[asyncio.Future[dict[str, Any]]]] = {}
        self._hooks: set[Callable[[dict[str, Any]], None]] = set()
        self._timers: dict[str, asyncio.TimerHandle] = {}
        self._running: dict[str, TaskContext] = {}
        self._busy: set[asyncio.Future[Any]] = set()
        self._renewal: asyncio.Task[None] | None = None
        self._last_sweep = 0
        self._unlisten: Callable[[], Awaitable[None]] | None = None
        self._closed = False

    async def open(self) -> Embedded:
        await self.store.init()
        # A process of the same name that stopped: its steps are not running.
        if self._named:
            await self.store.expire_leases(self.owner, self.now())
        listen = getattr(self.store, "listen", None)
        if listen is not None:
            self._unlisten = await listen(self._settled_elsewhere)
        return self

    def register_actions(self, specs: list[dict[str, Any]], handler: ActionHandler) -> None:
        self.core.register(specs)
        self.handler = handler

    def register_plan(self, definition: dict[str, Any]) -> dict[str, Any]:
        c = self.core.compile(definition)
        self.plans[c["name"]] = c
        return c

    def on_settled(self, hook: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        """Calls hook with each run that settles in this process; returns how to stop."""
        self._hooks.add(hook)
        return lambda: self._hooks.discard(hook)

    async def idle(self) -> None:
        """Returns once the work started here (steps, timers that fired) is done."""
        while True:
            pending = [f for f in self._busy if not f.done()]
            self._busy.difference_update([f for f in self._busy if f.done()])
            if not pending:
                return
            await asyncio.gather(*pending, return_exceptions=True)

    async def run(self, plan: str, input: Any, *, run_id: str, vars: dict[str, Any] | None = None) -> dict[str, Any]:
        """Starts a run of plan, or finds it: a run id is an idempotency key."""
        p = self.plans.get(plan)
        if p is None:
            raise KairoError(404, f"no plan {plan}")
        at = self.now()
        ev: dict[str, Any] = {"kind": "start", "at": at, "data": input}
        if vars:
            ev["vars"] = vars
        existing = await self._process(run_id, [ev], start=p)
        return {"run_id": run_id, "existing": existing}

    async def get(self, run_id: str) -> dict[str, Any]:
        row = await self.store.get(run_id)
        if row is None:
            raise KairoError(404, f"no run {run_id}")
        return info(row)

    async def wait(self, run_id: str) -> dict[str, Any]:
        """Waits until the run has finished or stopped for review (in this
        process's view, and other processes' with a store that listens)."""
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(run_id, set()).add(fut)
        try:
            # Registered first, then read: an end between the two still wakes us.
            now = await self.get(run_id)
            if now["status"] in SETTLED:
                return now
            return await fut
        finally:
            ws = self._waiters.get(run_id)
            if ws is not None:
                ws.discard(fut)
                if not ws:
                    del self._waiters[run_id]

    async def signal(self, run_id: str, name: str, payload: Any = None) -> None:
        await self._process(run_id, [{"kind": "signal", "at": self.now(), "name": name, "data": payload}])

    async def cancel(self, run_id: str) -> None:
        await self._process(run_id, [{"kind": "cancel", "at": self.now(), "error": "cancelled"}])

    async def tick(self) -> None:
        """Takes up what no process is doing: steps whose lease expired and
        timers that are due. Runs whose plan is not registered here are left alone."""
        self._last_sweep = self.now()
        for lease in await self.store.expired_leases(self.now(), 1000):
            await _skip_unknown_plan(self._recover(lease))
        for t in await self.store.due_timers(self.now(), 1000):
            await _skip_unknown_plan(self._fire(t))

    async def close(self) -> None:
        """Stops, as a process that stops: no new step starts, timers are
        dropped (they stay in the database), steps running here are told to
        stop. Returns once what this runtime was writing is written."""
        self._closed = True
        for h in self._timers.values():
            h.cancel()
        self._timers.clear()
        for ctx in self._running.values():
            ctx.cancelled.set()
        self._running.clear()
        self._renew()
        await self.idle()
        if self._unlisten is not None:
            await self._unlisten()
        await self.store.close()

    # --- internals ---------------------------------------------------------

    def _track(self, aw: Awaitable[Any]) -> None:
        fut = asyncio.ensure_future(aw)
        self._busy.add(fut)
        fut.add_done_callback(self._busy.discard)

    def _settled_elsewhere(self, run_id: str) -> None:
        if not self._waiters.get(run_id):
            return

        async def wake() -> None:
            r = await self.get(run_id)
            if r["status"] in SETTLED:
                for f in list(self._waiters.get(run_id, ())):
                    if not f.done():
                        f.set_result(r)

        self._track(wake())

    def _renew(self) -> None:
        """Renews this process's leases while it runs steps: one task for all of them."""
        if self._running and not self._closed:
            if self._renewal is None:

                async def loop() -> None:
                    while self._running and not self._closed:
                        await asyncio.sleep(max(self.lease_ms / 3, 10) / 1000)
                        if not self._running or self._closed:
                            return
                        await self.store.renew_leases(self.owner, self.now() + self.lease_ms)

                task = asyncio.ensure_future(loop())
                self._renewal = task

                def ended(_: Any) -> None:
                    if self._renewal is task:
                        self._renewal = None

                task.add_done_callback(ended)
            return
        if self._renewal is not None:
            self._renewal.cancel()
            self._renewal = None

    def _recover(self, lease: LeaseRow) -> Awaitable[bool]:
        return self._process(
            lease.run,
            [
                {
                    "kind": "step_err",
                    "at": self.now(),
                    "act": lease.act,
                    "attempt": lease.attempt,
                    "unknown": True,
                    "retryable": True,
                    "error": "the process running the step stopped",
                    "error_type": "process_lost",
                }
            ],
        )

    async def _fire(self, t: TimerRow) -> None:
        await self._process(t.run, [{"kind": "timer", "at": self.now(), "act": t.act, "timer": t.timer}])

    async def _process(self, run_id: str, events: list[dict[str, Any]], start: dict[str, Any] | None = None) -> bool:
        """Applies events to run_id in one transaction, then carries out the commands."""

        def change(row: RunRow | None) -> Changes[dict[str, Any]]:
            # A step's outcome ends its lease, applied or not (a stale one).
            end_leases = [e["act"] for e in events if e["kind"] in OUTCOMES and e.get("act") is not None]
            if row is not None and start is not None:
                return Changes({"existing": True, "settled": False, "commands": [], "row": row})
            if row is None and start is None:
                return Changes({"existing": False, "settled": False, "commands": [], "row": None}, end_leases=end_leases)
            plan = start if start is not None else self.plans.get(row.plan)  # type: ignore[union-attr]
            if plan is None:
                raise KairoError(404, f"run {run_id}: plan {row.plan} is not registered here")  # type: ignore[union-attr]
            if row is not None and row.hash != plan["hash"]:
                raise KairoError(409, f"run {run_id}: plan {row.plan} changed since it started")
            state = row.state if row is not None else b""
            recorded: list[dict[str, Any]] = []
            commands: list[dict[str, Any]] = []
            res: dict[str, Any] = {
                "status": row.status if row else "running",
                "output": json.loads(row.output) if row and row.output else None,
                "error": row.error if row else None,
            }

            def apply(ev: dict[str, Any]) -> None:
                nonlocal state, res
                s, r = self.core.apply(plan["plan"], run_id, state, ev)
                if r.get("ignored"):
                    return
                state = s
                recorded.append(ev)
                res = {"status": r["status"], "output": r.get("output"), "error": r.get("error")}
                for c in r["commands"]:
                    commands.append(c)
                    # A real step's intent, committed with what dispatched it.
                    if c["kind"] == "dispatch" and c.get("effect") == "real":
                        apply({"kind": "intent", "at": ev["at"], "act": c["act"], "attempt": c.get("attempt", 0)})

            for ev in events:
                apply(ev)
            if not recorded:
                return Changes({"existing": False, "settled": False, "commands": commands, "row": row}, end_leases=end_leases)
            at = self.now()
            nxt = RunRow(
                id=run_id,
                plan=plan["name"],
                hash=plan["hash"],
                state=state,
                input=row.input if row is not None else json.dumps(events[0].get("data")),
                status=res["status"],
                output=None if res["output"] is None else json.dumps(res["output"]),
                error=res["error"] or None,
                seq=0,
                created_at=row.created_at if row is not None else at,
                updated_at=at,
            )
            settled = res["status"] in SETTLED and res["status"] != (row.status if row else None)
            ch: Changes[dict[str, Any]] = Changes(
                {"existing": False, "settled": settled, "commands": commands, "row": nxt},
                events=recorded,
                row=nxt,
                clear=res["status"] in DONE,
                end_leases=end_leases,
                notify=settled,
            )
            for c in commands:
                if c["kind"] == "timer":
                    ch.set_timers.append(TimerRow(run_id, c["timer"], c.get("act", 0), c["at"]))
                elif c["kind"] == "cancel_timer":
                    ch.delete_timers.append(c["timer"])
                elif c["kind"] == "dispatch":
                    # Dispatched to this process: leased to it while it runs.
                    ch.set_leases.append(LeaseRow(run_id, c["act"], c.get("attempt", 0), self.owner, at + self.lease_ms))
            return ch

        out = await self.store.with_run(run_id, change)
        if self._closed:
            return out["existing"]
        for c in out["commands"]:
            self._carry_out(run_id, c)
        # Settled by this transaction (not a run found settled already).
        if out["settled"] and out["row"] is not None:
            r = info(out["row"])
            for f in list(self._waiters.get(run_id, ())):
                if not f.done():
                    f.set_result(r)
            for h in list(self._hooks):
                h(r)
        # Along the way: what no process is doing (at most once a lease period).
        if self.now() - self._last_sweep >= self.lease_ms:
            self._track(self.tick())
        return out["existing"]

    def _carry_out(self, run_id: str, c: dict[str, Any]) -> None:
        kind = c["kind"]
        if kind == "dispatch":
            self._track(self._dispatch(run_id, c))
        elif kind == "timer":
            key = f"{run_id}\0t{c['timer']}"
            old = self._timers.pop(key, None)
            if old is not None:
                old.cancel()
            t = TimerRow(run_id, c["timer"], c.get("act", 0), c["at"])

            def due() -> None:
                self._timers.pop(key, None)
                if not self._closed:
                    self._track(_skip_unknown_plan(self._fire(t)))

            delay = max(0, c["at"] - self.now()) / 1000
            self._timers[key] = asyncio.get_running_loop().call_later(delay, due)
        elif kind == "cancel_timer":
            h = self._timers.pop(f"{run_id}\0t{c['timer']}", None)
            if h is not None:
                h.cancel()
        elif kind == "abort":
            ctx = self._running.get(f"{run_id}\0{c.get('act', 0)}")
            if ctx is not None:
                ctx.cancelled.set()

    async def _dispatch(self, run_id: str, c: dict[str, Any]) -> None:
        """Runs a dispatched step here (on a thread) and applies its outcome."""
        if self.handler is None:
            return
        key = f"{run_id}\0{c['act']}"
        ctx = TaskContext(lambda _d: None)
        self._running[key] = ctx
        self._renew()
        task = Task(
            run_id=run_id,
            step_id=c.get("step_id", ""),
            act=c["act"],
            attempt=c.get("attempt", 0),
            idempotency_key=c.get("idempotency_key", ""),
            action=c.get("action", ""),
            input=c.get("input"),
        )
        try:
            res = await asyncio.to_thread(self.handler, task, ctx)
        except Exception as e:  # a handler bug is the step's definite failure
            res = Result(error=f"{type(e).__name__}: {e}", error_type=type(e).__name__)
        finally:
            self._running.pop(key, None)
            self._renew()
        if self._closed:
            return
        at = self.now()
        ev: dict[str, Any]
        if res.error or res.unknown:
            ev = {"kind": "step_err", "at": at, "act": c["act"], "attempt": c.get("attempt", 0), "error": res.error or "outcome unknown",
                  "retryable": res.retryable, "unknown": res.unknown}
            if res.error_type:
                ev["error_type"] = res.error_type
        elif res.wait is not None:
            ev = {"kind": "step_wait", "at": at, "act": c["act"], "attempt": c.get("attempt", 0), "deadline": int(res.wait["until"]),
                  "data": res.wait.get("output")}
        else:
            ev = {"kind": "step_ok", "at": at, "act": c["act"], "attempt": c.get("attempt", 0), "data": res.output}
        await self._process(run_id, [ev])


async def _skip_unknown_plan(aw: Awaitable[Any]) -> None:
    """A run whose plan this process does not have is another process's to take up."""
    try:
        await aw
    except KairoError as e:
        if e.status != 404:
            raise
