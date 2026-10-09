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
import logging
import os
import re
import secrets
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from .client import KairoError
from .core import Core
from .observe import LOGGER, RUN_SETTLED, RUN_STARTED, RUN_STUCK, STEP_FINISHED, STEP_STARTED, Observation, Observer, observe, step_status
from .protocol import Result
from .store import Changes, LeaseRow, RunRow, Store, TimerRow
from .worker import Task, TaskContext

#: Runs the action of a dispatched step here (it may block: it runs on a thread).
ActionHandler = Callable[[Task, TaskContext], Result]

#: Lets a dispatched step run (ADR 0059: limits): waits, on the event loop, and
#: returns what gives its slots back, or a Result if it stopped waiting.
Admit = Callable[[Task, TaskContext], Awaitable["Callable[[], None] | Result"]]

DONE = {"completed", "failed", "cancelled"}
SETTLED = DONE | {"blocked"}  # will not go on by itself
OUTCOMES = {"step_ok", "step_err", "step_wait"}
#: Events applied under the current plan whatever it is (ADR 0060): an
#: outcome that is in, cancel, resolve.
SAFE = {"cancel", "resolve", "step_ok", "step_wait"}

PLAN_WORKFLOW = "kairo.workflow"

#: EffectWeakenedError's message, without the run.
EFFECT_WEAKENED = "a real attempt is out and its action is no longer real"


class EffectWeakenedError(KairoError):
    """A call's action is no longer real while a real attempt of it is out
    (ADR 0060). The call waits: restore the action, or settle the call with
    resolve, resolve_failed or cancel."""

    def __init__(self, message: str) -> None:
        super().__init__(409, message)


def _sdk_plan(name: str) -> bool:
    """The SDK's own plans (one node each), which go on under the current
    version (ADR 0060)."""
    return name == PLAN_WORKFLOW or name.startswith("kairo.call/") or name.startswith("kairo.wait/")


def _strictly_real(plan: dict[str, Any]) -> bool:
    """Every action of plan is real and not retried on an unknown outcome."""
    effects = plan.get("effects") or {}
    idempotent = plan.get("idempotent") or {}
    return bool(effects) and all(e == "real" and not idempotent.get(a) for a, e in effects.items())


#: Finished trees removed per tick at most (ADR 0054); the rest next time.
REMOVE_PER_TICK = 100


def keep_finished_ms(opt: float | None | str = "env", env: str | None = None) -> float | None:
    """keep_finished from the options ("env": not given), or KAIRO_KEEP_FINISHED,
    or 24 hours (ADR 0054). None: kept for ever."""
    if opt != "env":
        return opt  # type: ignore[return-value]
    env = os.environ.get("KAIRO_KEEP_FINISHED") if env is None else env
    if not env:
        return 24 * 3600 * 1000
    if env == "forever":
        return None
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h|d)", env)
    if not m:
        raise ValueError(f"KAIRO_KEEP_FINISHED={env}: a duration such as 30m, 24h, 7d, or forever")
    return float(m.group(1)) * {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[m.group(2)]


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
    # The run that made this one, a workflow run's workflow and meta, when it
    # started and last changed (unix ms; ADR 0059).
    if row.parent:
        r["parent"] = row.parent
    if row.workflow:
        r["workflow"] = row.workflow
    # A workflow run's version (ADR 0060).
    if row.plan == PLAN_WORKFLOW and isinstance(r.get("input"), dict) and isinstance(r["input"].get("version"), str) and r["input"]["version"]:
        r["version"] = r["input"]["version"]
    if row.meta is not None:
        r["meta"] = json.loads(row.meta)
    r["created_at"] = row.created_at
    r["updated_at"] = row.updated_at
    return r


def new_token() -> int:
    """A drive lease's token: a nonzero int32 (ADR 0059)."""
    while True:
        t = secrets.randbelow(1 << 32) - (1 << 31)
        if t != 0:
            return t


class Embedded:
    def __init__(
        self,
        store: Store,
        *,
        wasm: Any = None,
        now: Callable[[], int] = _ms,
        lease_ms: int = 30_000,
        owner: str | None = None,
        keep_finished: float | None | str = "env",
        logger: logging.Logger | None = None,
        observe: Observer | None = None,
        manual_timers: bool = False,
    ) -> None:
        """keep_finished: how long a finished tree of runs is kept before tick
        removes it (ms; ADR 0054); None keeps it. Default: KAIRO_KEEP_FINISHED
        ("30m", "24h", "7d", "forever"), or 24 hours.

        logger takes what goes wrong in the runtime's background work (lease
        renewals, sweeps, timers, steps' outcomes; default
        logging.getLogger("kairo_sdk")). observe, if given, is given each
        Observation as it happens, on the event loop: it must not block.

        manual_timers: timers are kept in the store but not armed in this
        process: only tick fires them. For tests on a clock they move
        (kairo_sdk.testing.TestEnv), and for schedulers that own time."""
        self.logger = logger or LOGGER
        self.observer = observe
        self.core = Core(wasm)
        self.store = store
        self.now = now
        self._manual_timers = manual_timers
        self.lease_ms = lease_ms
        self.keep_ms = keep_finished_ms(keep_finished)
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
        # Workflows driven by this process now: their drive leases are
        # renewed with its steps' (ADR 0059).
        self._drives = 0
        # Set by Kairo (ADR 0059). lease_parents: a run that settles leases
        # its parent workflow to this process, to be driven on (suspend
        # mode). lost_drive takes up a drive lease whose owner stopped.
        # plan_for gives the definition of a plan not registered here (a
        # wait made elsewhere). admit lets a dispatched step run (limits).
        self.lease_parents = False
        self.lost_drive: Callable[[LeaseRow], Awaitable[None]] | None = None
        self.plan_for: Callable[[str], dict[str, Any] | None] | None = None
        self.admit: Admit | None = None

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

    def _plan(self, name: str) -> dict[str, Any] | None:
        """Plan name, registered here, or made now from plan_for."""
        c = self.plans.get(name)
        if c is not None or self.plan_for is None:
            return c
        d = self.plan_for(name)
        return self.register_plan(d) if d is not None else None

    async def run(
        self,
        plan: str,
        input: Any,
        *,
        run_id: str,
        vars: dict[str, Any] | None = None,
        parent: str | None = None,
        workflow: str | None = None,
        meta: dict[str, Any] | None = None,
        drive: int = 0,
        then: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Starts a run of plan, or finds it: a run id is an idempotency key.
        parent: the run that makes this one, which it is kept and removed with
        (ADR 0054). ADR 0059: workflow and meta, a workflow run's workflow and
        what the application gave it (set once); drive, its drive lease for
        this process (a token), set with its start; then, events applied in
        the start's transaction (a signal received before its wait)."""
        p = self._plan(plan)
        if p is None:
            raise KairoError(404, f"no plan {plan}")
        at = self.now()
        ev: dict[str, Any] = {"kind": "start", "at": at, "data": input}
        if vars:
            ev["vars"] = vars
        begin = {"plan": p, "parent": parent, "workflow": workflow, "meta": None if meta is None else json.dumps(meta), "drive": drive}
        try:
            existing = await self._process(run_id, [ev] + [dict(e, at=at) for e in then or ()], begin=begin)
        except Exception:
            # Two processes starting one id at once: the one that lost finds it.
            try:
                found = await self.store.get(run_id)
            except Exception:
                found = None
            if found is None:
                raise
            existing = True
        return {"run_id": run_id, "existing": existing}

    @property
    def lease(self) -> int:
        """The lease period (ms)."""
        return self.lease_ms

    async def claim_drive(self, run: str, token: int) -> bool:
        """Claims workflow run's drive lease with token for this process (ADR
        0059): whether no other process holds it."""
        now = self.now()
        return await self.store.claim_drive(LeaseRow(run, 0, token, self.owner, now + self.lease_ms), now)

    async def end_drive(self, run: str, token: int, resume: bool, owner: str | None = None) -> None:
        """Ends a drive lease (this process's, unless owner is given): removed,
        or (resume) left expired, for a tick to take up."""
        await self.store.end_drive(run, self.owner if owner is None else owner, token, self.now(), resume)

    def driving(self, on: bool) -> None:
        """Counts a workflow driven here (its lease renewed) while it is."""
        self._drives += 1 if on else -1
        self._renew()

    async def list(self, **f: Any) -> list[dict[str, Any]]:
        """Root runs in creation order (ADR 0059)."""
        return [info(r) for r in await self.store.list(**f)]

    async def recheck(self) -> None:
        """Without listen, reads the runs waited for here: one may have settled
        in another process (ADR 0059)."""
        if getattr(self.store, "listen", None) is not None:
            return
        for id in list(self._waiters):
            try:
                r = await self.get(id)
            except KairoError:
                continue
            if r["status"] in SETTLED:
                for f in list(self._waiters.get(id, ())):
                    if not f.done():
                        f.set_result(r)

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

    async def resolve(self, run_id: str, output: Any, error: str | None = None) -> None:
        """Settles a step stopped for review (ADR 0058), or a real attempt out
        that cannot go on (its action is no longer real, ADR 0060): it took
        effect (with output), or it did not (error; the run fails)."""
        row = await self.store.get(run_id)
        if row is None:
            raise KairoError(404, f"no run {run_id}")
        ins = self.core.inspect(row.state)
        # Stopped for review; or a real attempt out that cannot go on because
        # its action is no longer real (ADR 0060): only then, not one out as
        # usual (unknown marks it so to the core).
        acts = ins.get("review") or []
        out = False
        if not acts and ins.get("intents"):
            plan = self._plan(row.plan)
            if plan is not None and row.hash != plan["hash"] and self._effect_weakened(plan, row):
                acts, out = ins["intents"], True
        if not acts:
            raise KairoError(409, f"run {run_id} has no step stopped for review")
        ev: dict[str, Any] = {"kind": "resolve", "at": self.now(), "act": acts[0]}
        if out:
            ev["unknown"] = True
        if error is not None:
            ev["error"] = error
        else:
            ev["data"] = output
        await self._process(run_id, [ev])

    async def tick(self) -> None:
        """Takes up what no process is doing: steps whose lease expired and
        timers that are due. Runs whose plan is not registered here are left
        alone. One run that cannot go on does not stop the others (ADR 0060):
        it is logged and observed (RUN_STUCK), and put off a lease period, so
        that it does not stay first of what each tick takes up. Raises only
        when the store fails."""
        self._last_sweep = self.now()
        for lease in await self.store.expired_leases(self.now(), 1000):
            if self._closed:
                return
            # A workflow's driver stopped (ADR 0059).
            aw = self._recover(lease) if lease.act else (self.lost_drive(lease) if self.lost_drive is not None else None)
            if aw is not None and await self._stuck(lease.run, aw):
                try:
                    await self.store.hand_over(LeaseRow(lease.run, lease.act, lease.attempt, "", self.now() + self.lease_ms))
                except Exception as e:
                    self.logger.warning("kairo: run %s: putting off a lease: %s", lease.run, e)
        for t in await self.store.due_timers(self.now(), 1000):
            if self._closed:
                return
            if not await self._stuck(t.run, self._fire(t)):
                continue
            later = TimerRow(t.run, t.timer, t.act, self.now() + self.lease_ms)

            def put_off(row: RunRow | None, later: TimerRow = later) -> Changes[None]:
                ch: Changes[None] = Changes(None)
                if row is not None:
                    ch.set_timers.append(later)
                return ch

            try:
                await self.store.with_run(t.run, put_off)
            except Exception as e:
                self.logger.warning("kairo: run %s: putting off a timer: %s", t.run, e)
        # Finished trees past the time they are kept (ADR 0054).
        if self.keep_ms is not None:
            await self.store.remove_finished(self.now() - self.keep_ms, REMOVE_PER_TICK)

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

    def _background(self, aw: Awaitable[Any], msg: str, *args: Any) -> None:
        """Tracks background work aw, logging what goes wrong in it (but a
        run whose plan is another process's)."""

        async def run() -> None:
            try:
                await _skip_unknown_plan(aw)
            except Exception as e:
                self.logger.warning(msg + ": %s", *args, e)

        self._track(run())

    def _observe(self, o: Observation) -> None:
        observe(self.observer, self.logger, self.now, o)

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

    def _leasing(self) -> bool:
        return bool(self._running or self._drives > 0) and not self._closed

    def _renew(self) -> None:
        """Renews this process's leases while it runs steps or drives
        workflows: one task for all of them."""
        if self._leasing():
            if self._renewal is None:

                async def loop() -> None:
                    while self._leasing():
                        await asyncio.sleep(max(self.lease_ms / 3, 10) / 1000)
                        if not self._leasing():
                            return
                        try:
                            await self.store.renew_leases(self.owner, self.now() + self.lease_ms)
                        except Exception as e:
                            # Next time; past their expiry the leases are taken up.
                            self.logger.warning("kairo: renewing leases: %s", e)

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

    async def _stuck(self, run_id: str, aw: Awaitable[Any]) -> bool:
        """Awaits aw; logs and observes the run if it could not go on now (but
        one whose plan is another process's): whether it could not."""
        try:
            await _skip_unknown_plan(aw)
            return False
        except Exception as e:
            self.logger.warning("kairo: run %s cannot go on: %s", run_id, e)
            self._observe(Observation(RUN_STUCK, run_id=run_id, error=str(e)))
            return True

    def _adoptable(self, run_id: str, plan: dict[str, Any], row: RunRow, events: list[dict[str, Any]]) -> None:
        """Raises unless run row, which started under another version of plan,
        may go on under the current one (ADR 0060). Only the SDK's own plans
        (one node each) may; and not while a real attempt is out whose action
        the current plan does not treat as real (it would be retried on an
        unknown outcome, invariant 5), unless the events are safe under any
        settings (an outcome that is in, cancel, resolve)."""
        if not _sdk_plan(plan["name"]):
            raise KairoError(409, f"run {run_id}: plan {row.plan} changed since it started")
        if events and all(e["kind"] in SAFE for e in events):
            return
        if self._effect_weakened(plan, row):
            raise EffectWeakenedError(f"run {run_id}: {EFFECT_WEAKENED} ({row.plan})")

    def _effect_weakened(self, plan: dict[str, Any], row: RunRow) -> bool:
        """Run row, of an SDK plan, has a real attempt out whose action plan
        does not treat as real (ADR 0060)."""
        return _sdk_plan(plan["name"]) and not _strictly_real(plan) and bool(self.core.inspect(row.state).get("intent_durable"))

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

    async def _process(self, run_id: str, events: list[dict[str, Any]], begin: dict[str, Any] | None = None) -> bool:
        """Applies events to run_id in one transaction, then carries out the
        commands. With begin, a new run of its plan (or the existing one: the
        answer says which)."""
        start = begin["plan"] if begin is not None else None

        def change(row: RunRow | None) -> Changes[dict[str, Any]]:
            # A step's outcome ends its lease, applied or not (a stale one).
            end_leases = [e["act"] for e in events if e["kind"] in OUTCOMES and e.get("act") is not None]
            if row is not None and start is not None:
                return Changes({"existing": True, "started": False, "settled": False, "commands": [], "row": row})
            if row is None and start is None:
                return Changes({"existing": False, "started": False, "settled": False, "commands": [], "row": None}, end_leases=end_leases)
            plan = start if start is not None else self._plan(row.plan)  # type: ignore[union-attr]
            if plan is None:
                raise KairoError(404, f"run {run_id}: plan {row.plan} is not registered here")  # type: ignore[union-attr]
            # Goes on under the current plan, if it may (ADR 0060): nxt has its hash.
            if row is not None and row.hash != plan["hash"]:
                self._adoptable(run_id, plan, row, events)
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
                return Changes({"existing": False, "started": False, "settled": False, "commands": commands, "row": row}, end_leases=end_leases)
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
                parent=row.parent if row is not None else begin["parent"],  # type: ignore[index]
                workflow=row.workflow if row is not None else begin["workflow"],  # type: ignore[index]
                meta=row.meta if row is not None else begin["meta"],  # type: ignore[index]
            )
            settled = res["status"] in SETTLED and res["status"] != (row.status if row else None)
            ch: Changes[dict[str, Any]] = Changes(
                {"existing": False, "started": row is None, "settled": settled, "commands": commands, "row": nxt},
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
            done = res["status"] in DONE
            # Driven by this process from its start (ADR 0059).
            if row is None and begin is not None and begin["drive"] and not done:
                ch.set_leases.append(LeaseRow(run_id, 0, begin["drive"], self.owner, at + self.lease_ms))
            # Its workflow is to be driven on: by this process, or, if it
            # stops first, by whichever takes the lease up (ADR 0059).
            if settled and nxt.parent and self.lease_parents:
                ch.set_leases.append(LeaseRow(nxt.parent, 0, new_token(), self.owner, at + self.lease_ms))
            return ch

        out = await self.store.with_run(run_id, change)
        if self._closed:
            return out["existing"]
        row = out["row"]
        if out["started"] and row is not None:
            self._observe(Observation(RUN_STARTED, run_id=run_id, parent=row.parent or "", plan=row.plan, workflow=row.workflow or ""))
        for c in out["commands"]:
            self._carry_out(run_id, c)
        # Settled by this transaction (not a run found settled already).
        if out["settled"] and row is not None:
            self._observe(
                Observation(
                    RUN_SETTLED,
                    run_id=run_id,
                    parent=row.parent or "",
                    plan=row.plan,
                    workflow=row.workflow or "",
                    status=row.status,
                    error=row.error or "",
                )
            )
            r = info(row)
            for f in list(self._waiters.get(run_id, ())):
                if not f.done():
                    f.set_result(r)
            for h in list(self._hooks):
                h(r)
        # Along the way: what no process is doing (at most once a lease period).
        if self.now() - self._last_sweep >= self.lease_ms:
            self._background(self.tick(), "kairo: sweeping")
        return out["existing"]

    def _carry_out(self, run_id: str, c: dict[str, Any]) -> None:
        kind = c["kind"]
        if kind == "dispatch":
            self._background(self._dispatch(run_id, c), "kairo: run %s: applying a step's outcome", run_id)
        elif kind == "timer":
            if self._manual_timers:
                return  # fired by tick only
            key = f"{run_id}\0t{c['timer']}"
            old = self._timers.pop(key, None)
            if old is not None:
                old.cancel()
            t = TimerRow(run_id, c["timer"], c.get("act", 0), c["at"])

            def due() -> None:
                self._timers.pop(key, None)
                if not self._closed:
                    self._background(self._fire(t), "kairo: run %s: firing a timer", run_id)

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
        step = {"run_id": run_id, "action": task.action, "step_id": task.step_id, "attempt": task.attempt}
        release: Callable[[], None] | None = None
        began = 0.0
        try:
            # Over a limit, the step waits here for a slot, leased (ADR 0059).
            got = await self.admit(task, ctx) if self.admit is not None else None
            if isinstance(got, Result):
                res = got
            else:
                release = got
                self._observe(Observation(STEP_STARTED, **step))
                began = time.monotonic()
                res = await asyncio.to_thread(self.handler, task, ctx)
        except Exception as e:  # a handler bug is the step's definite failure
            res = Result(error=f"{type(e).__name__}: {e}", error_type=type(e).__name__)
        finally:
            if release is not None:
                release()
            self._running.pop(key, None)
            self._renew()
        if began:
            # It ran (not stopped waiting for a slot).
            self._observe(Observation(STEP_FINISHED, **step, status=step_status(res), error=res.error, duration=time.monotonic() - began))
        if self._closed:
            return
        if res.pending is not None:
            # It runs elsewhere (ADR 0052): its lease goes there, until its
            # outcome comes (complete) or the lease expires. An outcome that
            # came first ended the lease; then nothing is handed over.
            until = self.now() + int(res.pending["lease_ms"])
            try:
                await self.store.hand_over(LeaseRow(run_id, c["act"], c.get("attempt", 0), res.pending["owner"], until))
            except Exception as e:
                self.logger.warning("kairo: run %s: handing over a lease: %s", run_id, e)
            return
        await self._process(run_id, [_outcome(res, c["act"], c.get("attempt", 0), self.now())])

    async def next_wake(self) -> int | None:
        """When something is next to do: the earliest timer or lease expiry (ADR 0053)."""
        return await self.store.next_wake()

    async def complete(self, run_id: str, act: int, attempt: int, res: Result) -> None:
        """Applies the outcome of a step that ran elsewhere (ADR 0052). A stale one is ignored."""
        self._observe(Observation(STEP_FINISHED, run_id=run_id, attempt=attempt, status=step_status(res), error=res.error))
        await self._process(run_id, [_outcome(res, act, attempt, self.now())])


def _outcome(res: Result, act: int, attempt: int, at: int) -> dict[str, Any]:
    """The event of a step's outcome."""
    if res.error or res.unknown:
        ev = {"kind": "step_err", "at": at, "act": act, "attempt": attempt, "error": res.error or "outcome unknown",
              "retryable": res.retryable, "unknown": res.unknown}
        if res.error_type:
            ev["error_type"] = res.error_type
        return ev
    if res.wait is not None:
        return {"kind": "step_wait", "at": at, "act": act, "attempt": attempt, "deadline": int(res.wait["until"]), "data": res.wait.get("output")}
    return {"kind": "step_ok", "at": at, "act": act, "attempt": attempt, "data": res.output}


async def _skip_unknown_plan(aw: Awaitable[Any]) -> None:
    """A run whose plan this process does not have is another process's to take up."""
    try:
        await aw
    except KairoError as e:
        if e.status != 404:
            raise
