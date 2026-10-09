"""A kairo for an application's tests (the Go SDK's kairotest.Env): in
memory, in suspend mode, on a clock the test moves.

    env = await TestEnv.open()
    env.k.action("charge")(stub_charge)
    env.k.workflow("refund")(refund)
    await env.start()
    with self.assertRaises(Suspended):  # refund sleeps a day before it charges
        await env.k.run("refund", req, id="r-1")
    await env.advance(24 * 3600)
    out = await env.k.result("r-1")
    await env.close()
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any

from .backend import EmbeddedBackend
from .observe import RUN_STARTED, Observation
from .store import SQLiteStore
from .workflow import Kairo

__all__ = ["Call", "TestEnv"]

#: Where the clock starts by default.
DEFAULT_AT = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


@dataclass
class Call:
    """One call a workflow made: an action's, a wait for a signal, or a child workflow."""

    run_id: str
    kind: str  # "call", "wait" or "workflow"
    name: str  # the action, the signal or the workflow
    input: Any  # what it was given
    output: Any  # an action's output, a wait's payload (None if it timed out), a workflow's result
    status: str  # completed, failed, cancelled, blocked, running
    error: str = ""


class TestEnv:
    """A kairo for an application's tests: in memory (SQLite), in suspend
    mode, on a clock the test moves. A workflow that waits (a sleep, a
    signal, a timeout) raises Suspended from run; advance moves the clock and
    fires what is due, signal sends what it waits for, and result gives its
    result. Nothing waits in real time."""

    __test__ = False  # not a test case (pytest)

    def __init__(self) -> None:
        self.k: Kairo
        self._clock = 0  # unix ms
        # Run id: the order it started in, and the run that made it. A fake
        # clock gives runs made at once the same creation time: the order is
        # taken from the observations instead.
        self._started: dict[str, tuple[int, str]] = {}

    @classmethod
    async def open(cls, *, at: datetime.datetime | int | None = None, wasm: Any = None, **kairo: Any) -> TestEnv:
        """Opens a TestEnv. at: where the clock starts (an aware datetime or
        unix ms; default 2026-01-01T00:00:00Z). wasm: kairo.wasm (default:
        the one in this package). kairo: Kairo's other arguments
        (concurrency, observe, logger, ...); backend and mode are the env's.
        Declare actions and workflows on env.k, then env.start(); close it
        when the test ends (or use it with async with)."""
        e = cls()
        at = DEFAULT_AT if at is None else at
        e._clock = round(at.timestamp() * 1000) if isinstance(at, datetime.datetime) else int(at)
        observe = kairo.pop("observe", None)

        def observed(o: Observation) -> None:
            if o.kind == RUN_STARTED and o.run_id not in e._started:
                e._started[o.run_id] = (len(e._started), o.parent)
            if observe is not None:
                observe(o)

        backend = await EmbeddedBackend.open(SQLiteStore(":memory:"), wasm=wasm, now=lambda: e._clock, manual_timers=True)
        e.k = Kairo(**kairo, backend=backend, mode="suspend", observe=observed)
        return e

    async def __aenter__(self) -> TestEnv:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def start(self) -> None:
        """Starts env.k (Kairo.start)."""
        await self.k.start()

    def now(self) -> datetime.datetime:
        """The env's clock."""
        return datetime.datetime.fromtimestamp(self._clock / 1000, datetime.timezone.utc)

    async def advance(self, seconds: float | datetime.timedelta) -> None:
        """Moves the clock by seconds, firing what comes due on the way at the
        time it is due (a sleep that ends, then a timeout that starts from
        there), and driving the workflows on."""
        if isinstance(seconds, datetime.timedelta):
            seconds = seconds.total_seconds()
        target = self._clock + round(seconds * 1000)
        for _ in range(10_000):
            nxt = await self.k.tick()
            if nxt is None or nxt > target:
                self._clock = target
                return
            if nxt > self._clock:
                self._clock = nxt  # to the next thing due
        raise RuntimeError("kairo test env: still due after 10000 ticks: a timer that fires again at once?")

    async def signal(self, id: str, name: str, payload: Any = None) -> None:
        """Sends a signal (Kairo.signal)."""
        await self.k.signal(id, name, payload)

    async def calls(self, id: str) -> list[Call]:
        """The calls workflow id made, in the order it made them."""
        kids = sorted((n, run) for run, (n, parent) in self._started.items() if parent == id)
        return [_call(await self.k.backend.get(run)) for _, run in kids]

    async def close(self) -> None:
        await self.k.close()


def _call(r: dict[str, Any]) -> Call:
    plan: str = r["plan"]
    given = r.get("input") if isinstance(r.get("input"), dict) else {}
    out = r.get("output")
    status, error = r["status"], r.get("error", "")
    if plan.startswith("kairo.call/"):
        return Call(r["run_id"], "call", plan[len("kairo.call/") :], given.get("in"), out, status, error)
    if plan.startswith("kairo.wait/"):
        name = plan[len("kairo.wait/") :]
        head, sep, ms = name.rpartition("@")
        if sep and ms.isdigit():
            name = head  # its timeout (ADR 0059)
        payload = out.get("payload") if isinstance(out, dict) else None
        return Call(r["run_id"], "wait", name, given.get("in"), payload, status, error)
    value = out.get("payload", {}).get("value") if isinstance(out, dict) and isinstance(out.get("payload"), dict) else None
    return Call(r["run_id"], "workflow", r.get("workflow", ""), given.get("input"), value, status, error)
