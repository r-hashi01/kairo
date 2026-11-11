"""The embedded runtime's gaps filled (ADR 0059): drive leases, signals
before their waits, limits, wait timeouts, listing and submitting; the same
behaviour as sdk/go/kairotest and sdk/ts/src/driving.test.ts. SQLite;
needs go (to build kairo.wasm from this repository) and wasmtime."""

from __future__ import annotations

import asyncio
import datetime
import logging
import os
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
from typing import Any

from kairo_sdk.backend import EmbeddedBackend
from kairo_sdk.embedded import EFFECT_WEAKENED
from kairo_sdk.observe import RUN_SETTLED, RUN_STARTED, RUN_STUCK, STEP_FINISHED, STEP_STARTED, Observation
from kairo_sdk.store import SQLiteStore, WorkflowRetryError
from kairo_sdk.workflow import CallError, Cancelled, DestinationLimits, Kairo, NotWaitingError, RetryAfterError, RetryableError, SignalReceipt, StoppedError, Suspended, TimedOutError, WorkflowError

try:
    from .test_workflow import HAS_GO, HAS_WASMTIME, build_wasm
except ImportError:  # discovered from tests/ as a top-level module
    from test_workflow import HAS_GO, HAS_WASMTIME, build_wasm

DONE = ("completed", "failed", "cancelled")


def query(path: str, sql: str, *args: Any) -> list[tuple]:
    """Rows of the database at path, as another process would read them."""
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return db.execute(sql, args).fetchall()
    finally:
        db.close()


def children(path: str, id: str) -> list[str]:
    return [r[0] for r in query(path, "SELECT id FROM kairo_run WHERE parent = ? ORDER BY created_at, id", id)]


async def wait_until(cond) -> None:
    for _ in range(400):
        if cond():
            return
        await asyncio.sleep(0.025)
    raise AssertionError("condition not reached")


class Dying:
    """A store that stops answering, as the database does for a process that
    stopped without closing: its leases are not renewed any more."""

    CUT = {"with_run", "renew_leases", "expired_leases", "due_timers", "end_drive"}

    def __init__(self, store: SQLiteStore) -> None:
        self._store = store
        self.dead = False

    def __getattr__(self, name: str) -> Any:
        v = getattr(self._store, name)
        if name not in self.CUT:
            return v

        async def cut(*args: Any, **kw: Any) -> Any:
            if self.dead:
                raise OSError("the process stopped")
            return await v(*args, **kw)

        return cut


class FailingOnce:
    def __init__(self, store: SQLiteStore) -> None:
        self._store = store
        self.fail = False

    def __getattr__(self, name: str) -> Any:
        v = getattr(self._store, name)
        if name != "with_run":
            return v

        async def once(*args: Any, **kw: Any) -> Any:
            if self.fail:
                self.fail = False
                raise sqlite3.OperationalError("store busy")
            return await v(*args, **kw)

        return once


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime not found")
class DrivingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-driving-py-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    def path(self, name: str) -> str:
        return os.path.join(self.dir, name)

    async def open(self, path: str, *, lease_ms: int = 30_000, store: Any = None, **kw: Any) -> Kairo:
        """A process on the database at path (or on store)."""
        b = await EmbeddedBackend.open(store if store is not None else SQLiteStore(path), wasm=self.wasm, lease_ms=lease_ms)
        return Kairo(backend=b, **kw)

    def status(self, path: str, id: str) -> str:
        return query(path, "SELECT status FROM kairo_run WHERE id = ?", id)[0][0]

    def test_stopping_waiting_is_not_cancelling(self) -> None:
        """A caller that stops waiting does not cancel the workflow: its calls
        stay, and it goes on when it is run again."""
        path = self.path("stop.db")

        async def main() -> None:
            k = await self.open(path)

            @k.workflow("inner")
            async def inner(ctx, _):
                return await ctx.wait_for("go")

            @k.workflow("outer")
            async def outer(ctx, _):
                return await ctx.workflow("inner")

            await k.start()
            first = asyncio.ensure_future(k.run("outer", None, id="stop-1"))
            found: list[str] = []

            def waiting() -> bool:
                kids = children(path, "stop-1")
                if kids and children(path, kids[0]):
                    found.append(kids[0])
                    return True
                return False

            await wait_until(waiting)
            first.cancel()
            with self.assertRaises(StoppedError):
                await first
            for id in ("stop-1", found[0]):
                self.assertNotIn(self.status(path, id), DONE, id)
            again = asyncio.ensure_future(k.run("outer", None, id="stop-1"))
            await k.signal(found[0], "go", "on")
            self.assertEqual(await again, "on")
            await k.close()

        asyncio.run(main())

    def test_cancelling_cancels_the_workflows_it_made_and_their_calls(self) -> None:
        path = self.path("tree.db")

        async def main() -> None:
            k = await self.open(path)
            started, stopped = threading.Event(), threading.Event()

            @k.action("slow", effect="unprotected")
            def slow(_, ctx):
                started.set()
                ctx.cancelled.wait(30)
                stopped.set()
                return None

            @k.workflow("inner")
            async def inner(ctx, _):
                return await ctx.call("slow")

            @k.workflow("outer")
            async def outer(ctx, _):
                return await ctx.workflow("inner")

            await k.start()
            res = asyncio.ensure_future(k.run("outer", None, id="tree-1"))
            await wait_until(started.is_set)
            await k.cancel("tree-1")
            kids = children(path, "tree-1")
            self.assertEqual(len(kids), 1)
            grandkids = children(path, kids[0])
            self.assertEqual(len(grandkids), 1)
            for id in (kids[0], grandkids[0]):
                self.assertEqual((await k.backend.get(id))["status"], "cancelled", id)
            with self.assertRaises(Cancelled):
                await res
            await wait_until(stopped.is_set)
            await k.close()

        asyncio.run(main())

    def test_unreached_child_is_cancelled_before_parent_settles(self) -> None:
        """A replay that takes another path cancels the child left by the
        stopped process without recovering its action first (ADR 0062)."""

        async def case(outcome: str) -> None:
            path = self.path(f"unreached-{outcome}.db")
            attempts = 0
            skip = False

            async def start() -> Kairo:
                k = await self.open(path, mode="suspend")

                @k.action("old", effect="unprotected")
                def old(_, ctx):
                    nonlocal attempts
                    attempts += 1
                    ctx.cancelled.wait(30)
                    raise RuntimeError("stopped")

                @k.workflow("parent")
                async def parent(ctx, _):
                    if skip:
                        if outcome == "failure":
                            raise RuntimeError("guard failed")
                        return "new path"
                    return await ctx.call("old")

                await k.start()
                return k

            id = f"unreached-{outcome}"
            k = await start()
            first = asyncio.ensure_future(k.run("parent", None, id=id))
            await wait_until(lambda: attempts == 1)
            await k.close()
            with self.assertRaises(StoppedError):
                await first

            skip = True
            k = await start()
            if outcome == "failure":
                with self.assertRaises(WorkflowError):
                    await k.run("parent", None, id=id)
            else:
                self.assertEqual(await k.run("parent", None, id=id), "new path")
            self.assertEqual(attempts, 1)
            child = children(path, id)[0]
            self.assertEqual((await k.backend.get(child))["status"], "cancelled")
            self.assertIn((await k.backend.get(id))["status"], DONE)
            await k.close()

        async def main() -> None:
            await case("success")
            await case("failure")

        asyncio.run(main())

    def test_a_workflow_that_raises_fails_and_the_process_goes_on(self) -> None:
        async def main() -> None:
            k = await self.open(self.path("bad.db"))
            runs = {"n": 0}

            @k.workflow("bad")
            def bad(ctx, _):  # raises at once, not in a coroutine
                runs["n"] += 1
                raise TypeError("boom")

            await k.start()
            with self.assertRaisesRegex(RuntimeError, "boom"):
                await k.run("bad", None, id="bad-1")
            # Recorded as failed: run again, it fails at once, without running.
            with self.assertRaisesRegex(RuntimeError, "boom"):
                await k.run("bad", None, id="bad-1")
            self.assertEqual(runs["n"], 1)
            await k.close()

        asyncio.run(main())

    def test_a_stopped_driver_is_taken_up(self) -> None:
        """A resident process that stops without closing: another one takes its
        workflow up when its drive lease expires, and the real call it made is
        not made again."""
        path = self.path("taken.db")
        writes = {"n": 0}

        async def start(store: Any, hang: bool) -> Kairo:
            k = await self.open(path, store=store, lease_ms=200)

            @k.action("write")
            def write(p, ctx):
                writes["n"] += 1
                return f"wrote {p}"

            @k.workflow("w")
            async def w(ctx, p):
                out = await ctx.call("write", p)
                if hang:
                    await asyncio.Event().wait()  # as a process that stops here
                return out + "!"

            await k.start()
            return k

        async def main() -> None:
            store = Dying(SQLiteStore(path))
            k1 = await start(store, True)
            await k1.submit("w", "f", id="taken-1")
            await wait_until(lambda: bool(query(path, "SELECT 1 FROM kairo_run WHERE parent = 'taken-1' AND status = 'completed'")))
            store.dead = True  # stops: its lease is not renewed

            k2 = await start(None, False)
            self.assertEqual(await asyncio.wait_for(k2.result("taken-1"), 10), "wrote f!")
            self.assertEqual(writes["n"], 1, "the real call was not made again")
            await k2.close()
            await k1.close()

        asyncio.run(main())

    def test_a_live_driver_is_not_doubled(self) -> None:
        """While a process drives a workflow, another one that runs it does not
        drive it too: it waits for the result."""
        path = self.path("one.db")
        runs = {"k1": 0, "k2": 0}

        async def start(name: str) -> Kairo:
            k = await self.open(path, lease_ms=200)

            @k.workflow("w")
            async def w(ctx, _):
                runs[name] += 1
                return await ctx.wait_for("go")

            await k.start()
            return k

        async def main() -> None:
            k1 = await start("k1")
            k2 = await start("k2")
            await k1.submit("w", None, id="one-1")
            await wait_until(lambda: runs["k1"] == 1)
            res = asyncio.ensure_future(k2.run("w", None, id="one-1"))
            await asyncio.sleep(0.3)  # a lease period and more: k1 renews it
            await k2.signal("one-1", "go", "went")
            self.assertEqual(await res, "went")
            self.assertEqual(runs["k2"], 0, "k2 drove it")
            await k1.close()
            await k2.close()

        asyncio.run(main())

    def test_drain_keeps_the_outcome(self) -> None:
        """drain lets a real step that runs finish and records its outcome, and
        starts no new step: the next process goes on from there, with nothing
        to review and nothing run twice (issue #1)."""
        path = self.path("drain.db")
        runs = {"pay": 0, "mail": 0}

        gate = threading.Event()  # the actions run on threads of their own

        async def main() -> None:
            async def start() -> Kairo:
                k = await self.open(path, lease_ms=200)

                @k.action("pay", effect="real")
                def pay(_, ctx):
                    runs["pay"] += 1
                    gate.wait()
                    return "paid"

                @k.action("mail", effect="real")
                def mail(_, ctx):
                    runs["mail"] += 1
                    return "mailed"

                @k.workflow("w")
                async def w(ctx, _):
                    p = await ctx.call("pay")
                    m = await ctx.call("mail")
                    return f"{p}, {m}"

                await k.start()
                return k

            k = await start()
            await k.submit("w", None, id="drain-1")
            await wait_until(lambda: runs["pay"] == 1)
            drained = asyncio.ensure_future(asyncio.wait_for(k.drain(), 10))
            await asyncio.sleep(0.05)  # pay is held at its gate: drain cannot return before it opens
            self.assertFalse(drained.done(), "drained while pay runs")
            gate.set()
            await drained
            self.assertEqual(runs["mail"], 0, "a step started while draining")
            await k.close()

            k = await start()
            self.assertEqual(await asyncio.wait_for(k.run("w", None, id="drain-1"), 10), "paid, mailed")
            self.assertEqual(runs, {"pay": 1, "mail": 1})
            await k.close()

        asyncio.run(main())

    def test_signals_sent_before_their_waits_are_received_there_in_order(self) -> None:
        async def main() -> None:
            k = await self.open(self.path("ahead.db"))
            gate = threading.Event()

            @k.action("gate", effect="unprotected")
            def gate_(_, ctx):
                gate.wait(30)
                return None

            @k.workflow("w")
            async def w(ctx, _):
                await ctx.call("gate")
                a = await ctx.wait_for("s")
                b = await ctx.wait_for("s", timeout=60)
                return [a, b]

            await k.start()
            with self.assertRaisesRegex(LookupError, "no workflow"):
                await k.signal("nope", "s", "x")
            await k.submit("w", None, id="ahead-1")
            for p in ("first", "second"):
                await k.signal("ahead-1", "s", p)
            gate.set()
            self.assertEqual(await k.result("ahead-1"), ["first", "second"])
            await k.close()

        asyncio.run(main())

    def test_signal_receipts_and_only_if_waiting_are_atomic_across_processes(self) -> None:
        async def main() -> None:
            path = self.path("signal-receipts.db")
            gates = {"only": threading.Event(), "early": threading.Event()}
            gate_started = {"only": threading.Event(), "early": threading.Event()}

            def register(k: Kairo) -> None:
                @k.action("signal-gate", effect="unprotected")
                def signal_gate(key, ctx):
                    gate_started[key].set()
                    gates[key].wait()

                @k.workflow("signals")
                async def signals(ctx, key):
                    first = await ctx.wait_for("message")
                    await ctx.call("signal-gate", key)
                    return [first, await ctx.wait_for("message")]

                @k.workflow("signal-race")
                async def signal_race(ctx, _):
                    await ctx.call("signal-gate", "create")
                    first = await ctx.wait_for("message")
                    await ctx.call("signal-gate", "create-after")
                    return [first, await ctx.wait_for("message")]

            a = await self.open(path, lease_ms=50)
            b = await self.open(path, lease_ms=50)
            register(a)
            register(b)
            await a.start()
            await b.start()

            def waits(id: str) -> list[tuple]:
                return query(
                    path,
                    "SELECT id, status FROM kairo_run WHERE parent = ? AND plan = 'kairo.wait/message' ORDER BY created_at, id",
                    id,
                )

            async def waiting(id: str, n: int) -> None:
                await wait_until(lambda: len(waits(id)) == n and waits(id)[n - 1][1] not in DONE)

            await a.submit("signals", "only", id="signal-only-1")
            await waiting("signal-only-1", 1)
            only = await asyncio.gather(
                a.send_signal("signal-only-1", "message", "answer-a", only_if_waiting=True),
                b.send_signal("signal-only-1", "message", "answer-b", only_if_waiting=True),
                return_exceptions=True,
            )
            receipts = [x for x in only if isinstance(x, SignalReceipt)]
            failures = [x for x in only if isinstance(x, BaseException)]
            self.assertEqual(len(receipts), 1)
            self.assertEqual((receipts[0].index, receipts[0].early), (0, False))
            self.assertEqual(len(failures), 1)
            self.assertIsInstance(failures[0], NotWaitingError)
            await wait_until(gate_started["only"].is_set)
            self.assertEqual(len(waits("signal-only-1")), 1, "no answer was left for the next wait")
            gates["only"].set()
            await waiting("signal-only-1", 2)
            second = await a.send_signal("signal-only-1", "message", "second", only_if_waiting=True)
            self.assertEqual((second.index, second.early), (1, False))
            first_payload = "answer-a" if isinstance(only[0], SignalReceipt) else "answer-b"
            self.assertEqual(await a.result("signal-only-1"), [first_payload, "second"])
            with self.assertRaises(NotWaitingError):
                await a.send_signal("signal-only-1", "message", "late", only_if_waiting=True)
            with self.assertRaises(LookupError):
                await a.send_signal("no-such-workflow", "message", "x", only_if_waiting=True)

            await a.submit("signals", "early", id="signal-early-1")
            await waiting("signal-early-1", 1)
            sent = await asyncio.gather(
                a.send_signal("signal-early-1", "message", "early-a"),
                b.send_signal("signal-early-1", "message", "early-b"),
            )
            ordered = sorted(zip(sent, ("early-a", "early-b")), key=lambda x: x[0].index)
            self.assertEqual([(x.index, x.early) for x, _ in ordered], [(0, False), (1, True)])
            self.assertNotEqual(ordered[0][0].run_id, ordered[1][0].run_id)
            await wait_until(gate_started["early"].is_set)
            gates["early"].set()
            self.assertEqual(await a.result("signal-early-1"), [payload for _, payload in ordered])

            gates["create"] = threading.Event()
            gates["create-after"] = threading.Event()
            gate_started["create"] = threading.Event()
            gate_started["create-after"] = threading.Event()
            await a.submit("signal-race", None, id="signal-create-1")
            await wait_until(gate_started["create"].is_set)
            racing = asyncio.create_task(
                b.send_signal("signal-create-1", "message", "raced", only_if_waiting=True)
            )
            gates["create"].set()
            first = "raced"
            try:
                receipt = await racing
                self.assertEqual((receipt.index, receipt.early), (0, False))
            except NotWaitingError:
                await waiting("signal-create-1", 1)
                await a.send_signal("signal-create-1", "message", "manual", only_if_waiting=True)
                first = "manual"
            await wait_until(gate_started["create-after"].is_set)
            self.assertEqual(len(waits("signal-create-1")), 1, "the creation race did not leave an early signal")
            gates["create-after"].set()
            await waiting("signal-create-1", 2)
            await a.send_signal("signal-create-1", "message", "second", only_if_waiting=True)
            self.assertEqual(await a.result("signal-create-1"), [first, "second"])

            gates["legacy"] = threading.Event()
            gate_started["legacy"] = threading.Event()
            await a.submit("signals", "legacy", id="signal-legacy-1")
            await waiting("signal-legacy-1", 1)
            await asyncio.gather(
                a.signal("signal-legacy-1", "message", "legacy-a"),
                b.signal("signal-legacy-1", "message", "legacy-b"),
            )
            await wait_until(gate_started["legacy"].is_set)
            self.assertEqual(len(waits("signal-legacy-1")), 2)
            gates["legacy"].set()
            self.assertEqual(sorted(await a.result("signal-legacy-1")), ["legacy-a", "legacy-b"])
            await a.close()
            await b.close()

        asyncio.run(main())

    def test_a_wait_times_out(self) -> None:
        async def main() -> None:
            k = await self.open(self.path("late.db"))

            @k.workflow("w")
            async def w(ctx, _):
                try:
                    return await ctx.wait_for("approve", timeout=0.1)
                except TimedOutError:
                    return "timed out"

            await k.start()
            self.assertEqual(await k.run("w", None, id="late-1"), "timed out")
            await k.close()

        asyncio.run(main())

    def test_steps_over_a_limit_wait_for_a_slot_and_starts_keep_to_the_rate(self) -> None:
        async def main() -> None:
            k = await self.open(self.path("limits.db"), concurrency=3)
            lock = threading.Lock()
            seen = {"running": 0, "most": 0}
            starts: list[float] = []

            @k.action("slow", effect="unprotected", limit=2)
            def slow(n, ctx):
                with lock:
                    seen["running"] += 1
                    seen["most"] = max(seen["most"], seen["running"])
                time.sleep(0.02)
                with lock:
                    seen["running"] -= 1
                return n

            @k.action("paced", effect="unprotected", rate=600)  # one each 100ms
            def paced(n, ctx):
                with lock:
                    starts.append(time.monotonic())
                return n

            @k.workflow("w")
            async def w(ctx, _):
                out = await asyncio.gather(*[ctx.call("slow", i) for i in range(6)], *[ctx.call("paced", i) for i in range(3)])
                return sum(out[:6])

            await k.start()
            began = time.monotonic()
            self.assertEqual(await k.run("w", None, id="limits-1"), 15)
            self.assertLessEqual(seen["most"], 2, f"{seen['most']} steps of slow at once")
            starts.sort()
            # The i-th start is not before its turn: i spacings after the
            # workflow began (a start that comes late on its thread does not
            # fail this); 1ms for timers that fire early by their rounding.
            for i, s in enumerate(starts):
                self.assertGreaterEqual(s - began, i * 0.1 - 0.001, f"paced start {i} came {(s - began) * 1000:.0f}ms after the workflow began")
            await k.close()

        asyncio.run(main())

    def test_runtime_destinations_share_actual_weight_and_retry_after(self) -> None:
        async def main() -> None:
            resolved: list[str] = []

            def limits(destination: str) -> DestinationLimits:
                resolved.append(destination)
                return DestinationLimits(weight_per_minute=60_000)

            k = await self.open(self.path("runtime-destinations.db"), destination_limits=limits)
            state = {"runs": 0, "first_finished": 0.0, "second_started": 0.0, "retry_attempts": 0, "retry_began": 0.0}

            @k.action("metered", effect="unprotected")
            def metered(value, ctx):
                state["runs"] += 1
                if value == "first":
                    ctx.set_weight(10_100)
                    state["first_finished"] = time.monotonic()
                elif value == "second":
                    state["second_started"] = time.monotonic()
                return state["runs"]

            @k.action("retry-after", effect="unprotected", max_attempts=2, backoff="1ms")
            def retry_after(_value, _ctx):
                state["retry_attempts"] += 1
                if state["retry_attempts"] == 1:
                    state["retry_began"] = time.monotonic()
                    raise RetryAfterError("busy", 0.08)
                return time.monotonic() - state["retry_began"]

            @k.workflow("w")
            async def w(ctx, _):
                a = await ctx.call("metered", "same", destination="provider-a")
                b = await ctx.call("metered", "same", destination="provider-b")
                await ctx.call("metered", "first", destination="provider-a")
                await ctx.call("metered", "second", destination="provider-a")
                retry_delay = await ctx.call("retry-after", None, destination="provider-a")
                return {"a": a, "b": b, "retry_delay": retry_delay}

            await k.start()
            out = await k.run("w", None, id="runtime-destinations-1")
            self.assertNotEqual(out["a"], out["b"], "different destinations must have different call identities")
            self.assertIn("provider-a", resolved)
            self.assertIn("provider-b", resolved)
            self.assertGreaterEqual(state["second_started"] - state["first_finished"], 0.075)
            self.assertGreaterEqual(out["retry_delay"], 0.06)
            await k.close()

        asyncio.run(main())

    def test_submit_result_and_list(self) -> None:
        async def main() -> None:
            k = await self.open(self.path("list.db"))

            @k.workflow("double")
            async def double(ctx, n):
                return 2 * n

            @k.workflow("other")
            async def other(ctx, _):
                return None

            @k.workflow("parent")
            async def parent(ctx, n):
                return await ctx.workflow("double", n)

            await k.start()
            ids = ["list-1", "list-2", "list-3"]
            for i, id in enumerate(ids):
                self.assertEqual(await k.submit("double", i, id=id, meta={"user": id}), id)
            await k.run("other", None, id="other-1")
            self.assertEqual(await k.run("parent", 5, id="parent-1"), 10)
            for i, id in enumerate(ids):
                self.assertEqual(await k.result(id), 2 * i)
            all = await k.list(workflow="double")
            self.assertEqual(len(all), 3, "the child workflow of parent-1 is not a root")
            for i, r in enumerate(all):
                self.assertEqual(r["run_id"], ids[i])
                self.assertEqual(r["workflow"], "double")
                self.assertEqual(r["meta"], {"user": ids[i]})
                self.assertEqual(r["status"], "completed")
                self.assertTrue(r["created_at"] > 0 and r["updated_at"] >= r["created_at"])
            page = await k.list(workflow="double", after=all[0]["run_id"], limit=1)
            self.assertEqual([r["run_id"] for r in page], ["list-2"])
            self.assertEqual(len(await k.list()), 5)
            since = datetime.datetime.fromtimestamp(all[0]["created_at"] / 1000)
            self.assertEqual(len(await k.list(status="completed", since=since)), 5)
            self.assertEqual(len(await k.list(until=all[0]["created_at"])), 0)
            await k.close()

        asyncio.run(main())

    def test_a_transient_store_error_retries_the_workflow_without_recording_failure(self) -> None:
        async def main() -> None:
            path = self.path("store-retry.db")
            store = FailingOnce(SQLiteStore(path))
            k = await self.open(path, store=store, lease_ms=50)
            runs = {"first": 0, "second": 0, "workflow": 0, "explicit": 0}
            armed = False

            @k.action("first")
            def first(_, ctx):
                runs["first"] += 1
                return "one"

            @k.action("second", effect="unprotected")
            def second(_, ctx):
                runs["second"] += 1
                return "two"

            @k.workflow("store-retry")
            async def store_retry(ctx, _):
                nonlocal armed
                runs["workflow"] += 1
                a = await ctx.call("first")
                if not armed:
                    armed = True
                    store.fail = True
                return a + await ctx.call("second")

            @k.workflow("explicit-retry")
            async def explicit_retry(ctx, _):
                runs["explicit"] += 1
                if runs["explicit"] == 1:
                    raise WorkflowRetryError("try later")
                return "ok"

            await k.start()
            self.assertEqual(await k.run("store-retry", None, id="store-retry-1"), "onetwo")
            self.assertEqual((runs["first"], runs["second"]), (1, 1))
            self.assertGreaterEqual(runs["workflow"], 2)
            self.assertEqual((await k.backend.get("store-retry-1"))["status"], "completed")
            self.assertEqual(await k.run("explicit-retry", None, id="explicit-retry-1"), "ok")
            self.assertEqual(runs["explicit"], 2)
            await k.close()

        asyncio.run(main())

    def test_retries_and_typed_errors(self) -> None:
        """An action's retryable failures are retried up to its max_attempts,
        its backoff apart; a call that fails is a CallError, a workflow that
        fails a WorkflowError."""

        async def main() -> None:
            k = await self.open(self.path("retry.db"))
            runs = {"flaky": 0, "broken": 0}

            @k.action("flaky", effect="unprotected", max_attempts=5, backoff="1ms")
            def flaky(_, ctx):
                runs["flaky"] += 1
                if runs["flaky"] < 5:
                    raise RetryableError("busy")
                return runs["flaky"]

            @k.action("broken", effect="unprotected", max_attempts=5)
            def broken(_, ctx):
                runs["broken"] += 1
                raise OSError("no such file")

            @k.workflow("w")
            async def w(ctx, _):
                n = await ctx.call("flaky")
                try:
                    await ctx.call("broken")
                    raise AssertionError("broken did not fail")
                except CallError as e:
                    if e.action != "broken" or e.status != "failed" or "no such file" not in e.message:
                        raise AssertionError(f"not the CallError: {e!r}") from e
                await ctx.call("broken", 1)
                return n

            await k.start()
            with self.assertRaises(WorkflowError) as cm:
                await k.run("w", None, id="retry-1")
            self.assertEqual(cm.exception.id, "retry-1")
            self.assertIn("no such file", cm.exception.message)
            self.assertEqual(runs["flaky"], 5)
            self.assertEqual(runs["broken"], 2, "a failure not retryable: once each")
            await k.close()

        asyncio.run(main())

    def test_observe(self) -> None:
        """observe is told of runs that start and settle and of steps'
        attempts; an observer that raises does not stop the runtime."""

        async def main() -> None:
            seen: list[Observation] = []

            def observer(o: Observation) -> None:
                seen.append(o)
                if o.kind == STEP_STARTED and o.action == "boom":
                    raise RuntimeError("an observer that raises does not stop the runtime")

            logger = logging.getLogger("kairo_sdk.test_observe")
            k = await self.open(self.path("observe.db"), logger=logger, observe=observer)
            flaky_runs = [0]

            @k.action("flaky", effect="unprotected", backoff="1ms")
            def flaky(_, ctx):
                flaky_runs[0] += 1
                if flaky_runs[0] == 1:
                    raise RetryableError("busy")
                return "ok"

            @k.action("boom", effect="unprotected")
            def boom(_, ctx):
                return "b"

            @k.workflow("w")
            async def w(ctx, _):
                return await ctx.call("flaky") + await ctx.call("boom")

            await k.start()
            with self.assertLogs(logger, level="ERROR") as logs:
                self.assertEqual(await k.run("w", None, id="obs-1"), "okb")
            await k.close()
            self.assertEqual(len(logs.records), 1)
            self.assertIn("the observer raised", logs.records[0].getMessage())
            got = []
            for o in seen:
                self.assertGreater(o.at, 0, f"no time: {o}")
                if o.kind == RUN_STARTED:
                    got.append(f"start {o.plan} {o.workflow}")
                elif o.kind == RUN_SETTLED:
                    got.append(f"settled {o.plan} {o.status}")
                elif o.kind == STEP_STARTED:
                    got.append(f"step {o.action} {o.attempt}")
                elif o.kind == STEP_FINISHED:
                    self.assertIsNotNone(o.duration)
                    got.append(f"step {o.action} {o.attempt} {o.status} {o.error}")
            self.assertEqual(
                got,
                [
                    "start kairo.workflow w",
                    "start kairo.call/flaky ",
                    "step flaky 1",
                    # The error as recorded: the Python SDK records the exception's type with it.
                    "step flaky 1 retryable RetryableError: busy",
                    "step flaky 2",
                    "step flaky 2 ok ",
                    "settled kairo.call/flaky completed",
                    "start kairo.call/boom ",
                    "step boom 1",
                    "step boom 1 ok ",
                    "settled kairo.call/boom completed",
                    "settled kairo.workflow completed",
                ],
            )

        asyncio.run(main())

    def test_versions_are_pinned(self) -> None:
        """A run goes on with the version it started with; new runs start with
        the current one; a process without a run's version leaves it (ADR 0060)."""
        path = self.path("versions.db")
        runs: dict[str, int] = {}

        async def start(*versions: str) -> Kairo:
            k = await self.open(path, lease_ms=100)
            for i, v in enumerate(versions):

                async def w(ctx, _, v=v):
                    runs[v] = runs.get(v, 0) + 1
                    return f"{v}:{await ctx.wait_for('go')}"

                k.workflow("w", version=v, draining=i < len(versions) - 1)(w)
            await k.start()
            return k

        async def main() -> None:
            k1 = await start("1")
            await k1.submit("w", None, id="ver-1")
            await wait_until(lambda: runs.get("1") == 1)
            await k1.close()  # its drive lease is left expired

            # Only version 2 here: it does not drive ver-1.
            k2 = await start("2")
            await k2.tick()  # takes up ver-1's lease, as a sweep does
            self.assertEqual(runs.get("2", 0), 0, "version 2 drove a version 1 run")
            await k2.close()

            k3 = await start("1", "2")
            await k3.signal("ver-1", "go", "x")
            self.assertEqual(await asyncio.wait_for(k3.result("ver-1"), 10), "1:x")
            await k3.submit("w", None, id="ver-2")
            await k3.signal("ver-2", "go", "y")
            self.assertEqual(await asyncio.wait_for(k3.result("ver-2"), 10), "2:y")
            self.assertEqual((await k3.backend.get("ver-1")).get("version"), "1")
            self.assertEqual((await k3.backend.get("ver-2")).get("version"), "2")
            await k3.close()

            bad = await self.open(self.path("versions-bad.db"))
            for v in ("1", "2"):

                async def f(ctx, _):
                    return None

                bad.workflow("w", version=v)(f)
            with self.assertRaisesRegex(ValueError, "2 current versions"):
                await bad.start()
            await bad.close()

        asyncio.run(main())

    def test_settings_change_under_a_call(self) -> None:
        """An action's settings change while a call of it waits to retry: the
        call goes on under the new ones (it used to stop for good)."""
        path = self.path("settings.db")
        attempts = [0]

        async def start(timeout: str | None = None) -> Kairo:
            k = await self.open(path, mode="suspend")

            @k.workflow("w")
            async def w(ctx, _):
                return await ctx.call("a")

            @k.action("a", effect="unprotected", backoff="50ms", timeout=timeout)
            def a(_, ctx):
                attempts[0] += 1
                if attempts[0] == 1:
                    raise RetryableError("busy")
                return "ok"

            await k.start()
            return k

        async def main() -> None:
            k = await start()
            with self.assertRaises(Suspended):
                await k.run("w", None, id="set-1")
            await k.close()
            k = await start("1m")  # a deploy changed its settings
            for _ in range(400):
                await k.tick()
                if (await k.backend.get("set-1"))["status"] in DONE:
                    break
                await asyncio.sleep(0.025)
            self.assertEqual(await k.run("w", None, id="set-1"), "ok")
            await k.close()

        asyncio.run(main())

    def test_an_effect_is_not_weakened(self) -> None:
        """A real attempt is out when its action is changed to unprotected: the
        call does not go on under the new settings (it would be retried on its
        unknown outcome); tick goes on with the others; resolve_failed settles it."""
        path = self.path("weakened.db")
        stuck: list[str] = []
        quiet = logging.getLogger("kairo_sdk.test_weakened")
        quiet.addHandler(logging.NullHandler())
        quiet.propagate = False

        def observer(o: Observation) -> None:
            if o.kind == RUN_STUCK:
                stuck.append(o.error)

        async def start(mode: str, effect: str) -> Kairo:
            k = await self.open(path, lease_ms=100, mode=mode, logger=quiet, observe=observer)

            @k.action("pay", effect=effect)
            def pay(_, ctx):
                ctx.cancelled.wait()  # the process stops while it runs
                raise RuntimeError("stopped")

            @k.workflow("w")
            async def w(ctx, _):
                try:
                    await ctx.call("pay")
                    return "paid"
                except CallError as e:
                    return f"resolved: {e.message}"

            @k.workflow("nap")
            async def nap(ctx, _):
                await ctx.sleep(0.2)
                return "rested"

            await k.start()
            return k

        async def main() -> None:
            # A resident process: pay's real attempt is out (its intent
            # recorded) when it stops; nap's timer is in the store.
            k = await start("wait", "real")
            for w in ("w", "nap"):
                await k.submit(w, None, id=f"{w}-1")
            found: dict[str, str] = {}

            def out() -> bool:
                pay, nap = children(path, "w-1"), children(path, "nap-1")
                if pay:
                    found["call"] = pay[0]
                return bool(pay) and bool(nap) and bool(query(path, "SELECT 1 FROM kairo_timer WHERE run = ?", nap[0]))

            await wait_until(out)
            await k.close()

            k = await start("suspend", "unprotected")  # a deploy made pay unprotected
            # Ticks, as a scheduler's, until nap's timer has fired and nap has
            # finished, and pay's call (its lease expired) was found stuck.
            for _ in range(400):
                await k.tick()
                if self.status(path, "nap-1") in DONE and any(EFFECT_WEAKENED in e for e in stuck):
                    break
                await asyncio.sleep(0.025)
            self.assertEqual(await k.run("nap", None, id="nap-1"), "rested", "the other run did not go on")
            self.assertTrue(any(EFFECT_WEAKENED in e for e in stuck), f"stuck: {stuck}")
            self.assertNotIn(self.status(path, found["call"]), DONE, "the call went on")
            await k.resolve_failed(found["call"], "not paid")
            self.assertIn("not paid", await k.run("w", None, id="w-1"))
            await k.close()

        asyncio.run(main())

    def test_a_childs_version_not_here(self) -> None:
        """A child workflow goes on with the version it started with: a process
        that has its parent's version but not the child's leaves both, without
        failing the parent; one with both finishes them (ADR 0060)."""
        path = self.path("child-version.db")
        runs: dict[str, int] = {}

        def add(k: str) -> None:
            runs[k] = runs.get(k, 0) + 1

        async def start(*child_versions: str) -> Kairo:
            k = await self.open(path, lease_ms=100)

            @k.workflow("parent")
            async def parent(ctx, _):
                add("parent")
                return await ctx.workflow("child")

            for i, v in enumerate(child_versions):

                async def child(ctx, _, v=v):
                    add(f"child {v}")
                    return f"{v}:{await ctx.wait_for('go')}"

                k.workflow("child", version=v, draining=i < len(child_versions) - 1)(child)
            await k.start()
            return k

        async def main() -> None:
            k = await start("1")
            await k.submit("parent", None, id="fam-1")
            found: list[str] = []

            def child_runs() -> bool:
                kids = children(path, "fam-1")
                if kids and runs.get("child 1") == 1:
                    found.append(kids[0])
                    return True
                return False

            await wait_until(child_runs)
            await k.close()

            k = await start("2")  # the parent's version, not the child's
            await wait_until(lambda: runs.get("parent", 0) >= 2)  # it took the parent up
            self.assertNotIn(self.status(path, "fam-1"), DONE, "the parent ended without its child's version")
            self.assertEqual(runs.get("child 2", 0), 0, "version 2 drove a version 1 child")
            await k.close()

            k = await start("1", "2")
            await k.signal(found[0], "go", "x")
            self.assertEqual(await asyncio.wait_for(k.result("fam-1"), 10), "1:x")
            await k.close()

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()


DSN = os.environ.get("KAIRO_SDK_PG_DSN", "")

try:
    import psycopg_pool  # noqa: F401

    HAS_PSYCOPG = True
except ImportError:
    HAS_PSYCOPG = False


@unittest.skipUnless(HAS_GO and HAS_WASMTIME and HAS_PSYCOPG and DSN, "go, wasmtime, psycopg_pool or KAIRO_SDK_PG_DSN missing")
class DrivingPostgresTest(unittest.TestCase):
    """The same on PostgreSQL: the drive lease's claim and hand-off, signals
    sent ahead, and listing, in its SQL."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-driving-pg-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    def run_pg(self, body) -> None:
        from psycopg_pool import AsyncConnectionPool

        from kairo_sdk.store import PostgresStore

        prefix = f"kd{os.getpid()}_{body.__name__}_"[:30]

        async def main() -> None:
            pool = AsyncConnectionPool(DSN, min_size=1, max_size=8, open=False)
            await pool.open()
            try:

                async def open_(store: Any = None, lease_ms: int = 30_000) -> Kairo:
                    b = await EmbeddedBackend.open(store or PostgresStore(pool, prefix=prefix), wasm=self.wasm, lease_ms=lease_ms)
                    return Kairo(backend=b)

                await body(open_, lambda: PostgresStore(pool, prefix=prefix))
            finally:
                async with pool.connection() as c:
                    for t in ("run", "event", "timer", "lease"):
                        await c.execute(f"DROP TABLE IF EXISTS {prefix}{t}")
                await pool.close()

        asyncio.run(main())

    def test_a_stopped_driver_is_taken_up(self) -> None:
        writes = {"n": 0}

        async def body(open_, pg_store) -> None:
            async def start(store: Any, hang: bool) -> Kairo:
                k = await open_(store, lease_ms=200)

                @k.action("write")
                def write(p, ctx):
                    writes["n"] += 1
                    return f"wrote {p}"

                @k.workflow("w")
                async def w(ctx, p):
                    out = await ctx.call("write", p)
                    if hang:
                        await asyncio.Event().wait()
                    return out + "!"

                await k.start()
                return k

            store = Dying(pg_store())
            k1 = await start(store, True)
            await k1.submit("w", "f", id="taken-pg")
            await wait_until(lambda: writes["n"] == 1)
            await asyncio.sleep(0.1)  # the write's outcome recorded
            store.dead = True
            k2 = await start(pg_store(), False)
            self.assertEqual(await asyncio.wait_for(k2.result("taken-pg"), 10), "wrote f!")
            self.assertEqual(writes["n"], 1)
            await k2.close()
            await k1.close()

        self.run_pg(body)

    def test_signals_ahead_submit_result_and_list(self) -> None:
        gate = threading.Event()

        async def body(open_, pg_store) -> None:
            k = await open_()

            @k.action("gate", effect="unprotected")
            def gate_(_, ctx):
                gate.wait(10)

            @k.workflow("w")
            async def w(ctx, _):
                await ctx.call("gate")
                return [await ctx.wait_for("s"), await ctx.wait_for("s", timeout=60)]

            await k.start()
            for id in ("pg-1", "pg-2"):
                await k.submit("w", None, id=id, meta={"user": id})
            for id in ("pg-1", "pg-2"):
                for p in ("first", "second"):
                    await k.signal(id, "s", f"{id} {p}")
            gate.set()
            for id in ("pg-1", "pg-2"):
                self.assertEqual(await k.result(id), [f"{id} first", f"{id} second"])
            rows = await k.list(workflow="w")
            self.assertEqual([(r["run_id"], r["meta"]) for r in rows], [("pg-1", {"user": "pg-1"}), ("pg-2", {"user": "pg-2"})])
            self.assertEqual([r["run_id"] for r in await k.list(workflow="w", after="pg-1")], ["pg-2"])
            await k.close()

        self.run_pg(body)
