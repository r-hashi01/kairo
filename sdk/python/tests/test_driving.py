"""The embedded runtime's gaps filled (ADR 0059): drive leases, signals
before their waits, limits, wait timeouts, listing and submitting; the same
behaviour as sdk/go/kairotest and sdk/ts/src/driving.test.ts. SQLite;
needs go (to build kairo.wasm from this repository) and wasmtime."""

from __future__ import annotations

import asyncio
import datetime
import os
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
from typing import Any

from kairo_sdk.backend import EmbeddedBackend
from kairo_sdk.store import SQLiteStore
from kairo_sdk.workflow import Cancelled, Kairo, StoppedError, TimedOutError

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
            with self.assertRaises(Cancelled):
                await res
            await wait_until(stopped.is_set)
            kids = children(path, "tree-1")
            self.assertEqual(len(kids), 1)
            await wait_until(lambda: self.status(path, kids[0]) == "cancelled")
            await k.close()

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
            self.assertEqual(await k.run("w", None, id="limits-1"), 15)
            self.assertLessEqual(seen["most"], 2, f"{seen['most']} steps of slow at once")
            starts.sort()
            for a, b in zip(starts, starts[1:]):
                self.assertGreaterEqual(b - a, 0.09, f"paced started {(b - a) * 1000:.0f}ms apart")
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
