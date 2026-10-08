"""The embedded runtime keeps kairo's invariants with a database (ADR 0051).
Needs go (to build kairo.wasm from this repository) and wasmtime."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest

from kairo_sdk.embedded import Embedded
from kairo_sdk.protocol import Result
from kairo_sdk.store import SQLiteStore

try:
    from .test_workflow import HAS_GO, HAS_WASMTIME, build_wasm
except ImportError:  # discovered from tests/ as a top-level module
    from test_workflow import HAS_GO, HAS_WASMTIME, build_wasm


def events(path: str, run: str) -> list[dict]:
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return [json.loads(r[0]) for r in db.execute("SELECT body FROM kairo_event WHERE run = ? ORDER BY seq", (run,))]
    finally:
        db.close()


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime not found")
class EmbeddedTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-embedded-py-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    async def open(self, name: str, **kw) -> Embedded:
        return await Embedded(SQLiteStore(os.path.join(self.dir, name)), wasm=self.wasm, **kw).open()

    def test_intent_committed_first_and_ignored_not_recorded(self) -> None:
        path = os.path.join(self.dir, "inv.db")

        async def main() -> None:
            rt = await self.open("inv.db")
            seen: list[dict] = []
            act = {}

            def send(task, ctx):
                # What another process would find in the database now.
                act["n"] = task.act
                seen.extend(events(path, "r1"))
                return Result(output={"sent": True})

            rt.register_actions([{"action": "send", "effect": "real"}], send)
            rt.register_plan({"name": "p", "root": {"kind": "seq", "nodes": [
                {"kind": "step", "id": "s", "action": "send"}, {"kind": "wait", "id": "w", "signal": "go"}]}})
            await rt.run("p", None, run_id="r1")
            self.assertTrue((await rt.run("p", None, run_id="r1"))["existing"], "a run id is an idempotency key")
            await rt.idle()
            await rt.signal("r1", "go", 1)
            self.assertEqual((await rt.wait("r1"))["status"], "completed")
            self.assertTrue(any(e["kind"] == "intent" and e["act"] == act["n"] for e in seen), f"intent before the step: {seen}")
            n = len(events(path, "r1"))
            await rt.signal("r1", "go", 2)  # the run is over: ignored
            self.assertEqual(len(events(path, "r1")), n, "an ignored event is not recorded")
            await rt.close()

        asyncio.run(main())

    def test_steps_of_a_stopped_process_are_taken_up(self) -> None:
        specs = [{"action": "think", "effect": "unprotected"}, {"action": "send", "effect": "real"}]

        def plans(rt: Embedded) -> None:
            rt.register_plan({"name": "u", "root": {"kind": "step", "id": "think", "action": "think"}})
            rt.register_plan({"name": "r", "root": {"kind": "step", "id": "send", "action": "send"}})

        async def main() -> None:
            a = await self.open("lease.db", lease_ms=200)
            started = threading.Semaphore(0)

            def hang(task, ctx):
                started.release()
                ctx.cancelled.wait(10)
                return Result(output="never recorded")

            a.register_actions(specs, hang)
            plans(a)
            await a.run("u", None, run_id="u1")
            await a.run("r", None, run_id="r1")
            for _ in range(2):
                await asyncio.to_thread(started.acquire, True, 5)
            await a.close()

            b = await self.open("lease.db", lease_ms=200)
            ran: list[str] = []

            def done(task, ctx):
                ran.append(task.action)
                return Result(output=f"{task.action} done")

            b.register_actions(specs, done)
            plans(b)
            await b.tick()
            self.assertEqual(ran, [], "not taken up before the lease expired")
            await asyncio.sleep(0.25)
            await b.tick()
            self.assertEqual((await b.wait("u1"))["status"], "completed", "the unprotected step ran again")
            self.assertEqual((await b.wait("r1"))["status"], "blocked", "the real step stops for review")
            self.assertEqual(ran, ["think"], "the real step did not run twice")
            await b.close()

            # C stops with a step running; D comes back as C at once.
            c = await self.open("lease.db", lease_ms=60_000, owner="host-1")
            c_started = threading.Event()

            def hang2(task, ctx):
                c_started.set()
                ctx.cancelled.wait(10)
                return Result(output=None)

            c.register_actions(specs, hang2)
            plans(c)
            await c.run("u", None, run_id="u2")
            await asyncio.to_thread(c_started.wait, 5)
            await c.close()
            d = await self.open("lease.db", lease_ms=60_000, owner="host-1")
            d.register_actions(specs, lambda task, ctx: Result(output="again"))
            plans(d)
            await d.tick()
            u2 = await d.wait("u2")
            self.assertEqual((u2["status"], u2["output"]), ("completed", "again"))
            await d.close()

        asyncio.run(main())

    def test_a_running_step_is_not_taken_up_while_its_process_lives(self) -> None:
        specs = [{"action": "think", "effect": "unprotected"}]
        plan = {"name": "u", "root": {"kind": "step", "id": "think", "action": "think"}}

        async def main() -> None:
            a = await self.open("renew.db", lease_ms=300)
            a.register_actions(specs, lambda task, ctx: (ctx.cancelled.wait(0.9), Result(output="a"))[1])
            a.register_plan(plan)
            b = await self.open("renew.db", lease_ms=300)
            ran_in_b: list[str] = []

            def other(task, ctx):
                ran_in_b.append(task.action)
                return Result(output="b")

            b.register_actions(specs, other)
            b.register_plan(plan)
            await a.run("u", None, run_id="long1")
            for _ in range(6):
                await asyncio.sleep(0.15)
                await b.tick()
            self.assertEqual((await a.wait("long1"))["output"], "a")
            self.assertEqual(ran_in_b, [], "b took up a step that a was running")
            await a.close()
            await b.close()

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
class PostgresTest(unittest.TestCase):
    """With PostgreSQL: the store keeps the invariants, and a run settled in
    one process wakes a wait in another (LISTEN/NOTIFY)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-embedded-pg-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_settled_elsewhere_and_recovery(self) -> None:
        from psycopg_pool import AsyncConnectionPool

        from kairo_sdk.store import PostgresStore

        prefix = f"kp{os.getpid()}_"

        async def main() -> None:
            pool = AsyncConnectionPool(DSN, min_size=1, max_size=8, open=False)
            await pool.open()
            try:
                plan = {"name": "p", "root": {"kind": "wait", "id": "w", "signal": "go"}}
                a = await Embedded(PostgresStore(pool, prefix=prefix), wasm=self.wasm).open()
                b = await Embedded(PostgresStore(pool, prefix=prefix), wasm=self.wasm).open()
                for rt in (a, b):
                    rt.register_actions([], lambda task, ctx: Result(output=None))
                    rt.register_plan(plan)
                await a.run("p", None, run_id="x1")
                waiting = asyncio.ensure_future(a.wait("x1"))
                await asyncio.sleep(0.05)
                await b.signal("x1", "go", "from b")
                r = await asyncio.wait_for(waiting, 5)
                self.assertEqual((r["status"], r["output"]), ("completed", {"timed_out": False, "payload": "from b"}))

                # A step of a process that stopped, taken up by another.
                specs = [{"action": "think", "effect": "unprotected"}]
                c = await Embedded(PostgresStore(pool, prefix=prefix), wasm=self.wasm, lease_ms=200).open()
                started = threading.Event()

                def hang(task, ctx):
                    started.set()
                    ctx.cancelled.wait(10)
                    return Result(output="never")

                c.register_actions(specs, hang)
                c.register_plan({"name": "u", "root": {"kind": "step", "id": "t", "action": "think"}})
                await c.run("u", None, run_id="u1")
                await asyncio.to_thread(started.wait, 5)
                await c.close()
                b.register_actions(specs, lambda task, ctx: Result(output="b"))
                b.register_plan({"name": "u", "root": {"kind": "step", "id": "t", "action": "think"}})
                await asyncio.sleep(0.25)
                await b.tick()
                self.assertEqual((await b.wait("u1"))["output"], "b")
                await a.close()
                await b.close()
            finally:
                async with pool.connection() as conn:
                    for t in ("run", "event", "timer", "lease"):
                        await conn.execute(f"DROP TABLE IF EXISTS {prefix}{t}")
                await pool.close()

        asyncio.run(main())
