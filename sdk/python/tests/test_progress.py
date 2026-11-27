"""A run's progress (ADR 0088): its tree and each run's recorded events,
read without changing anything; the same as sdk/ts/src/workflow.test.ts's
'progress gives the tree and its recorded attempts'. SQLite (and PostgreSQL
with KAIRO_SDK_PG_DSN); needs go (to build kairo.wasm from this repository)
and wasmtime."""

from __future__ import annotations

import asyncio
import os
import shutil
import sqlite3
import tempfile
import unittest
from collections.abc import Awaitable, Callable
from typing import Any

from kairo_sdk import RunEvent, RunProgress
from kairo_sdk.backend import EmbeddedBackend
from kairo_sdk.client import KairoError
from kairo_sdk.progress import progress_kind
from kairo_sdk.store import SQLiteStore
from kairo_sdk.workflow import Kairo, RetryableError

try:
    from .test_workflow import HAS_GO, HAS_WASMTIME, build_wasm
except ImportError:  # discovered from tests/ as a top-level module
    from test_workflow import HAS_GO, HAS_WASMTIME, build_wasm

#: Every run's row and every event, as the store holds them.
Rows = Callable[[], Awaitable[Any]]


class _NoTree:
    """A store of one's own without tree: it cannot read runs' events."""

    def __init__(self, store: SQLiteStore) -> None:
        self._store = store

    def __getattr__(self, name: str) -> Any:
        if name == "tree":
            raise AttributeError(name)
        return getattr(self._store, name)


async def _progress(test: unittest.TestCase, k: Kairo, id: str, rows: Rows) -> None:
    flaky = {"n": 0}

    @k.action("llm", effect="unprotected")
    def llm(p, ctx):
        return {"a": p["q"]}

    @k.action("flaky", effect="unprotected", max_attempts=3, backoff="1ms")
    def flaky_(p, ctx):
        flaky["n"] += 1
        if flaky["n"] == 1:
            raise RetryableError("not yet")
        return "ok"

    @k.action("write")
    def write(p, ctx):
        return f"wrote {p['path']}"

    @k.workflow("progress")
    async def progress(ctx, _):
        await ctx.call("llm", {"q": "a"})
        await ctx.call("flaky", None)
        await ctx.call("write", {"path": "p"})
        return await ctx.wait_for("approve")

    await k.start()
    out = asyncio.ensure_future(k.run("progress", None, id=id))
    for _ in range(400):
        if any(c["plan"] == "kairo.wait/approve" for c in await k.children(id)):
            break
        await asyncio.sleep(0.025)
    else:
        test.fail("the workflow did not reach its wait")

    before = await rows()
    p = await k.progress(id)
    test.assertIsInstance(p, RunProgress)
    test.assertEqual(p.root, id)
    root = p.runs[0]
    test.assertEqual((root["run_id"], root["kind"], root["name"], root["status"]), (id, "workflow", "progress", "running"))
    test.assertEqual([(r["kind"], r["name"]) for r in p.runs[1:]], [("call", "llm"), ("call", "flaky"), ("call", "write"), ("wait", "approve")])
    # The runs as children gives them, and nothing of the core's.
    test.assertEqual([{k: v for k, v in r.items() if k not in ("kind", "name", "events")} for r in p.runs[1:]], await k.children(id))

    def run(name: str) -> dict[str, Any]:
        return next(r for r in p.runs if r["name"] == name)

    def kinds(name: str) -> list[tuple[str, int | None]]:
        return [(e.kind, e.attempt) for e in run(name)["events"]]

    test.assertEqual(kinds("llm"), [("started", None), ("succeeded", 1)])
    test.assertEqual(kinds("flaky"), [("started", None), ("failed", 1), ("timer", None), ("succeeded", 2)])
    failed = run("flaky")["events"][1]
    test.assertTrue(failed.retryable)
    test.assertFalse(failed.unknown)
    test.assertIn("not yet", failed.error or "")
    test.assertEqual(kinds("write"), [("started", None), ("dispatched", 1), ("succeeded", 1)])
    test.assertEqual(kinds("approve"), [("started", None)])
    for r in p.runs:
        for e in r["events"]:
            test.assertIsInstance(e, RunEvent)
            test.assertGreater(e.at, 0)
            test.assertIsNone(e.signal)
            test.assertIsNone(e.deadline)

    # Reading changed nothing.
    test.assertEqual(await rows(), before)
    test.assertEqual(await k.progress(id), p)

    await k.signal(id, "approve", "yes")
    test.assertEqual(await asyncio.wait_for(out, 10), "yes")
    done = await k.progress(id)
    test.assertEqual(done.runs[0]["status"], "completed")
    test.assertEqual(
        [(e.kind, e.signal) for e in next(r for r in done.runs if r["name"] == "approve")["events"]],
        [("started", None), ("signal", "approve")],
    )
    with test.assertRaises(KairoError) as cm:
        await k.progress(f"{id}-absent")
    test.assertEqual(cm.exception.status, 404)


class ProgressKindTest(unittest.TestCase):
    def test_a_wait_with_a_timeout_is_named_by_its_signal(self) -> None:
        def kind(plan: str, workflow: str | None = None) -> tuple[str, str]:
            return progress_kind({"plan": plan, "workflow": workflow})

        self.assertEqual(kind("kairo.workflow", "orders"), ("workflow", "orders"))
        self.assertEqual(kind("kairo.call/llm"), ("call", "llm"))
        self.assertEqual(kind("kairo.wait/approve"), ("wait", "approve"))
        self.assertEqual(kind("kairo.wait/approve@60000"), ("wait", "approve"))
        self.assertEqual(kind("kairo.wait/a@b"), ("wait", "a@b"))
        self.assertEqual(kind("graph"), ("run", "graph"))


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime missing")
class ProgressTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-progress-py-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_progress_gives_the_tree_and_its_recorded_attempts_and_changes_nothing(self) -> None:
        path = os.path.join(self.dir, "progress.db")

        async def rows() -> Any:
            db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                return (
                    db.execute("SELECT * FROM kairo_run ORDER BY id").fetchall(),
                    db.execute("SELECT * FROM kairo_event ORDER BY run, seq").fetchall(),
                )
            finally:
                db.close()

        async def main() -> None:
            k = Kairo(backend=await EmbeddedBackend.open(SQLiteStore(path), wasm=self.wasm))
            try:
                await _progress(self, k, "py-progress", rows)
            finally:
                await k.close()

        asyncio.run(main())

    def test_a_store_without_tree_cannot_read_progress(self) -> None:
        async def main() -> None:
            k = Kairo(backend=await EmbeddedBackend.open(_NoTree(SQLiteStore(":memory:")), wasm=self.wasm))
            try:
                with self.assertRaises(NotImplementedError):
                    await k.progress("any")
            finally:
                await k.close()

        asyncio.run(main())


DSN = os.environ.get("KAIRO_SDK_PG_DSN", "")

try:
    import psycopg_pool  # noqa: F401

    HAS_PSYCOPG = True
except ImportError:
    HAS_PSYCOPG = False


@unittest.skipUnless(HAS_GO and HAS_WASMTIME and HAS_PSYCOPG and DSN, "go, wasmtime, psycopg_pool or KAIRO_SDK_PG_DSN missing")
class ProgressPostgresTest(unittest.TestCase):
    """The same on PostgreSQL: the tree and its events in one snapshot."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-progress-pg-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_progress_gives_the_tree_and_its_recorded_attempts_and_changes_nothing(self) -> None:
        from psycopg_pool import AsyncConnectionPool

        from kairo_sdk.store import PostgresStore

        prefix = f"kp{os.getpid()}_"

        async def main() -> None:
            pool = AsyncConnectionPool(DSN, min_size=1, max_size=8, open=False)
            await pool.open()

            async def rows() -> Any:
                async with pool.connection() as c:
                    runs = await (await c.execute(f"SELECT * FROM {prefix}run ORDER BY id")).fetchall()
                    events = await (await c.execute(f"SELECT * FROM {prefix}event ORDER BY run, seq")).fetchall()
                return runs, events

            try:
                k = Kairo(backend=await EmbeddedBackend.open(PostgresStore(pool, prefix=prefix), wasm=self.wasm))
                try:
                    await _progress(self, k, "py-progress-pg", rows)
                finally:
                    await k.close()
            finally:
                async with pool.connection() as c:
                    for t in ("run", "counter", "event", "timer", "lease", "rate"):
                        await c.execute(f"DROP TABLE IF EXISTS {prefix}{t}")
                await pool.close()

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()
