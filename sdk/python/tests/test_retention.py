"""Finished runs removed by trees, after the time they are kept (ADR 0054).
The clock of the runtime is moved on; the database is looked at directly.
Needs go and wasmtime."""

from __future__ import annotations

import asyncio
import os
import shutil
import sqlite3
import tempfile
import time
import unittest

from kairo_sdk.backend import EmbeddedBackend
from kairo_sdk.embedded import Embedded, keep_finished_ms
from kairo_sdk.protocol import Result
from kairo_sdk.store import SQLiteStore
from kairo_sdk.workflow import Kairo, Suspended

from test_workflow import HAS_GO, HAS_WASMTIME, build_wasm

HOUR = 3600 * 1000


def rows(path: str, prefix: str) -> dict[str, int]:
    """Rows of each table for runs whose id starts with prefix."""
    db = sqlite3.connect(path)
    try:
        n = lambda t, col: db.execute(f"SELECT COUNT(*) FROM kairo_{t} WHERE {col} LIKE ?", (prefix + "%",)).fetchone()[0]  # noqa: E731
        return {"run": n("run", "id"), "event": n("event", "run"), "timer": n("timer", "run"), "lease": n("lease", "run")}
    finally:
        db.close()


class Clock:
    def __init__(self) -> None:
        self.skew = 0

    def __call__(self) -> int:
        return int(time.time() * 1000) + self.skew


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime not found")
class RetentionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-sdk-py-retention-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_tree_removed_once_kept_long_enough(self) -> None:
        """A finished workflow goes with its calls; a removed id starts anew."""
        path = os.path.join(self.dir, "tree.db")
        clock = Clock()
        runs = {"llm": 0}

        async def main() -> None:
            backend = await EmbeddedBackend.open(SQLiteStore(path), wasm=self.wasm, now=clock, keep_finished=HOUR)
            k = Kairo(backend=backend)

            @k.action("llm", effect="unprotected")
            def llm(q, ctx):
                runs["llm"] += 1
                return q.upper()

            @k.workflow("w")
            async def w(ctx, _):
                return await ctx.call("llm", "a") + await ctx.workflow("child", "b")

            @k.workflow("child")
            async def child(ctx, q):
                return await ctx.call("llm", q)

            await k.start()
            self.assertEqual(await k.run("w", None, id="tree-1"), "AB")
            before = rows(path, "tree-1")
            self.assertEqual(before["run"], 4, "the workflow, its call, the child workflow and its call")

            clock.skew = HOUR // 2
            await k.tick()
            self.assertEqual(rows(path, "tree-1"), before, "kept for the time given")

            clock.skew = HOUR + 1000
            await k.tick()
            self.assertEqual(rows(path, "tree-1"), {"run": 0, "event": 0, "timer": 0, "lease": 0}, "removed as one tree")

            self.assertEqual(await k.run("w", None, id="tree-1"), "AB")
            self.assertEqual(runs["llm"], 4, "the removed id is a new run")
            await k.close()

        asyncio.run(main())

    def test_running_workflow_keeps_its_calls(self) -> None:
        path = os.path.join(self.dir, "running.db")
        clock = Clock()
        runs = {"write": 0}

        async def kairo() -> Kairo:
            backend = await EmbeddedBackend.open(SQLiteStore(path), wasm=self.wasm, now=clock, keep_finished=HOUR)
            k = Kairo(backend=backend, mode="suspend")

            @k.action("write")
            def write(v, ctx):
                runs["write"] += 1
                return f"wrote {v}"

            @k.workflow("w")
            async def w(ctx, _):
                wrote = await ctx.call("write", "x")
                ok = await ctx.wait_for("approve")
                return f"{wrote} for {ok['by']}"

            await k.start()
            return k

        async def main() -> None:
            k = await kairo()
            with self.assertRaises(Suspended):
                await k.run("w", None, id="long-1")
            await k.close()

            # Days later: the workflow still waits; its finished call is kept.
            clock.skew = 72 * HOUR
            k = await kairo()
            await k.tick()
            self.assertEqual(rows(path, "long-1")["run"], 3, "the workflow, its call, its wait")
            await k.signal("long-1", "approve", {"by": "alice"})
            self.assertEqual(await k.run("w", None, id="long-1"), "wrote x for alice")
            self.assertEqual(runs["write"], 1, "the real call did not run again")

            clock.skew = 73 * HOUR + 1000
            await k.tick()
            self.assertEqual(rows(path, "long-1")["run"], 0)
            await k.close()

        asyncio.run(main())

    def test_unfinished_under_finished_root(self) -> None:
        """A finished root goes with what under it has not finished, timers and leases too."""
        path = os.path.join(self.dir, "unfinished.db")
        clock = Clock()

        async def main() -> None:
            rt = await Embedded(SQLiteStore(path), wasm=self.wasm, now=clock, keep_finished=HOUR).open()
            rt.register_actions(
                [{"action": "slow", "effect": "unprotected", "timeout": "100h"}],
                lambda task, ctx: Result(pending={"owner": "remote:x", "lease_ms": 100 * HOUR}),
            )
            rt.register_plan({"name": "wait", "root": {"kind": "wait", "id": "w", "signal": "go"}})
            rt.register_plan({"name": "step", "root": {"kind": "step", "id": "s", "action": "slow"}})
            await rt.run("wait", None, run_id="u-1")
            await rt.run("wait", None, run_id="u-1/waits", parent="u-1")
            await rt.run("step", None, run_id="u-1/waits/step", parent="u-1/waits")
            await rt.idle()
            await rt.signal("u-1", "go", None)
            self.assertEqual((await rt.get("u-1"))["status"], "completed")
            r = rows(path, "u-1")
            self.assertEqual((r["run"], r["timer"], r["lease"]), (3, 1, 1))

            clock.skew = HOUR + 1000
            await rt.tick()
            self.assertEqual(rows(path, "u-1"), {"run": 0, "event": 0, "timer": 0, "lease": 0})
            await rt.complete("u-1/waits/step", 1, 1, Result(output="late"))  # does nothing
            self.assertEqual(rows(path, "u-1")["run"], 0)
            await rt.close()

        asyncio.run(main())

    def test_alone_limit_and_older_tables(self) -> None:
        path = os.path.join(self.dir, "alone.db")
        old = sqlite3.connect(path)
        old.execute(
            """CREATE TABLE kairo_run (id TEXT PRIMARY KEY, plan TEXT NOT NULL, hash TEXT NOT NULL, state BLOB NOT NULL,
            input TEXT, status TEXT NOT NULL, output TEXT, error TEXT, seq INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)"""
        )
        old.close()
        clock = Clock()

        async def main() -> None:
            store = SQLiteStore(path)
            rt = await Embedded(store, wasm=self.wasm, now=clock, keep_finished=None).open()
            rt.register_plan({"name": "wait", "root": {"kind": "wait", "id": "w", "signal": "go"}})
            for id in ("a-1", "a-2", "a-3"):
                await rt.run("wait", None, run_id=id)
                await rt.signal(id, "go", None)
            await rt.run("wait", None, run_id="a-running")
            clock.skew = HOUR
            await rt.tick()
            self.assertEqual(rows(path, "a-")["run"], 4, "None keeps them")

            cutoff = clock()
            self.assertEqual(await store.remove_finished(cutoff, 2), 2)
            self.assertEqual(await store.remove_finished(cutoff, 2), 1)
            self.assertEqual(await store.remove_finished(cutoff, 2), 0)
            self.assertEqual(rows(path, "a-")["run"], 1, "the running one stays")
            await rt.close()

        asyncio.run(main())

    def test_keep_finished(self) -> None:
        self.assertEqual(keep_finished_ms("env", ""), 24 * HOUR)
        self.assertEqual(keep_finished_ms("env", "7d"), 7 * 24 * HOUR)
        self.assertEqual(keep_finished_ms("env", "30m"), 30 * 60 * 1000)
        self.assertIsNone(keep_finished_ms("env", "forever"))
        self.assertEqual(keep_finished_ms(5000, "7d"), 5000, "the option first")
        with self.assertRaisesRegex(ValueError, "KAIRO_KEEP_FINISHED"):
            keep_finished_ms("env", "a week")


if __name__ == "__main__":
    unittest.main()
