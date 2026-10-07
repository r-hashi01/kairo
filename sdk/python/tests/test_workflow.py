"""Workflows written as code (ADR 0049), against a real kairod built from
this repository (needs go)."""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.request

from kairo_worker.workflow import Cancelled, Kairo

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
HAS_GO = shutil.which("go") is not None


@unittest.skipUnless(HAS_GO, "go not found")
class WorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-sdk-py-")
        bin = os.path.join(cls.dir, "kairod")
        subprocess.run(["go", "build", "-o", bin, "./cmd/kairod"], cwd=REPO, check=True)
        port = 19420 + os.getpid() % 1000
        cls.url = f"http://127.0.0.1:{port}"
        cls.sock = os.path.join(cls.dir, "worker.sock")
        cls.kairod = subprocess.Popen(
            [bin, "-data", os.path.join(cls.dir, "data"), "-http", f"127.0.0.1:{port}", "-socket", cls.sock, "-nosync"],
            stderr=subprocess.DEVNULL,
        )
        for _ in range(200):
            try:
                urllib.request.urlopen(cls.url + "/v1/stats", timeout=1)
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("kairod did not start")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.kairod.terminate()
        cls.kairod.wait()
        shutil.rmtree(cls.dir, ignore_errors=True)

    def setUp(self) -> None:
        self.runs = {"write": 0, "llm": 0, "slow": 0}
        self.hang = True
        self.slow_cancelled = threading.Event()

    def kairo(self) -> Kairo:
        k = Kairo(self.url, worker=self.sock, concurrency=8)

        @k.action("write")
        def write(input, ctx):
            self.runs["write"] += 1
            return {"wrote": input["path"]}

        @k.action("llm", effect="unprotected")
        async def llm(input, ctx):
            self.runs["llm"] += 1
            return {"answer": input["q"].upper()}

        @k.action("slow")
        def slow(input, ctx):
            self.runs["slow"] += 1
            ctx.cancelled.wait(30)
            self.slow_cancelled.set()
            return None

        @k.workflow("edit")
        async def edit(ctx, input):
            answers = await asyncio.gather(*(ctx.call("llm", {"q": f}) for f in input["files"]))
            w = await ctx.call("write", {"path": input["files"][0]})
            if self.hang:
                await asyncio.Event().wait()  # as a process that stopped here
            again = await ctx.call("llm", {"q": "done"})
            return {"answers": list(answers), "w": w, "again": again}

        @k.workflow("timed")
        async def timed(ctx, input):
            t0 = await ctx.now()
            await ctx.sleep(0.3)
            t1 = await ctx.now()
            approval = await ctx.wait_for("approve")
            return {"slept": t1 - t0, "by": approval["by"]}

        @k.workflow("long")
        async def long(ctx, input):
            return await ctx.call("slow")

        @k.workflow("parent")
        async def parent(ctx, n):
            return await ctx.workflow("child", n)

        @k.workflow("child")
        async def child(ctx, n):
            return (await ctx.call("llm", {"q": f"c{n}"}))["answer"]

        return k

    async def wait_until(self, cond) -> None:
        for _ in range(400):
            if cond():
                return
            await asyncio.sleep(0.025)
        raise TimeoutError

    def test_resumed_elsewhere_runs_nothing_twice(self) -> None:
        async def main() -> None:
            k1 = self.kairo()
            await k1.start()
            first = asyncio.ensure_future(k1.run("edit", {"files": ["a", "b", "c"]}, id="py-edit-1"))
            await self.wait_until(lambda: self.runs["write"] == 1)
            self.assertEqual(self.runs["llm"], 3)
            await k1.close()  # the first process "stops"
            with self.assertRaises(asyncio.CancelledError):
                await first

            self.hang = False
            k2 = self.kairo()
            await k2.start()
            out = await k2.run("edit", {"files": ["a", "b", "c"]}, id="py-edit-1")
            self.assertEqual(
                out,
                {"answers": [{"answer": "A"}, {"answer": "B"}, {"answer": "C"}], "w": {"wrote": "a"}, "again": {"answer": "DONE"}},
            )
            self.assertEqual(self.runs["write"], 1, "the real action ran once")
            self.assertEqual(self.runs["llm"], 4)
            self.assertEqual(await k2.run("edit", {"files": ["a", "b", "c"]}, id="py-edit-1"), out)
            self.assertEqual(self.runs["llm"], 4)
            self.assertEqual(await k2.run("parent", 7, id="py-parent-1"), "C7")
            await k2.close()

        asyncio.run(main())

    def test_sleep_and_signal(self) -> None:
        async def main() -> None:
            k = self.kairo()
            await k.start()
            done = asyncio.ensure_future(k.run("timed", None, id="py-timed-1"))
            for _ in range(400):
                try:
                    await k.signal("py-timed-1", "approve", {"by": "alice"})
                    break
                except LookupError:
                    await asyncio.sleep(0.025)
            out = await done
            self.assertGreaterEqual(out["slept"], 300)
            self.assertEqual(out["by"], "alice")
            await k.close()

        asyncio.run(main())

    def test_cancel(self) -> None:
        async def main() -> None:
            k = self.kairo()
            await k.start()
            done = asyncio.ensure_future(k.run("long", None, id="py-long-1"))
            await self.wait_until(lambda: self.runs["slow"] == 1)
            await k.cancel("py-long-1")
            with self.assertRaises(Cancelled):
                await done
            await asyncio.to_thread(self.slow_cancelled.wait, 10)
            self.assertTrue(self.slow_cancelled.is_set())
            await k.close()

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()
