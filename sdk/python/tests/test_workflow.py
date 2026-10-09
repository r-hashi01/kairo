"""Workflows written as code (ADR 0049), one suite against each backend:
kairod (built from this repository) and the runtime embedded here with
SQLite (ADR 0051; its WASM core built from this repository, and wasmtime).
Needs go. Also the serverless mode (suspend)."""

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

from kairo_sdk.backend import EmbeddedBackend
from kairo_sdk.store import SQLiteStore
from kairo_sdk.workflow import Cancelled, Kairo, StoppedError, Suspended

try:
    import wasmtime  # noqa: F401

    HAS_WASMTIME = True
except ImportError:
    HAS_WASMTIME = False

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
HAS_GO = shutil.which("go") is not None


def build_wasm(dir: str) -> str:
    """kairo.wasm: the one scripts/check.sh built (KAIRO_WASM), or built here."""
    if os.environ.get("KAIRO_WASM"):
        return os.environ["KAIRO_WASM"]
    wasm = os.path.join(dir, "kairo.wasm")
    env = dict(os.environ, GOOS="wasip1", GOARCH="wasm")
    subprocess.run(["go", "build", "-buildmode=c-shared", "-o", wasm, "./cmd/kairo-wasm"], cwd=REPO, check=True, env=env)
    return wasm


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

    async def backend(self):
        return None  # kairod (the default)

    async def kairo(self) -> Kairo:
        b = await self.backend()
        k = Kairo(self.url, worker=self.sock, concurrency=8) if b is None else Kairo(backend=b)

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

        await k.start()
        return k

    async def wait_until(self, cond) -> None:
        for _ in range(400):
            if cond():
                return
            await asyncio.sleep(0.025)
        raise TimeoutError

    def test_resumed_elsewhere_runs_nothing_twice(self) -> None:
        async def main() -> None:
            k1 = await self.kairo()
            first = asyncio.ensure_future(k1.run("edit", {"files": ["a", "b", "c"]}, id="py-edit-1"))
            await self.wait_until(lambda: self.runs["write"] == 1)
            self.assertEqual(self.runs["llm"], 3)
            await k1.close()  # the first process "stops"
            with self.assertRaises(StoppedError):  # not cancelled: it goes on elsewhere (ADR 0059)
                await first

            self.hang = False
            k2 = await self.kairo()
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
            k = await self.kairo()
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
            k = await self.kairo()
            done = asyncio.ensure_future(k.run("long", None, id="py-long-1"))
            await self.wait_until(lambda: self.runs["slow"] == 1)
            await k.cancel("py-long-1")
            with self.assertRaises(Cancelled):
                await done
            await asyncio.to_thread(self.slow_cancelled.wait, 10)
            self.assertTrue(self.slow_cancelled.is_set())
            await k.close()

        asyncio.run(main())

    def test_a_run_goes_on_with_the_version_it_started_with(self) -> None:
        """ADR 0060; through kairod too, which gives a run's input back when
        asked (GetInput)."""
        seen: list[str] = []

        async def versioned(*versions: str) -> Kairo:
            b = await self.backend()
            k = Kairo(self.url, worker=self.sock, concurrency=8) if b is None else Kairo(backend=b)

            @k.action("mark", effect="unprotected")
            def mark(v, ctx):
                seen.append(v)
                return v

            for i, v in enumerate(versions):

                async def ver(ctx, _, v=v):
                    await ctx.call("mark", v)
                    return f"{v}:{await ctx.wait_for('go')}"

                k.workflow("ver", version=v, draining=i < len(versions) - 1)(ver)
            await k.start()
            return k

        async def main() -> None:
            id = f"wf-ver-{type(self).__name__}"
            k1 = await versioned("1")
            first = asyncio.ensure_future(k1.run("ver", None, id=id))
            for _ in range(400):
                if "1" in seen:
                    break
                await asyncio.sleep(0.025)
            await k1.close()
            first.cancel()
            k2 = await versioned("1", "2")
            out = asyncio.ensure_future(k2.run("ver", None, id=id))
            for _ in range(400):
                try:
                    await k2.signal(id, "go", "x")
                    break
                except LookupError:
                    await asyncio.sleep(0.025)
            self.assertEqual(await asyncio.wait_for(out, 30), "1:x")
            self.assertEqual(seen, ["1"], "version 2 did not run it, and its call ran once")
            await k2.close()

        asyncio.run(main())


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime not found")
class EmbeddedWorkflowTest(WorkflowTest):
    """The same suite on the runtime embedded here, with SQLite (one file: a
    new backend on it is a new process)."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.wasm = build_wasm(cls.dir)
        cls.db = os.path.join(cls.dir, "embedded.db")

    async def backend(self):
        return await EmbeddedBackend.open(SQLiteStore(self.db), wasm=self.wasm)


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime not found")
class ServerlessTest(unittest.TestCase):
    """Suspend mode: each invocation is a new process that goes as far as it
    can and returns; a scheduler's tick and a signal drive the workflow on."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-sdk-py-sl-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_across_invocations(self) -> None:
        path = os.path.join(self.dir, "serverless.db")
        runs = {"llm": 0, "write": 0}

        async def invocation() -> Kairo:
            k = Kairo(backend=await EmbeddedBackend.open(SQLiteStore(path), wasm=self.wasm), mode="suspend")

            @k.action("llm", effect="unprotected")
            def llm(i, ctx):
                runs["llm"] += 1
                return i["q"].upper()

            @k.action("write")
            def write(i, ctx):
                runs["write"] += 1
                return f"wrote {i['v']}"

            @k.workflow("job")
            async def job(ctx, inp):
                a = await ctx.call("llm", {"q": inp["q"]})
                await ctx.sleep(0.3)
                w = await ctx.call("write", {"v": a})
                ok = await ctx.wait_for("approve")
                return {"a": a, "w": w, "by": ok["by"]}

            await k.start()
            return k

        async def main() -> None:
            k = await invocation()
            with self.assertRaises(Suspended):
                await k.run("job", {"q": "hi"}, id="sl-1")
            self.assertEqual(runs, {"llm": 1, "write": 0})
            await k.close()

            await asyncio.sleep(0.35)
            k = await invocation()
            await k.tick()
            self.assertEqual(runs, {"llm": 1, "write": 1})
            await k.close()

            k = await invocation()
            await k.signal("sl-1", "approve", {"by": "alice"})
            self.assertEqual(await k.run("job", {"q": "hi"}, id="sl-1"), {"a": "HI", "w": "wrote HI", "by": "alice"})
            self.assertEqual(runs, {"llm": 1, "write": 1}, "no call ran twice")
            await k.close()

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()
