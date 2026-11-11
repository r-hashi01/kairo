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

from kairo_sdk.backend import EmbeddedBackend, HttpBackend
from kairo_sdk.observe import STEP_FINISHED, STEP_UNKNOWN
from kairo_sdk.store import ParentUnavailableError, SQLiteStore
from kairo_sdk.client import KairoError
from kairo_sdk.metrics import Metric, MetricEmitter
from kairo_sdk.workflow import CallError, Cancelled, Kairo, RunSettledError, StoppedError, Suspended, UnknownOutcomeError

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


class CancelBeforeDone:
    """Cancels a workflow durably immediately before its driver sends done."""

    def __init__(self, backend) -> None:
        self.backend = backend
        self.once = False

    def __getattr__(self, name):
        return getattr(self.backend, name)

    async def finish(self, run_id: str, payload) -> None:
        if not self.once:
            self.once = True
            await self.backend.cancel(run_id)
            await self.backend.wait(run_id)
        await self.backend.finish(run_id, payload)


class TickSerializationTest(unittest.TestCase):
    def test_remote_drain_uses_the_backend(self) -> None:
        class Backend:
            def __init__(self) -> None:
                self.drained = False

            async def drain(self) -> None:
                self.drained = True

            async def close(self) -> None:
                pass

        async def main() -> None:
            backend = Backend()
            measured: list[Metric] = []
            ready = threading.Event()

            def metric(value: Metric) -> None:
                measured.append(value)
                if len(measured) == 4:
                    ready.set()

            k = Kairo(backend=backend, metrics=metric)
            await k.drain()
            self.assertTrue(backend.drained)
            self.assertTrue(await asyncio.to_thread(ready.wait, 1))
            self.assertEqual(
                {m.name for m in measured},
                {"kairo_drain_total", "kairo_drain_seconds", "kairo_drain_starting_tasks", "kairo_drain_starting_drives"},
            )
            await k.close()

        asyncio.run(main())

    def test_concurrent_ticks_take_turns(self) -> None:
        class Backend:
            def __init__(self) -> None:
                self.entered = asyncio.Event()
                self.release = asyncio.Event()
                self.active = 0
                self.maximum = 0

            async def tick(self) -> None:
                self.active += 1
                self.maximum = max(self.maximum, self.active)
                self.entered.set()
                await self.release.wait()
                self.active -= 1

            async def idle(self) -> None: pass

            async def next_wake(self): return None

        async def main() -> None:
            backend = Backend()
            k = Kairo(backend=backend, mode="suspend")
            first = asyncio.create_task(k.tick())
            await backend.entered.wait()
            second = asyncio.create_task(k.tick())
            await asyncio.sleep(0)
            self.assertEqual(backend.maximum, 1)
            backend.release.set()
            await asyncio.gather(first, second)
            self.assertEqual(backend.maximum, 1)

        asyncio.run(main())

    def test_unconfirmed_start_is_retried_with_same_id(self) -> None:
        async def main() -> None:
            measured: list[Metric] = []
            ready = threading.Event()

            def metric(value: Metric) -> None:
                measured.append(value)
                if len(measured) == 3:
                    ready.set()

            backend = HttpBackend(metrics=MetricEmitter(metric))
            calls: list[str] = []

            def run(_plan, _input, **opts):
                calls.append(opts["run_id"])
                if len(calls) == 1:
                    raise KairoError(504, "start not confirmed")
                return {"run_id": opts["run_id"], "existing": True}

            backend.client.run = run  # type: ignore[method-assign]
            out = await backend.run("plan", None, run_id="same")
            self.assertTrue(out["existing"])
            self.assertEqual(calls, ["same", "same"])
            self.assertTrue(await asyncio.to_thread(ready.wait, 1))
            self.assertEqual(
                {m.name for m in measured},
                {
                    "kairo_run_start_unconfirmed_total",
                    "kairo_run_start_confirmation_retries_total",
                    "kairo_run_start_confirmation_seconds",
                },
            )
            self.assertTrue(all("run_id" not in m.labels for m in measured))

        asyncio.run(main())

    def test_overloaded_start_is_retried_with_same_id(self) -> None:
        async def main() -> None:
            backend = HttpBackend()
            calls: list[str] = []

            def run(_plan, _input, **opts):
                calls.append(opts["run_id"])
                if len(calls) == 1:
                    raise KairoError(429, "busy", 0)
                return {"run_id": opts["run_id"], "existing": False}

            backend.client.run = run  # type: ignore[method-assign]
            out = await backend.run("plan", None, run_id="same")
            self.assertFalse(out["existing"])
            self.assertEqual(calls, ["same", "same"])

        asyncio.run(main())

    def test_cancelling_unconfirmed_start_stops_retries(self) -> None:
        async def main() -> None:
            backend = HttpBackend(request_timeout=0.25)
            self.assertEqual(backend.client.timeout, 0.25)
            calls = 0
            second = threading.Event()

            def run(_plan, _input, **_opts):
                nonlocal calls
                calls += 1
                if calls == 2:
                    second.set()
                if calls > 2:
                    return {"run_id": "cancelled", "existing": True}
                raise KairoError(504, "start not confirmed")

            backend.client.run = run  # type: ignore[method-assign]
            running = asyncio.create_task(backend.run("plan", None, run_id="cancelled"))
            self.assertTrue(await asyncio.to_thread(second.wait, 1))
            running.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await running
            stopped_at = calls
            await asyncio.sleep(0.1)
            self.assertEqual(calls, stopped_at)
            self.assertTrue((await backend.run("plan", None, run_id="cancelled"))["existing"])
            self.assertEqual(calls, 3)

        asyncio.run(main())

    def test_drain_stops_and_waits_for_unconfirmed_start(self) -> None:
        async def main() -> None:
            backend = HttpBackend()
            entered = threading.Event()

            def run(_plan, _input, **_opts):
                entered.set()
                raise KairoError(504, "start not confirmed")

            backend.client.run = run  # type: ignore[method-assign]
            running = asyncio.create_task(backend.run("plan", None, run_id="drained"))
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            await backend.drain()
            with self.assertRaises(asyncio.CancelledError):
                await running

        asyncio.run(main())


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
        self.unknown_calls: list[str] = []
        self.hang = True
        self.slow_cancelled = threading.Event()

    async def backend(self):
        return None  # kairod (the default)

    async def raw_backend(self):
        return await self.backend() or HttpBackend(self.url, worker=self.sock, concurrency=8)

    async def kairo(self) -> Kairo:
        b = await self.backend()

        def observer(o) -> None:
            if o.kind == STEP_FINISHED and o.status == STEP_UNKNOWN:
                self.unknown_calls.append(o.run_id)

        k = Kairo(self.url, worker=self.sock, concurrency=8, observe=observer) if b is None else Kairo(backend=b, observe=observer)

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

        @k.action("lost")
        def lost(input, ctx):
            raise UnknownOutcomeError("connection lost")

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

        @k.workflow("uncertain")
        async def uncertain(ctx, input):
            try:
                return await ctx.call("lost")
            except CallError as e:
                return f"failed: {e.message}"

        @k.workflow("tree-hold")
        async def tree_hold(ctx, input):
            await asyncio.Event().wait()

        @k.workflow("listed")
        async def listed(ctx, input):
            return input

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

    def test_resolve_unknown_call(self) -> None:
        if type(self) is not WorkflowTest:
            self.skipTest("kairod only")

        async def main() -> None:
            k = await self.kairo()
            key = type(self).__name__
            succeeded = asyncio.ensure_future(k.run("uncertain", None, id=f"py-resolve-ok-{key}"))

            async def blocked() -> bool:
                return bool(self.unknown_calls) and (await k.backend.get(self.unknown_calls[0]))["status"] == "blocked"

            for _ in range(400):
                if await blocked():
                    break
                await asyncio.sleep(0.025)
            else:
                raise TimeoutError
            call = self.unknown_calls.pop(0)
            await k.resolve(call, "confirmed")
            self.assertEqual(await succeeded, "confirmed")
            await k.resolve(call, "confirmed")  # an HTTP retry is idempotent

            failed = asyncio.ensure_future(k.run("uncertain", None, id=f"py-resolve-failed-{key}"))
            for _ in range(400):
                if await blocked():
                    break
                await asyncio.sleep(0.025)
            else:
                raise TimeoutError
            await k.resolve_failed(self.unknown_calls.pop(0), "not paid")
            self.assertIn("not paid", await failed)
            await k.close()

        asyncio.run(main())

    def test_recorded_cancellation_wins_over_local_outcome(self) -> None:
        async def main() -> None:
            async def success(ctx, input):
                return "local value"

            async def failure(ctx, input):
                raise RuntimeError("local failure")

            for outcome, fn in (("success", success), ("failure", failure)):
                k = Kairo(backend=CancelBeforeDone(await self.raw_backend()))
                k.workflow("race")(fn)
                await k.start()
                id = f"py-cancel-before-done-{type(self).__name__}-{outcome}"
                with self.assertRaises(Cancelled):
                    await k.run("race", None, id=id)
                with self.assertRaises(Cancelled):
                    await k.run("race", None, id=id)
                await k.cancel(id)  # repeated cancellation is idempotent
                await k.close()

        asyncio.run(main())

    def test_cancel_reports_a_workflow_that_already_completed(self) -> None:
        async def main() -> None:
            k = Kairo(backend=await self.raw_backend())

            @k.workflow("quick")
            async def quick(ctx, input):
                return "done"

            await k.start()
            id = f"py-already-complete-{type(self).__name__}"
            self.assertEqual(await k.run("quick", None, id=id), "done")
            with self.assertRaises(RunSettledError):
                await k.cancel(id)
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

    def test_external_workflow_belongs_to_parent_tree(self) -> None:
        async def main() -> None:
            key = type(self).__name__
            parent = f"py-tree-parent-{key}"
            child = f"py-tree-child-{key}"
            k = await self.kairo()
            await k.submit("tree-hold", None, id=parent)
            await k.submit("tree-hold", None, id=child, parent=parent)
            self.assertEqual([(r["run_id"], r.get("parent")) for r in await k.children(parent)], [(child, parent)])
            with self.assertRaises(ParentUnavailableError):
                await k.submit("tree-hold", None, id=f"{child}-orphan", parent=f"{parent}-missing")
            await k.cancel(parent)
            self.assertEqual((await k.backend.get(child))["status"], "cancelled")
            await k.close()

        asyncio.run(main())

    def test_lists_roots_with_metadata_and_pagination(self) -> None:
        async def main() -> None:
            key = type(self).__name__
            ids = [f"py-list-{key}-1", f"py-list-{key}-2"]
            k = await self.kairo()
            await k.run("listed", 1, id=ids[0], meta={"owner": "alice"})
            await k.run("listed", 2, id=ids[1], meta={"owner": "bob"})
            rows = await k.list(workflow="listed", status="completed")
            self.assertEqual([r["run_id"] for r in rows], ids)
            self.assertEqual([r["meta"] for r in rows], [{"owner": "alice"}, {"owner": "bob"}])
            self.assertTrue(all(r.get("created_at") is not None and r.get("updated_at") is not None for r in rows))
            page = await k.list(workflow="listed", after=ids[0], limit=1)
            self.assertEqual([r["run_id"] for r in page], [ids[1]])
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
            # It stopped with the process: the workflow goes on in k2.
            with self.assertRaises((StoppedError, asyncio.CancelledError)):
                await first
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

    def test_suspend_over_kairod(self) -> None:
        async def main() -> None:
            if await self.backend() is not None:
                self.skipTest("kairod only")
            wakes: list[int] = []

            async def wake(at: int) -> None:
                wakes.append(at)

            k = Kairo(self.url, worker=self.sock, mode="suspend", wake=wake)
            calls = 0

            @k.action("suspend-upper", effect="unprotected")
            async def upper(value, ctx):
                nonlocal calls
                await asyncio.sleep(0.04)
                calls += 1
                return value.upper()

            @k.workflow("suspend-job")
            async def job(ctx, value):
                out = await ctx.call("suspend-upper", value)
                await ctx.sleep(0.12)
                return out

            await k.start()
            began = time.monotonic()
            with self.assertRaises(Suspended):
                await k.run("suspend-job", "hi", id="python-kairod-suspend")
            self.assertGreaterEqual(time.monotonic() - began, 0.035)
            self.assertEqual(calls, 1)
            self.assertTrue(wakes and wakes[-1] > int(time.time() * 1000))
            deadline = await k.tick()
            self.assertEqual(deadline, wakes[-1])
            await asyncio.sleep(max(0, deadline / 1000 - time.time()) + 0.04)
            for _ in range(50):
                await k.tick()
                try:
                    self.assertEqual(await k.result("python-kairod-suspend"), "HI")
                    break
                except Suspended:
                    await asyncio.sleep(0.01)
            else:
                self.fail("workflow did not finish after its timer")
            self.assertEqual(calls, 1)
            await k.close()

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
