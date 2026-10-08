"""Waking the runtime that does not stay up (ADR 0053): a scheduler's /tick,
and wake(at) for one-off wake-ups instead of polling. Needs go and wasmtime."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
import time
import unittest
import urllib.request
from typing import Any

from kairo_worker.backend import EmbeddedBackend
from kairo_worker.store import LeaseRow, SQLiteStore, TimerRow, Changes
from kairo_worker.workflow import Kairo, Suspended, tick_handler

from test_http import run, serve
from test_workflow import HAS_GO, HAS_WASMTIME, build_wasm


def get(url: str, method: str = "GET", token: str | None = None) -> tuple[int, Any]:
    req = urllib.request.Request(url, method=method, data=b"" if method == "POST" else None)
    if token is not None:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        with e:
            return e.code, None


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime not found")
class ScheduleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-sdk-py-schedule-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    async def embedded(self, name: str) -> EmbeddedBackend:
        return await EmbeddedBackend.open(SQLiteStore(os.path.join(self.dir, name)), wasm=self.wasm)

    def test_tick_over_http(self) -> None:
        async def main() -> None:
            wakes: list[int] = []

            async def wake(at: int) -> None:
                wakes.append(at)

            k = Kairo(backend=await self.embedded("tick.db"), mode="suspend", tick_secret="cron-secret", wake=wake)
            k.action("llm", effect="unprotected")(lambda q, ctx: q.upper())

            @k.workflow("w")
            async def w(ctx, q):
                await ctx.sleep(0.3)
                return await ctx.call("llm", q)

            await k.start()
            base, _ = await serve(k.asgi_app())
            tick = base + "/kairo/tick"

            t0 = int(time.time() * 1000)
            with self.assertRaises(Suspended):
                await k.run("w", "hi", id="tick-1")
            self.assertEqual(len(wakes), 1, "told when to wake")
            self.assertTrue(t0 + 300 <= wakes[0] < t0 + 2000, f"wake at {wakes[0] - t0}ms")

            self.assertEqual((await asyncio.to_thread(get, tick))[0], 401, "no token")
            self.assertEqual((await asyncio.to_thread(get, tick, "GET", "nope"))[0], 401, "a wrong token")
            # Too early: nothing to do; the same time comes back.
            self.assertEqual(await asyncio.to_thread(get, tick, "GET", "cron-secret"), (200, {"next": wakes[0]}))

            await asyncio.sleep(max(0, wakes[0] - time.time() * 1000) / 1000 + 0.02)
            self.assertEqual(await asyncio.to_thread(get, tick, "POST", "cron-secret"), (200, {"next": None}), "nothing left")
            self.assertEqual(await k.run("w", "hi", id="tick-1"), "HI")
            await k.close()

            # Without tick_secret, /tick is not served.
            k2 = Kairo(backend=await self.embedded("tick.db"), mode="suspend")
            await k2.start()
            base2, _ = await serve(k2.asgi_app())
            self.assertEqual((await asyncio.to_thread(get, base2 + "/kairo/tick", "GET", "cron-secret"))[0], 404)
            await k2.close()

        run(main)

    def test_next_wake(self) -> None:
        async def main() -> None:
            store = SQLiteStore(os.path.join(self.dir, "next.db"))
            await store.init()
            self.assertIsNone(await store.next_wake())
            await store.with_run("r", lambda _: Changes(None, set_timers=[TimerRow("r", 1, 1, 5000)]))
            self.assertEqual(await store.next_wake(), 5000)
            await store.with_run("r", lambda _: Changes(None, set_leases=[LeaseRow("r", 2, 1, "x", 4000)]))
            self.assertEqual(await store.next_wake(), 4000)
            await store.with_run("r", lambda _: Changes(None, end_leases=[2]))
            self.assertEqual(await store.next_wake(), 5000)
            await store.close()

        run(main)

    def test_wake_drives_to_the_end(self) -> None:
        """Each tick a new process, called only at the times given: no polling."""

        async def main() -> None:
            runs = {"llm": 0}
            ticks = 0
            due: list[asyncio.Future[Any]] = []

            async def wake(at: int) -> None:
                # The "one-off scheduler": calls the function at the time given.
                async def later() -> None:
                    nonlocal ticks
                    await asyncio.sleep(max(0, at - time.time() * 1000) / 1000)
                    ticks += 1
                    await tick()

                due.append(asyncio.ensure_future(later()))

            async def make() -> Kairo:
                k = Kairo(backend=await self.embedded("wake.db"), mode="suspend", wake=wake)

                @k.action("llm", effect="unprotected")
                def llm(q, ctx):
                    runs["llm"] += 1
                    return q.upper()

                @k.workflow("w")
                async def w(ctx, _):
                    await ctx.sleep(0.15)
                    a = await ctx.call("llm", "a")
                    await ctx.sleep(0.15)
                    return a + await ctx.call("llm", "b")

                await k.start()
                return k

            tick = tick_handler(make)
            k = await make()
            with self.assertRaises(Suspended):
                await k.run("w", None, id="wake-1")
            await k.close()
            while due:
                await due.pop(0)

            k = await make()
            self.assertEqual(await k.run("w", None, id="wake-1"), "AB")
            await k.close()
            self.assertEqual(runs["llm"], 2)
            # Woken at most once per sleep (no polling). Under load a tick can
            # last until the next sleep is due, and its process goes on with it.
            self.assertIn(ticks, (1, 2), "woken at most once per sleep")

        run(main)


if __name__ == "__main__":
    unittest.main()
