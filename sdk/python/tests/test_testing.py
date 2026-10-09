"""The test env (kairo_sdk.testing.TestEnv): a workflow that sleeps a day and
waits an hour for an approval, tested without waiting; the same test as
sdk/go's kairotest TestEnv. Needs go (to build kairo.wasm from this
repository) and wasmtime."""

from __future__ import annotations

import asyncio
import datetime
import shutil
import tempfile
import unittest

from kairo_sdk.testing import TestEnv
from kairo_sdk.workflow import Suspended, TimedOutError

try:
    from .test_workflow import HAS_GO, HAS_WASMTIME, build_wasm
except ImportError:  # discovered from tests/ as a top-level module
    from test_workflow import HAS_GO, HAS_WASMTIME, build_wasm

HOUR = 3600


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime not found")
class TestEnvTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-testing-py-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_a_day_and_an_hour_without_waiting(self) -> None:
        async def main() -> None:
            async with await TestEnv.open(wasm=self.wasm) as env:
                charged = {"n": 0}

                @env.k.action("charge")
                def charge(o, _ctx):
                    charged["n"] += 1
                    return f"charged {o['id']}"

                @env.k.workflow("refund")
                async def refund(ctx, o):
                    await ctx.sleep(24 * HOUR)
                    try:
                        by = await ctx.wait_for("approve", timeout=HOUR)
                    except TimedOutError:
                        return "not approved"
                    r = await ctx.call("charge", o)
                    return f"{r} by {by}"

                await env.start()
                t0 = env.now()
                self.assertEqual(t0, datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc))

                with self.assertRaises(Suspended):
                    await env.k.run("refund", {"id": "o-1", "amount": 5}, id="r-1")
                await env.advance(23 * HOUR)
                with self.assertRaises(Suspended, msg="an hour early"):
                    await env.k.result("r-1")
                await env.advance(HOUR)
                await env.signal("r-1", "approve", "alice")
                self.assertEqual(await env.k.result("r-1"), "charged o-1 by alice")
                self.assertEqual(charged["n"], 1)
                calls = await env.calls("r-1")
                self.assertEqual(
                    [f"{c.kind} {c.name} {c.status}" for c in calls],
                    ["call kairo.sleep completed", "wait approve completed", "call charge completed"],
                )
                self.assertEqual(calls[2].input, {"id": "o-1", "amount": 5})
                self.assertEqual(calls[1].output, "alice")

                # No approval within the hour.
                with self.assertRaises(Suspended):
                    await env.k.run("refund", {"id": "o-2", "amount": 0}, id="r-2")
                await env.advance(25 * HOUR)
                self.assertEqual(await env.k.result("r-2"), "not approved")
                self.assertEqual(charged["n"], 1)
                self.assertEqual(env.now(), t0 + datetime.timedelta(hours=24 + 25))

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()
