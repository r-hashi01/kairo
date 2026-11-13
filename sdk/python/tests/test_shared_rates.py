import os
import tempfile
import unittest

from kairo_sdk.store import Changes, SharedRateRequest, SharedRateSettlement, SQLiteStore
from kairo_sdk.workflow import Kairo


class SharedRateTest(unittest.IsolatedAsyncioTestCase):
    async def test_shared_limits_require_capable_embedded_store(self) -> None:
        kairo = Kairo()

        @kairo.action("shared", effect="unprotected", rate=60, shared_limits=True)
        def shared(_input, _ctx):
            return None

        with self.assertRaisesRegex(ValueError, "embedded runtime with a shared-rate Store"):
            await kairo.start()

    async def test_sqlite_shares_request_weight_and_retry_after(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "rate.db")
            a, b = SQLiteStore(path), SQLiteStore(path)
            await a.init()
            await b.init()
            try:
                self.assertEqual(await a.admit_destination(SharedRateRequest("provider", 600)), 0)
                self.assertGreater(await b.admit_destination(SharedRateRequest("provider", 600)), 0)

                await a.with_run("missing", lambda _: Changes(None, shared_rates=[SharedRateSettlement("weighted", 0, 60, 11)]))
                self.assertGreaterEqual(await b.admit_destination(SharedRateRequest("weighted", 0, 60)), 900)

                await a.with_run("missing", lambda _: Changes(None, shared_rates=[SharedRateSettlement("paused", retry_after_ms=1000)]))
                self.assertGreaterEqual(await b.admit_destination(SharedRateRequest("paused")), 900)
            finally:
                await a.close()
                await b.close()


if __name__ == "__main__":
    unittest.main()
