from __future__ import annotations

import asyncio
import os
import tempfile
import unittest

from kairo_sdk.store import Changes, SQLiteStore


class SQLiteNotifyTest(unittest.TestCase):
    def test_notification_is_published_after_commit(self) -> None:
        async def main() -> None:
            with tempfile.TemporaryDirectory(prefix="kairo-notify-") as directory:
                path = os.path.join(directory, "kairo.db")
                a = SQLiteStore(path, notify=True)
                b = SQLiteStore(path, notify=True)
                await a.init()
                await b.init()
                stop_a = await a.listen(lambda _: None)  # type: ignore[attr-defined]
                loop = asyncio.get_running_loop()
                heard: asyncio.Future[str] = loop.create_future()
                stop_b = await b.listen(lambda run_id: heard.set_result(run_id))  # type: ignore[attr-defined]
                try:
                    await a.with_run("n-1", lambda _: Changes(None, notify=True))
                    self.assertEqual(await asyncio.wait_for(heard, 2), "n-1")
                finally:
                    await stop_b()
                    await stop_a()
                    await a.close()
                    await b.close()

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()
