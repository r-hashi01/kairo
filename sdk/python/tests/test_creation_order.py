import unittest

from kairo_sdk.store import Changes, RunRow, SQLiteStore


class CreationOrderTest(unittest.IsolatedAsyncioTestCase):
    async def test_same_millisecond_uses_store_creation_order(self) -> None:
        store = SQLiteStore(":memory:")
        await store.init()

        async def put(id: str, parent: str | None, workflow: str | None) -> None:
            row = RunRow(id, "p", "h", b"x", None, "running", None, None, 0, 42, 42, parent, workflow, None)
            await store.with_run(id, lambda _: Changes(None, row=row))

        await put("z-root", None, "orders")
        await put("a-root", None, "orders")
        await put("z-child", "parent", None)
        await put("a-child", "parent", None)
        self.assertEqual([r.id for r in await store.list(workflow="orders", after="z-root")], ["a-root"])
        self.assertEqual([r.id for r in await store.children("parent")], ["z-child", "a-child"])
        await store.close()


if __name__ == "__main__":
    unittest.main()
