"""Initializing one database from several processes at once: the look at
the columns of a table made before them, and the adding of them, are not
split by another process's initialization."""

from __future__ import annotations

import asyncio
import os
import shutil
import sqlite3
import tempfile
import time
import unittest
from typing import Any
from unittest import mock

from kairo_sdk import store as store_module
from kairo_sdk.store import SQLiteStore

ADDED = ["parent", "workflow", "meta", "created_order"]


def old_schema(p: str, blob: str) -> str:
    """A run table from before the columns added later."""
    return f"""CREATE TABLE {p}run (id TEXT PRIMARY KEY, plan TEXT NOT NULL, hash TEXT NOT NULL, state {blob} NOT NULL,
        input TEXT, status TEXT NOT NULL, output TEXT, error TEXT, seq INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)"""


def run_columns(path: str) -> list[str]:
    db = sqlite3.connect(path)
    try:
        return [c[1] for c in db.execute("PRAGMA table_info(kairo_run)").fetchall()]
    finally:
        db.close()


class SQLiteInitTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="kairo-sdk-store-init-")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_another_process_cannot_come_between_the_look_and_the_change(self) -> None:
        """Another process's initialization comes between this one's look at
        the columns and its adding them, the moment the two would both add
        the same column (duplicate column name). The look and the change are
        one transaction holding the write lock: the other process cannot
        change the schema in between (told the database is locked, with no
        wait here), and both initializations succeed."""
        path = os.path.join(self.dir, "race.db")
        old = sqlite3.connect(path)
        old.execute(old_schema("kairo_", "BLOB"))
        old.close()

        connect = sqlite3.connect
        interleaved: list[BaseException | None] = []

        class Hooked(sqlite3.Connection):
            def execute(self, sql: str, *args: Any) -> sqlite3.Cursor:  # type: ignore[override]
                if not interleaved and sql.startswith("ALTER TABLE kairo_run ADD COLUMN parent"):
                    # Between the look and the change: what another process's
                    # initialization does, without waiting for a lock.
                    other = connect(path, timeout=0, isolation_level=None)
                    try:
                        other.execute("ALTER TABLE kairo_run ADD COLUMN parent TEXT")
                        interleaved.append(None)
                    except sqlite3.Error as e:
                        interleaved.append(e)
                    finally:
                        other.close()
                return super().execute(sql, *args)

        async def main() -> None:
            a = SQLiteStore(path)
            with mock.patch.object(store_module.sqlite3, "connect", lambda *a, **k: connect(*a, factory=Hooked, **k)):
                await a.init()
            self.assertEqual(len(interleaved), 1, "the column was not added")
            self.assertIsNotNone(interleaved[0], "another process changed the schema between the look and the change")
            self.assertRegex(str(interleaved[0]), "locked|busy")
            # The other process, initializing after: nothing left to add.
            b = SQLiteStore(path)
            await b.init()
            cols = run_columns(path)
            for c in ADDED:
                self.assertIn(c, cols)
            await a.close()
            await b.close()

        asyncio.run(main())

    def test_older_table_gets_the_added_columns(self) -> None:
        path = os.path.join(self.dir, "old.db")
        old = sqlite3.connect(path)
        old.execute(old_schema("kairo_", "BLOB"))
        old.execute(
            "INSERT INTO kairo_run (id, plan, hash, state, status, seq, created_at, updated_at) VALUES ('old', 'p', 'h', x'00', 'running', 0, 1, 1)"
        )
        old.commit()
        old.close()

        async def main() -> None:
            s = SQLiteStore(path)
            await s.init()
            r = await s.get("old")
            assert r is not None
            self.assertEqual(r.status, "running")
            self.assertIsNone(r.parent)
            self.assertEqual(r.created_order, 0)
            cols = run_columns(path)
            for c in ADDED:
                self.assertIn(c, cols)
            await s.close()

        asyncio.run(main())


DSN = os.environ.get("KAIRO_SDK_PG_DSN", "")
try:
    import psycopg_pool  # noqa: F401

    HAS_PSYCOPG = True
except ImportError:
    HAS_PSYCOPG = False

KEY = "hashtextextended('kairo.schema:' || %s::text, 0)"


@unittest.skipUnless(HAS_PSYCOPG and DSN, "psycopg_pool or KAIRO_SDK_PG_DSN missing")
class PostgresInitTest(unittest.TestCase):
    def test_initialization_takes_the_schema_lock(self) -> None:
        """Two sessions making the same table at once can both find it
        missing, so initialization takes an advisory lock of the prefix's
        first. While another session holds it, initialization waits for it
        (seen in pg_locks), and goes on once it is let go. Several
        initializations at once, of a table from before the added columns,
        all succeed."""
        from psycopg_pool import AsyncConnectionPool

        from kairo_sdk.store import PostgresStore

        prefix = f"kpyinit{os.getpid()}_"

        async def main() -> None:
            async with AsyncConnectionPool(DSN, min_size=1, max_size=8, kwargs={"autocommit": True}) as pool:
                async def drop() -> None:
                    async with pool.connection() as conn:
                        for t in ("run", "counter", "event", "timer", "lease", "rate"):
                            await conn.execute(f"DROP TABLE IF EXISTS {prefix}{t}")

                await drop()
                try:
                    async with pool.connection() as conn:
                        await conn.execute(old_schema(prefix, "BYTEA"))
                    async with pool.connection() as holder:
                        await holder.execute(f"SELECT pg_advisory_lock({KEY})", (prefix,))
                        init = asyncio.ensure_future(PostgresStore(pool, prefix=prefix).init())
                        deadline = time.monotonic() + 10
                        while True:
                            async with pool.connection() as conn:
                                cur = await conn.execute(
                                    f"""SELECT count(*) FROM pg_locks l, (SELECT {KEY} AS k) h
                                    WHERE l.locktype = 'advisory' AND NOT l.granted AND l.objsubid = 1
                                    AND l.classid::bigint = ((h.k >> 32) & 4294967295) AND l.objid::bigint = (h.k & 4294967295)""",
                                    (prefix,),
                                )
                                (n,) = await cur.fetchone()  # type: ignore[misc]
                            if n > 0:
                                break
                            self.assertFalse(init.done(), "initialization did not wait for the schema lock")
                            self.assertLess(time.monotonic(), deadline, "initialization never asked for the schema lock")
                            await asyncio.sleep(0.005)
                        await holder.execute(f"SELECT pg_advisory_unlock({KEY})", (prefix,))
                    await init

                    await asyncio.gather(*(PostgresStore(pool, prefix=prefix).init() for _ in range(4)))
                    async with pool.connection() as conn:
                        cur = await conn.execute(
                            "SELECT count(*) FROM information_schema.columns WHERE table_name = %s AND column_name = ANY(%s)",
                            (prefix + "run", ADDED),
                        )
                        (cols,) = await cur.fetchone()  # type: ignore[misc]
                    self.assertEqual(cols, len(ADDED))
                finally:
                    await drop()

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()
