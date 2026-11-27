"""A process's liveness, where the store can tell it (ADR 0086): a mark the
process holds while it lives, which the database or the OS takes away when it
ends. Another process takes a gone owner's leases up without waiting for them
to expire. "Gone" is said only on proof (the mark could be taken); on any
doubt, the leases expire by time, as before. The same marks as the
TypeScript and Go SDKs', so that processes of each language see each other."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from typing import Any, Protocol


class Liveness(Protocol):
    async def hold(self, owner: str) -> None: ...
    async def gone(self, owner: str) -> bool: ...
    def lost(self) -> bool: ...
    async def close(self) -> None: ...


class SQLiteLiveness:
    """SQLite: a lock file of the owner's, beside the database, held in an
    exclusive transaction for the process's life. SQLite's lock is the OS's,
    which it takes away when the process ends."""

    def __init__(self, db_path: str) -> None:
        self._dir = db_path + ".owners"
        self._held: sqlite3.Connection | None = None
        self._path = ""

    def _file(self, owner: str) -> str:
        return os.path.join(self._dir, hashlib.sha256(owner.encode()).hexdigest()[:32] + ".lock")

    async def hold(self, owner: str) -> None:
        os.makedirs(self._dir, mode=0o700, exist_ok=True)
        path = self._file(owner)
        db = sqlite3.connect(path, timeout=0, isolation_level=None, check_same_thread=False)
        db.execute("BEGIN EXCLUSIVE")
        self._held, self._path = db, path

    async def gone(self, owner: str) -> bool:
        path = self._file(owner)
        # No mark: an owner that never held one (an older SDK), not proof.
        if not os.path.exists(path):
            return False
        db = None
        try:
            db = sqlite3.connect(path, timeout=0, isolation_level=None)
            db.execute("BEGIN EXCLUSIVE")  # fails while its owner lives
            db.execute("ROLLBACK")
            return True
        except sqlite3.Error:
            return False
        finally:
            if db is not None:
                db.close()

    def lost(self) -> bool:
        return False  # a process that ends loses it with its life

    async def close(self) -> None:
        if self._held is None:
            return
        # Its own file goes with it; one of a process that stopped stays, as a
        # gone owner's (another's is never removed: an owner of that name may
        # be taking it again).
        try:
            os.remove(self._path)
        except OSError:
            pass
        self._held.close()
        self._held = None


_KEY = "hashtextextended('kairo.owner:' || %s, 0)"


class PgLiveness:
    """PostgreSQL: a session advisory lock of the owner's, on a connection of
    its own, which the server takes away when the session ends. The key is
    the server's hash of the owner. The same connection tries others' locks
    (and gives back one it took)."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self._cm: Any = None
        self._conn: Any = None
        self._owner = ""
        self._broken = False

    async def hold(self, owner: str) -> None:
        cm = self._pool.connection()
        conn = await cm.__aenter__()
        try:
            await conn.set_autocommit(True)
            await conn.execute(f"SELECT pg_advisory_lock({_KEY})", (owner,))
        except BaseException:
            await cm.__aexit__(None, None, None)
            raise
        self._cm, self._conn, self._owner = cm, conn, owner

    async def gone(self, owner: str) -> bool:
        conn = self._conn
        if conn is None or self._broken or owner == self._owner:
            return False
        try:
            # Advisory locks nest in a session: one with our own key (a hash
            # collision) would be taken here, so it is never taken as gone.
            cur = await conn.execute(
                f"SELECT {_KEY} = {_KEY} AS same, pg_try_advisory_lock({_KEY}) AS took",
                (owner, self._owner, owner),
            )
            same, took = await cur.fetchone()
            if took:
                await conn.execute(f"SELECT pg_advisory_unlock({_KEY})", (owner,))
            return bool(took) and not same
        except Exception:
            self._broken = True
            return False

    def lost(self) -> bool:
        return self._broken

    async def close(self) -> None:
        conn, cm = self._conn, self._cm
        self._conn = self._cm = None
        if conn is None:
            return
        try:
            if not self._broken:
                await conn.execute(f"SELECT pg_advisory_unlock({_KEY})", (self._owner,))
            await conn.set_autocommit(False)
        except Exception:
            pass
        await cm.__aexit__(None, None, None)
