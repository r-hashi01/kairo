"""Where the embedded runtime keeps runs (ADR 0051): per run its state (the
core's encoding), its input and its events (appended only), the timers
armed and the leases of the steps running. Applying an event is one
transaction: lock the run's row, then append the events, replace the state,
arm or disarm timers, take or end leases. The tables are those of the
TypeScript SDK (sdk/ts/src/store.ts)."""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar

T = TypeVar("T")


@dataclass
class RunRow:
    id: str
    plan: str
    hash: str
    state: bytes
    input: str | None  # JSON
    status: str
    output: str | None  # JSON
    error: str | None
    seq: int  # events recorded so far
    created_at: int
    updated_at: int
    parent: str | None = None  # the run that made this one: removed with it (ADR 0054)


@dataclass
class TimerRow:
    run: str
    timer: int
    act: int
    at: int


@dataclass
class LeaseRow:
    run: str
    act: int
    attempt: int
    owner: str
    until: int


@dataclass
class Changes(Generic[T]):
    """What a transaction on a run writes; computed from the row as read."""

    result: T
    events: list[dict[str, Any]] = field(default_factory=list)
    row: RunRow | None = None
    set_timers: list[TimerRow] = field(default_factory=list)
    delete_timers: list[int] = field(default_factory=list)
    clear: bool = False  # the run ended: drop its timers and leases
    set_leases: list[LeaseRow] = field(default_factory=list)
    end_leases: list[int] = field(default_factory=list)
    notify: bool = False  # the run settled: tell the listeners at commit


class Store(Protocol):
    async def init(self) -> None: ...
    async def with_run(self, id: str, fn: Callable[[RunRow | None], Changes[T]]) -> T: ...
    async def get(self, id: str) -> RunRow | None: ...
    async def due_timers(self, now: int, limit: int) -> list[TimerRow]: ...
    async def expired_leases(self, now: int, limit: int) -> list[LeaseRow]: ...
    async def renew_leases(self, owner: str, until: int) -> None: ...
    async def expire_leases(self, owner: str, now: int) -> None: ...
    async def remove_finished(self, cutoff: int, limit: int) -> int:
        """Removes finished trees of runs (ADR 0054): a run with no parent (or
        whose parent is gone) that finished before cutoff, with every run under
        it, finished or not, and their events, timers and leases. One
        transaction per tree; at most limit trees. Returns how many."""
        ...
    async def next_wake(self) -> int | None:
        """When something is next to do (ADR 0053): the earliest timer, or the
        earliest lease to expire; None if neither."""
        ...
    async def hand_over(self, lease: LeaseRow) -> None:
        """Hands a step's lease to lease.owner until lease.until (ADR 0052: the
        step runs elsewhere). Nothing if the lease is gone: its outcome is in."""
        ...
    async def close(self) -> None: ...


def _earliest(*ts: Any) -> int | None:
    """The earliest of times as the database gives them (None for none)."""
    vs = [int(t) for t in ts if t is not None]
    return min(vs) if vs else None


def _prefix(p: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,30}", p):
        raise ValueError(f"table prefix {p}: letters, digits and _ only")
    return p


def _ddl(p: str, blob: str, big: str) -> list[str]:
    return [
        f"""CREATE TABLE IF NOT EXISTS {p}run (id TEXT PRIMARY KEY, plan TEXT NOT NULL, hash TEXT NOT NULL, state {blob} NOT NULL,
          input TEXT, status TEXT NOT NULL, output TEXT, error TEXT, seq {big} NOT NULL, created_at {big} NOT NULL, updated_at {big} NOT NULL)""",
        f"CREATE TABLE IF NOT EXISTS {p}event (run TEXT NOT NULL, seq {big} NOT NULL, body TEXT NOT NULL, PRIMARY KEY (run, seq))",
        f"CREATE TABLE IF NOT EXISTS {p}timer (run TEXT NOT NULL, timer {big} NOT NULL, act {big} NOT NULL, at {big} NOT NULL, PRIMARY KEY (run, timer))",
        f"CREATE INDEX IF NOT EXISTS {p}timer_at ON {p}timer (at)",
        f"""CREATE TABLE IF NOT EXISTS {p}lease (run TEXT NOT NULL, act {big} NOT NULL, attempt {big} NOT NULL, owner TEXT NOT NULL,
          until {big} NOT NULL, PRIMARY KEY (run, act))""",
        f"CREATE INDEX IF NOT EXISTS {p}lease_until ON {p}lease (until)",
        f"CREATE INDEX IF NOT EXISTS {p}lease_owner ON {p}lease (owner)",
    ]


_DONE_SQL = "('completed', 'failed', 'cancelled')"


def _ddl_removal(p: str) -> list[str]:
    """After the tables (and the parent column, added to older ones): the indexes for removal (ADR 0054)."""
    return [
        f"CREATE INDEX IF NOT EXISTS {p}run_parent ON {p}run (parent)",
        f"CREATE INDEX IF NOT EXISTS {p}run_done ON {p}run (updated_at) WHERE status IN {_DONE_SQL}",
    ]


def _roots(p: str, ph: str) -> str:
    """Roots of finished trees: no parent, or its parent gone (oldest first)."""
    return (f"SELECT id FROM {p}run r WHERE status IN {_DONE_SQL} AND updated_at < {ph} "
            f"AND (parent IS NULL OR NOT EXISTS (SELECT 1 FROM {p}run x WHERE x.id = r.parent)) ORDER BY updated_at LIMIT {ph}")


def _is_root(p: str, ph: str) -> str:
    """Whether a run (first parameter) is (still) the root of a finished tree."""
    return (f"SELECT 1 FROM {p}run r WHERE id = {ph} AND status IN {_DONE_SQL} AND updated_at < {ph} "
            f"AND (parent IS NULL OR NOT EXISTS (SELECT 1 FROM {p}run x WHERE x.id = r.parent))")


def _tree(p: str, ph: str) -> str:
    """The ids of a tree: its root and every run under it."""
    return f"WITH RECURSIVE tree(id) AS (SELECT {ph} UNION SELECT r.id FROM {p}run r JOIN tree t ON r.parent = t.id) SELECT id FROM tree"


def _removals(p: str, ph: str) -> list[str]:
    """A tree's rows: the runs (which the tree is found by) last."""
    return [f"DELETE FROM {p}{t} WHERE run IN ({_tree(p, ph)})" for t in ("event", "timer", "lease")] + [
        f"DELETE FROM {p}run WHERE id IN ({_tree(p, ph)})"
    ]


_RUN_COLS = "id, plan, hash, state, input, status, output, error, seq, created_at, updated_at, parent"


def _row(r: Any) -> RunRow | None:
    if r is None:
        return None
    return RunRow(r[0], r[1], r[2], bytes(r[3]), r[4], r[5], r[6], r[7], int(r[8]), int(r[9]), int(r[10]), r[11])


class SQLiteStore:
    """SQLite (the standard library's sqlite3): a file, or ":memory:" (tests).

    One connection, used from the event loop's thread; a transaction runs
    without awaiting, so nothing else runs on the connection meanwhile.
    """

    def __init__(self, path: str, *, prefix: str = "kairo_", synchronous: str = "FULL") -> None:
        self.path = path
        self.p = _prefix(prefix)
        self.sync = synchronous
        self.db: sqlite3.Connection | None = None

    async def init(self) -> None:
        self.db = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        if self.path != ":memory:":
            self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(f"PRAGMA synchronous={self.sync}")
        for q in _ddl(self.p, "BLOB", "INTEGER"):
            self.db.execute(q)
        cols = [c[1] for c in self.db.execute(f"PRAGMA table_info({self.p}run)").fetchall()]
        if "parent" not in cols:
            self.db.execute(f"ALTER TABLE {self.p}run ADD COLUMN parent TEXT")
        for q in _ddl_removal(self.p):
            self.db.execute(q)

    async def remove_finished(self, cutoff: int, limit: int) -> int:
        db, p = self.db, self.p
        assert db is not None
        n = 0
        for (id,) in db.execute(_roots(p, "?"), (cutoff, limit)).fetchall():
            db.execute("BEGIN IMMEDIATE")
            try:
                # Still a finished root (not resubmitted, not removed by another process)?
                if db.execute(_is_root(p, "?"), (id, cutoff)).fetchone():
                    for q in _removals(p, "?"):
                        db.execute(q, (id,))
                    n += 1
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise
        return n

    async def with_run(self, id: str, fn: Callable[[RunRow | None], Changes[T]]) -> T:
        db, p = self.db, self.p
        assert db is not None
        db.execute("BEGIN IMMEDIATE")
        try:
            before = _row(db.execute(f"SELECT {_RUN_COLS} FROM {p}run WHERE id = ?", (id,)).fetchone())
            c = fn(before)
            seq = before.seq if before else 0
            for ev in c.events:
                db.execute(f"INSERT INTO {p}event (run, seq, body) VALUES (?, ?, ?)", (id, seq, json.dumps(ev)))
                seq += 1
            if c.row is not None:
                r = c.row
                db.execute(
                    f"""INSERT INTO {p}run ({_RUN_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET
                    state = excluded.state, status = excluded.status, output = excluded.output, error = excluded.error,
                    seq = excluded.seq, updated_at = excluded.updated_at""",
                    (r.id, r.plan, r.hash, r.state, r.input, r.status, r.output, r.error, seq, r.created_at, r.updated_at, r.parent),
                )
            if c.clear:
                db.execute(f"DELETE FROM {p}timer WHERE run = ?", (id,))
                db.execute(f"DELETE FROM {p}lease WHERE run = ?", (id,))
            for t in c.delete_timers:
                db.execute(f"DELETE FROM {p}timer WHERE run = ? AND timer = ?", (id, t))
            for t in c.set_timers:
                db.execute(
                    f"INSERT INTO {p}timer (run, timer, act, at) VALUES (?, ?, ?, ?) ON CONFLICT (run, timer) DO UPDATE SET at = excluded.at",
                    (t.run, t.timer, t.act, t.at),
                )
            for a in c.end_leases:
                db.execute(f"DELETE FROM {p}lease WHERE run = ? AND act = ?", (id, a))
            for lease in c.set_leases:
                db.execute(
                    f"""INSERT INTO {p}lease (run, act, attempt, owner, until) VALUES (?, ?, ?, ?, ?) ON CONFLICT (run, act) DO UPDATE SET
                    attempt = excluded.attempt, owner = excluded.owner, until = excluded.until""",
                    (lease.run, lease.act, lease.attempt, lease.owner, lease.until),
                )
            db.execute("COMMIT")
            return c.result
        except BaseException:
            db.execute("ROLLBACK")
            raise

    async def get(self, id: str) -> RunRow | None:
        assert self.db is not None
        return _row(self.db.execute(f"SELECT {_RUN_COLS} FROM {self.p}run WHERE id = ?", (id,)).fetchone())

    async def due_timers(self, now: int, limit: int) -> list[TimerRow]:
        assert self.db is not None
        rows = self.db.execute(f"SELECT run, timer, act, at FROM {self.p}timer WHERE at <= ? ORDER BY at LIMIT ?", (now, limit)).fetchall()
        return [TimerRow(r[0], int(r[1]), int(r[2]), int(r[3])) for r in rows]

    async def expired_leases(self, now: int, limit: int) -> list[LeaseRow]:
        assert self.db is not None
        rows = self.db.execute(
            f"SELECT run, act, attempt, owner, until FROM {self.p}lease WHERE until < ? ORDER BY until LIMIT ?", (now, limit)
        ).fetchall()
        return [LeaseRow(r[0], int(r[1]), int(r[2]), r[3], int(r[4])) for r in rows]

    async def renew_leases(self, owner: str, until: int) -> None:
        assert self.db is not None
        self.db.execute(f"UPDATE {self.p}lease SET until = ? WHERE owner = ?", (until, owner))

    async def expire_leases(self, owner: str, now: int) -> None:
        assert self.db is not None
        self.db.execute(f"UPDATE {self.p}lease SET until = ? WHERE owner = ?", (now - 1, owner))

    async def next_wake(self) -> int | None:
        assert self.db is not None
        t = self.db.execute(f"SELECT MIN(at) FROM {self.p}timer").fetchone()[0]
        l = self.db.execute(f"SELECT MIN(until) FROM {self.p}lease").fetchone()[0]
        return _earliest(t, l)

    async def hand_over(self, lease: LeaseRow) -> None:
        assert self.db is not None
        self.db.execute(
            f"UPDATE {self.p}lease SET owner = ?, until = ? WHERE run = ? AND act = ? AND attempt = ?",
            (lease.owner, lease.until, lease.run, lease.act, lease.attempt),
        )

    async def close(self) -> None:
        if self.db is not None:
            self.db.close()


class PostgresStore:
    """PostgreSQL through an async pool the application gives (psycopg's
    psycopg_pool.AsyncConnectionPool fits); no dependency here."""

    def __init__(self, pool: Any, *, prefix: str = "kairo_") -> None:
        self.pool = pool
        self.p = _prefix(prefix)

    async def _exec(self, q: str, params: tuple[Any, ...] = ()) -> list[Any]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(q, params)
            return await cur.fetchall() if cur.description else []

    async def init(self) -> None:
        for q in _ddl(self.p, "BYTEA", "BIGINT"):
            await self._exec(q)
        await self._exec(f"ALTER TABLE {self.p}run ADD COLUMN IF NOT EXISTS parent TEXT")
        for q in _ddl_removal(self.p):
            await self._exec(q)

    async def remove_finished(self, cutoff: int, limit: int) -> int:
        p = self.p
        n = 0
        for (id,) in await self._exec(_roots(p, "%s"), (cutoff, limit)):
            async with self.pool.connection() as conn:
                async with conn.transaction():
                    # Lock the root, then check it is still a finished root (not
                    # resubmitted, not removed by another process).
                    await conn.execute(f"SELECT 1 FROM {p}run WHERE id = %s FOR UPDATE", (id,))
                    cur = await conn.execute(_is_root(p, "%s"), (id, cutoff))
                    if await cur.fetchone():
                        for q in _removals(p, "%s::text"):
                            await conn.execute(q, (id,))
                        n += 1
        return n

    async def with_run(self, id: str, fn: Callable[[RunRow | None], Changes[T]]) -> T:
        p = self.p
        async with self.pool.connection() as conn:
            async with conn.transaction():
                cur = await conn.execute(f"SELECT {_RUN_COLS} FROM {p}run WHERE id = %s FOR UPDATE", (id,))
                before = _row(await cur.fetchone())
                c = fn(before)
                seq = before.seq if before else 0
                for ev in c.events:
                    await conn.execute(f"INSERT INTO {p}event (run, seq, body) VALUES (%s, %s, %s)", (id, seq, json.dumps(ev)))
                    seq += 1
                if c.row is not None:
                    r = c.row
                    await conn.execute(
                        f"""INSERT INTO {p}run ({_RUN_COLS}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET
                        state = excluded.state, status = excluded.status, output = excluded.output, error = excluded.error,
                        seq = excluded.seq, updated_at = excluded.updated_at""",
                        (r.id, r.plan, r.hash, r.state, r.input, r.status, r.output, r.error, seq, r.created_at, r.updated_at, r.parent),
                    )
                if c.clear:
                    await conn.execute(f"DELETE FROM {p}timer WHERE run = %s", (id,))
                    await conn.execute(f"DELETE FROM {p}lease WHERE run = %s", (id,))
                for t in c.delete_timers:
                    await conn.execute(f"DELETE FROM {p}timer WHERE run = %s AND timer = %s", (id, t))
                for t in c.set_timers:
                    await conn.execute(
                        f"INSERT INTO {p}timer (run, timer, act, at) VALUES (%s, %s, %s, %s) ON CONFLICT (run, timer) DO UPDATE SET at = excluded.at",
                        (t.run, t.timer, t.act, t.at),
                    )
                for a in c.end_leases:
                    await conn.execute(f"DELETE FROM {p}lease WHERE run = %s AND act = %s", (id, a))
                for lease in c.set_leases:
                    await conn.execute(
                        f"""INSERT INTO {p}lease (run, act, attempt, owner, until) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (run, act) DO UPDATE SET
                        attempt = excluded.attempt, owner = excluded.owner, until = excluded.until""",
                        (lease.run, lease.act, lease.attempt, lease.owner, lease.until),
                    )
                if c.notify:
                    await conn.execute("SELECT pg_notify(%s, %s)", (f"{p}settled", id))
        return c.result

    async def get(self, id: str) -> RunRow | None:
        rows = await self._exec(f"SELECT {_RUN_COLS} FROM {self.p}run WHERE id = %s", (id,))
        return _row(rows[0]) if rows else None

    async def due_timers(self, now: int, limit: int) -> list[TimerRow]:
        rows = await self._exec(f"SELECT run, timer, act, at FROM {self.p}timer WHERE at <= %s ORDER BY at LIMIT %s", (now, limit))
        return [TimerRow(r[0], int(r[1]), int(r[2]), int(r[3])) for r in rows]

    async def expired_leases(self, now: int, limit: int) -> list[LeaseRow]:
        rows = await self._exec(f"SELECT run, act, attempt, owner, until FROM {self.p}lease WHERE until < %s ORDER BY until LIMIT %s", (now, limit))
        return [LeaseRow(r[0], int(r[1]), int(r[2]), r[3], int(r[4])) for r in rows]

    async def renew_leases(self, owner: str, until: int) -> None:
        await self._exec(f"UPDATE {self.p}lease SET until = %s WHERE owner = %s", (until, owner))

    async def expire_leases(self, owner: str, now: int) -> None:
        await self._exec(f"UPDATE {self.p}lease SET until = %s WHERE owner = %s", (now - 1, owner))

    async def next_wake(self) -> int | None:
        t = await self._exec(f"SELECT MIN(at) FROM {self.p}timer")
        l = await self._exec(f"SELECT MIN(until) FROM {self.p}lease")
        return _earliest(t[0][0] if t else None, l[0][0] if l else None)

    async def hand_over(self, lease: LeaseRow) -> None:
        await self._exec(
            f"UPDATE {self.p}lease SET owner = %s, until = %s WHERE run = %s AND act = %s AND attempt = %s",
            (lease.owner, lease.until, lease.run, lease.act, lease.attempt),
        )

    async def listen(self, settled: Callable[[str], None]) -> Callable[[], Any]:
        """Keeps one connection to LISTEN for runs settled in any process; returns how to stop."""
        conn = await self.pool.getconn()
        await conn.set_autocommit(True)
        await conn.execute(f"LISTEN {self.p}settled")

        async def read() -> None:
            async for n in conn.notifies():
                if n.channel == f"{self.p}settled" and n.payload:
                    settled(n.payload)

        task = asyncio.ensure_future(read())

        async def stop() -> None:
            task.cancel()
            try:
                await conn.execute(f"UNLISTEN {self.p}settled")
            except Exception:
                pass
            await self.pool.putconn(conn)

        return stop

    async def close(self) -> None:
        pass
