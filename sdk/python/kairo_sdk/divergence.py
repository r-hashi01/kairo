"""A workflow's replay that leaves its record is stopped before it makes a
call with an effect (ADR 0084). Calls are matched with the record by what
they are, not where (ADR 0049): a function that is not deterministic makes
another call on replay, and one that acts outside would act again."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from .client import finished


class DivergedError(Exception):
    """A workflow's replay left its record: it was about to make a call with
    an effect while a recorded call with an effect was never reached (ADR
    0084). Nothing was made; the workflow is not failed, and goes on once
    code that replays as it first ran drives it."""

    def __init__(self, workflow: str, call: str, unreached: list[str]) -> None:
        super().__init__(
            f"workflow {workflow} left its record: it was about to make {call}, "
            f"but has not reached {', '.join(unreached)} (ADR 0084)"
        )
        self.workflow = workflow
        self.call = call
        self.unreached = unreached


# How many turns of the event loop with nothing done in the drive count as
# "it can go no further", where the loop cannot say that nothing else is
# ready to run (an event loop other than asyncio's own).
_QUIET_TURNS = 32


class Divergence:
    """One drive of a workflow. A new call with an effect is held while a
    recorded call with an effect is not reached; it is judged once the
    function can go no further without it: the calls still out are the held
    ones, and those that cannot lead to the record (recorded and unfinished,
    or new waits), and nothing else is ready to run in the event loop."""

    def __init__(self, workflow: str, children: Callable[[], Awaitable[list[dict[str, Any]]]],
                 has_effect: Callable[[dict[str, Any]], bool]) -> None:
        self._workflow = workflow
        self._children = children
        self._has_effect = has_effect
        self._reached: set[str] = set()
        self._out: dict[str, str] = {}  # call -> "effect", "call" or "wait"
        self._recorded: dict[str, tuple[bool, bool]] | None = None  # call -> (effect, finished)
        self._loading: asyncio.Future[None] | None = None
        self._held: list[asyncio.Future[None]] = []
        self._held_call = ""
        self._scheduled = False
        self._activity = 0
        self.err: DivergedError | None = None

    def check(self) -> None:
        """Raises the drive's divergence, once found, even if the workflow caught it."""
        if self.err is not None:
            raise self.err

    async def enter(self, id: str, kind: str) -> None:
        """Call id is being made; one with an effect waits until it is judged."""
        self.check()
        self._reached.add(id)
        self._out[id] = kind
        self._wake()
        if kind != "effect":
            return
        if self._recorded is None:
            if self._loading is None:
                self._loading = asyncio.ensure_future(self._load())
            await asyncio.shield(self._loading)
        self.check()
        assert self._recorded is not None
        if id in self._recorded or not self._unreached():
            self._recorded.setdefault(id, (True, False))
            return
        del self._out[id]
        self._held_call = self._held_call or id
        judged: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._held.append(judged)
        self._wake()
        await judged
        self._recorded[id] = (True, False)
        self._out[id] = kind

    def leave(self, id: str) -> None:
        """Call id answered (or failed)."""
        self._out.pop(id, None)
        self._wake()

    async def _load(self) -> None:
        self._recorded = {r["run_id"]: (self._has_effect(r), finished(r)) for r in await self._children()}

    def _unreached(self) -> list[str]:
        return sorted(id for id, (effect, _) in (self._recorded or {}).items() if effect and id not in self._reached)

    def _wake(self) -> None:
        self._activity += 1
        if not self._held or self._scheduled:
            return
        self._scheduled = True
        asyncio.get_running_loop().call_soon(self._judge, self._activity, 0)

    def _judge(self, activity: int, quiet: int) -> None:
        loop = asyncio.get_running_loop()
        if not self._held or self._recorded is None:
            self._scheduled = False
            return
        unreached = self._unreached()
        if unreached:
            for id, kind in self._out.items():
                rec = self._recorded.get(id)
                # One answered from the record, or a new call that runs, may
                # still lead to the record: wait for it (leave wakes us).
                if (rec[1] if rec is not None else kind != "wait"):
                    self._scheduled = False
                    return
            # Anything still to run in the loop may be the workflow going on.
            ready = getattr(loop, "_ready", None)
            busy = len(ready) > 0 if ready is not None else quiet < _QUIET_TURNS
            if busy or activity != self._activity:
                loop.call_soon(self._judge, self._activity, 0 if activity != self._activity else quiet + 1)
                return
            self.err = DivergedError(self._workflow, self._held_call, unreached)
        self._scheduled = False
        held, self._held, self._held_call = self._held, [], ""
        for f in held:
            if f.done():
                continue
            if self.err is not None:
                f.set_exception(self.err)
            else:
                f.set_result(None)
