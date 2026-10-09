"""Where workflows written as code run their calls (ADR 0049, 0051): kairod
over HTTP and the worker protocol, or the runtime embedded in this process
with a database."""

from __future__ import annotations

import asyncio
import os
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol

from .client import Client
from .embedded import ActionHandler, Embedded
from .protocol import Result
from .store import Store
from .worker import Worker


class Backend(Protocol):
    async def start(self, specs: list[dict[str, Any]], serve: ActionHandler) -> None:
        """Registers the actions, and runs their steps with serve (here, or on a worker)."""
        ...

    async def register_plan(self, definition: dict[str, Any]) -> None: ...

    async def run(
        self,
        plan: str,
        input: Any,
        *,
        run_id: str,
        vars: dict[str, Any] | None = None,
        parent: str | None = None,
        workflow: str | None = None,
        meta: dict[str, Any] | None = None,
        drive: int = 0,
        then: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Starts a run, or finds it: the run id is an idempotency key. Runs are durable.
        parent: the run that makes this one (the embedded runtime keeps and
        removes them together, ADR 0054). workflow, meta, drive and then: the
        embedded runtime only (ADR 0059); kairod ignores them."""
        ...

    async def get(self, run_id: str, *, input: bool = False) -> dict[str, Any]:
        """Raises KairoError 404 for a run that does not exist. With input:
        the run's input too (ADR 0060)."""
        ...

    async def wait(self, run_id: str) -> dict[str, Any]: ...
    async def signal(self, run_id: str, name: str, payload: Any) -> None: ...
    async def cancel(self, run_id: str) -> None: ...
    async def close(self) -> None: ...


class HttpBackend:
    """kairod: its HTTP API, and a worker that pulls this process's actions."""

    def __init__(
        self,
        url: str = "http://127.0.0.1:8420",
        *,
        worker: str | tuple[str, int] | None = None,
        token: str = "",
        tenant: str = "default",
        concurrency: int | None = None,
    ) -> None:
        self.client = Client(url)
        self._addr = worker
        self._token = token
        self._tenant = tenant
        self._concurrency = concurrency
        self._worker: Worker | None = None

    async def start(self, specs: list[dict[str, Any]], serve: ActionHandler) -> None:
        await asyncio.to_thread(self.client.register_nodes, specs)
        if self._addr is None:
            return
        self._worker = Worker(f"kairo-sdk-{os.getpid()}", [s["action"] for s in specs], serve, self._concurrency, token=self._token)
        threading.Thread(target=self._serve_forever, args=(self._worker,), daemon=True).start()

    def _serve_forever(self, w: Worker) -> None:
        while self._worker is w:
            try:
                w.run(self._addr)  # type: ignore[arg-type]
            except OSError:
                pass  # kairod went away: connect again
            if self._worker is w:
                time.sleep(0.5)

    async def register_plan(self, definition: dict[str, Any]) -> None:
        await asyncio.to_thread(self.client.register_plan, definition)

    async def run(
        self, plan: str, input: Any, *, run_id: str, vars: dict[str, Any] | None = None, parent: str | None = None, **_: Any
    ) -> dict[str, Any]:
        # Kept after they finish (ADR 0050): a workflow resumed after kairod
        # restarts still finds its finished calls' results.
        return await asyncio.to_thread(
            lambda: self.client.run(plan, input, run_id=run_id, tenant=self._tenant, tier="file", vars=vars, keep_output=True)
        )

    async def get(self, run_id: str, *, input: bool = False) -> dict[str, Any]:
        return await asyncio.to_thread(self.client.get, run_id, input=input)

    async def wait(self, run_id: str) -> dict[str, Any]:
        while True:
            r = await asyncio.to_thread(self.client.wait_once, run_id)
            if r is not None:
                return r

    async def signal(self, run_id: str, name: str, payload: Any) -> None:
        await asyncio.to_thread(self.client.signal, run_id, name, payload)

    async def cancel(self, run_id: str) -> None:
        await asyncio.to_thread(self.client.cancel, run_id)

    async def close(self) -> None:
        w, self._worker = self._worker, None
        if w is not None:
            w.stop()


class EmbeddedBackend:
    """The runtime embedded in this process, with a database (ADR 0051)."""

    def __init__(self, runtime: Embedded) -> None:
        self.runtime = runtime

    @classmethod
    async def open(cls, store: Store, **kw: Any) -> EmbeddedBackend:
        """Opens the runtime on store (kw: wasm, lease_ms, owner, now)."""
        return cls(await Embedded(store, **kw).open())

    async def start(self, specs: list[dict[str, Any]], serve: ActionHandler) -> None:
        self.runtime.register_actions(specs, serve)

    async def register_plan(self, definition: dict[str, Any]) -> None:
        self.runtime.register_plan(definition)

    #: Keeps a workflow's calls while it runs, however long (ADR 0054).
    keeps_calls = True

    async def run(self, plan: str, input: Any, *, run_id: str, **kw: Any) -> dict[str, Any]:
        return await self.runtime.run(plan, input, run_id=run_id, **kw)

    async def get(self, run_id: str, *, input: bool = False) -> dict[str, Any]:
        return await self.runtime.get(run_id)

    async def wait(self, run_id: str) -> dict[str, Any]:
        return await self.runtime.wait(run_id)

    async def signal(self, run_id: str, name: str, payload: Any) -> None:
        await self.runtime.signal(run_id, name, payload)

    async def cancel(self, run_id: str) -> None:
        await self.runtime.cancel(run_id)

    async def resolve(self, run_id: str, output: Any, error: str | None = None) -> None:
        """Settles a step stopped for review, or a real attempt out that cannot
        go on (ADR 0060)."""
        await self.runtime.resolve(run_id, output, error)

    async def close(self) -> None:
        await self.runtime.close()

    async def idle(self) -> None:
        await self.runtime.idle()

    def on_settled(self, hook: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        return self.runtime.on_settled(hook)

    async def tick(self) -> None:
        await self.runtime.tick()

    async def next_wake(self) -> int | None:
        return await self.runtime.next_wake()

    async def complete(self, run_id: str, act: int, attempt: int, res: Result) -> None:
        await self.runtime.complete(run_id, act, attempt, res)
