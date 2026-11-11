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

from .client import Client, KairoError, KairoTransportError, finished
from .embedded import ActionHandler, Embedded
from .metrics import Metric, MetricEmitter, MetricSink, RUN_START_CONFIRMATION, RUN_START_RETRIES, RUN_START_UNCONFIRMED
from .protocol import Result
from .store import Store
from .store import ChildrenUnsettledError, ParentUnavailableError
from .worker import Worker


def _retry_delay(base: float, minimum: float | None, run_id: str, attempt: int) -> float:
    hash_value = 2166136261
    for byte in run_id.encode():
        hash_value = ((hash_value ^ byte) * 16777619) & 0xFFFFFFFF
    hash_value = ((hash_value ^ attempt) * 16777619) & 0xFFFFFFFF
    jittered = base * (75 + hash_value % 51) / 100
    return max(jittered, minimum or 0.0)


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
        guard_parent: bool = False,
        workflow: str | None = None,
        meta: dict[str, Any] | None = None,
        drive: int = 0,
        then: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Starts a run, or finds it: the run id is an idempotency key. Runs are durable.
        parent: the unfinished workflow that owns this run (ADR 0054, 0067).
        workflow and meta identify root code workflows (ADR 0059, 0068);
        drive and then are embedded-runtime details."""
        ...

    async def get(self, run_id: str, *, input: bool = False) -> dict[str, Any]:
        """Raises KairoError 404 for a run that does not exist. With input:
        the run's input too (ADR 0060)."""
        ...

    async def wait(self, run_id: str) -> dict[str, Any]: ...
    async def signal(self, run_id: str, name: str, payload: Any) -> None: ...
    async def children(self, run_id: str) -> list[dict[str, Any]]: ...
    async def list(self, **opts: Any) -> list[dict[str, Any]]: ...
    async def finish(self, run_id: str, payload: Any) -> None: ...
    async def cancel(self, run_id: str) -> None: ...
    async def resolve(self, run_id: str, output: Any, error: str | None = None) -> None: ...
    async def drain(self) -> None: ...
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
        lease_ms: int = 30_000,
        request_timeout: float = 90.0,
        metrics: MetricSink | MetricEmitter | None = None,
    ) -> None:
        # A cancelled asyncio.to_thread call cannot stop urllib's thread. Its
        # lifetime is nevertheless bounded by this HTTP timeout.
        self.client = Client(url, timeout=request_timeout)
        self._addr = worker
        self._token = token
        self._tenant = tenant
        self._concurrency = concurrency
        self._worker: Worker | None = None
        self.owner = f"kairo-python-{os.getpid()}-{id(self):x}"
        self.lease_ms = lease_ms
        self._claims: dict[str, dict[str, Any]] = {}
        self._renew_task: asyncio.Task[None] | None = None
        self._next_at: int | None = None
        self._closing = False
        self._draining = False
        self._starts: set[asyncio.Task[Any]] = set()
        self._starts_idle = asyncio.Event()
        self._starts_idle.set()
        self.metrics = (
            metrics if isinstance(metrics, MetricEmitter) else MetricEmitter(metrics) if metrics is not None else None
        )

    async def start(self, specs: list[dict[str, Any]], serve: ActionHandler) -> None:
        await asyncio.to_thread(self.client.register_nodes, specs)
        self._renew_task = asyncio.create_task(self._renew())
        if self._addr is None:
            return
        self._worker = Worker(f"kairo-sdk-{os.getpid()}", [s["action"] for s in specs], serve, self._concurrency, token=self._token)
        threading.Thread(target=self._serve_forever, args=(self._worker,), daemon=True).start()

    def _serve_forever(self, w: Worker) -> None:
        while self._worker is w and not self._draining:
            try:
                w.run(self._addr)  # type: ignore[arg-type]
            except OSError:
                pass  # kairod went away: connect again
            if self._worker is w and not self._draining:
                time.sleep(0.5)

    async def register_plan(self, definition: dict[str, Any]) -> None:
        await asyncio.to_thread(self.client.register_plan, definition)

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
        **_: Any,
    ) -> dict[str, Any]:
        # Kept after they finish (ADR 0050): a workflow resumed after kairod
        # restarts still finds its finished calls' results.
        task = asyncio.current_task()
        if task is not None:
            self._starts.add(task)
            self._starts_idle.clear()
        started = time.monotonic() if self.metrics is not None else 0.0
        outcome = "failed"
        try:
            delay = 0.05
            attempt = 0
            while True:
                if self._closing or self._draining:
                    raise asyncio.CancelledError
                minimum = None
                try:
                    result = await asyncio.to_thread(
                        lambda: self.client.run(
                            plan,
                            input,
                            run_id=run_id,
                            tenant=self._tenant,
                            tier="file",
                            vars=vars,
                            keep_output=True,
                            parent=parent,
                            workflow=workflow if not parent else None,
                            meta=meta if not parent else None,
                            drive=drive != 0,
                        )
                    )
                    outcome = "confirmed"
                    return result
                except KairoError as e:
                    if e.status == 409:
                        raise ParentUnavailableError(str(e)) from None
                    if e.status not in (429, 504):
                        raise
                    minimum = e.retry_after
                    cause = "overloaded" if e.status == 429 else "unconfirmed"
                    if e.status == 504 and self.metrics is not None:
                        self.metrics.emit(
                            Metric(RUN_START_UNCONFIRMED, "counter", "{event}", 1, {"cause": "durability_timeout"})
                        )
                except KairoTransportError:
                    cause = "transport"
                    if self.metrics is not None:
                        self.metrics.emit(
                            Metric(RUN_START_UNCONFIRMED, "counter", "{event}", 1, {"cause": "transport_ambiguity"})
                        )
                if self.metrics is not None:
                    self.metrics.emit(Metric(RUN_START_RETRIES, "counter", "{event}", 1, {"cause": cause}))
                await asyncio.sleep(_retry_delay(delay, minimum, run_id, attempt))
                attempt += 1
                delay = min(delay * 2, 1.0)
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            if self.metrics is not None:
                self.metrics.emit(
                    Metric(RUN_START_CONFIRMATION, "histogram", "s", time.monotonic() - started, {"outcome": outcome})
                )
            if task is not None:
                self._starts.discard(task)
                if not self._starts:
                    self._starts_idle.set()

    async def get(self, run_id: str, *, input: bool = False) -> dict[str, Any]:
        return await asyncio.to_thread(self.client.get, run_id, input=input)

    async def wait(self, run_id: str) -> dict[str, Any]:
        while True:
            r = await asyncio.to_thread(self.client.wait_once, run_id)
            if r is not None:
                return r

    async def wait_quiescent(self, run_id: str) -> dict[str, Any]:
        r = await asyncio.to_thread(self.client.wait_quiescent, run_id)
        self.note_next(r.get("next_at"))
        return r

    def note_next(self, at: Any) -> None:
        if isinstance(at, int) and at > 0 and (self._next_at is None or at < self._next_at):
            self._next_at = at

    async def claim(self, workflows: dict[str, list[str]], run_id: str = "", timeout_ms: int = 0) -> dict[str, Any] | None:
        if self._draining:
            return None
        out = await asyncio.to_thread(
            self.client.claim_workflow,
            owner=self.owner,
            lease_ms=self.lease_ms,
            workflows=workflows,
            run_id=run_id,
            timeout_ms=timeout_ms,
        )
        self.note_next(out.get("next_at"))
        claim = out.get("claim")
        if isinstance(claim, dict) and self._draining:
            await self.release(claim, True)
            return None
        if isinstance(claim, dict):
            self._claims[claim["run"]["run_id"]] = claim
            return claim
        return None

    async def release(self, claim: dict[str, Any], retry: bool = False) -> None:
        run_id = claim["run"]["run_id"]
        self._claims.pop(run_id, None)
        try:
            await asyncio.to_thread(
                self.client.release_workflow, run_id, owner=self.owner, token=claim["token"], retry=retry
            )
        except KairoError as e:
            if e.status != 409:
                raise

    async def _renew(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(self.lease_ms / 3000, 0.01))
                for run_id, claim in list(self._claims.items()):
                    try:
                        await asyncio.to_thread(
                            self.client.renew_workflow, run_id, owner=self.owner, token=claim["token"], lease_ms=self.lease_ms
                        )
                    except Exception:
                        self._claims.pop(run_id, None)
        except asyncio.CancelledError:
            pass

    async def next_wake(self) -> int | None:
        return self._next_at

    def clear_due(self, now: int) -> None:
        if self._next_at is not None and self._next_at <= now:
            self._next_at = None

    async def signal(self, run_id: str, name: str, payload: Any) -> None:
        await asyncio.to_thread(self.client.signal, run_id, name, payload)

    async def children(self, run_id: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.client.children, run_id)

    async def list(self, **opts: Any) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.client.list, **opts)

    async def finish(self, run_id: str, payload: Any) -> None:
        try:
            if finished(await self.get(run_id)):
                return
            await asyncio.to_thread(self.client.finish, run_id, payload)
        except KairoError as e:
            if e.status == 404:
                try:
                    if finished(await self.get(run_id)):
                        return
                except KairoError:
                    pass
            if e.status == 409:
                raise ChildrenUnsettledError(str(e)) from None
            raise

    async def cancel(self, run_id: str) -> None:
        try:
            await asyncio.to_thread(self.client.cancel, run_id)
        except KairoError as e:
            if e.status == 409:
                raise ChildrenUnsettledError(str(e)) from None
            raise

    async def resolve(self, run_id: str, output: Any, error: str | None = None) -> None:
        r = await self.get(run_id)
        if finished(r):
            return  # an HTTP retry after an ambiguous response
        reviews = r.get("reviews") or []
        if not reviews:
            raise KairoError(409, f"run {run_id} has no step stopped for review")
        await asyncio.to_thread(self.client.resolve, run_id, reviews[0]["act"], output, error)
        await self.wait(run_id)  # terminal event is durable when this answers

    async def close(self) -> None:
        self._closing = True
        current = asyncio.current_task()
        for task in list(self._starts):
            if task is not current:
                task.cancel()
        for claim in list(self._claims.values()):
            await self.release(claim, True)
        if self._renew_task is not None:
            self._renew_task.cancel()
            await asyncio.gather(self._renew_task, return_exceptions=True)
            self._renew_task = None
        w, self._worker = self._worker, None
        if w is not None:
            w.stop()

    async def drain(self) -> None:
        self._draining = True
        current = asyncio.current_task()
        for task in list(self._starts):
            if task is not current:
                task.cancel()
        await self._starts_idle.wait()
        w = self._worker
        if w is None:
            return
        await asyncio.to_thread(w.drain)

    @property
    def inflight(self) -> int:
        return self._worker.inflight if self._worker is not None else 0


class EmbeddedBackend:
    """The runtime embedded in this process, with a database (ADR 0051)."""

    def __init__(self, runtime: Embedded) -> None:
        self.runtime = runtime

    @classmethod
    async def open(cls, store: Store, **kw: Any) -> EmbeddedBackend:
        """Opens the runtime on store (kw: wasm, lease_ms, owner, now, manual_timers, ...)."""
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

    async def children(self, run_id: str) -> list[dict[str, Any]]:
        return await self.runtime.children(run_id)

    async def list(self, **opts: Any) -> list[dict[str, Any]]:
        return await self.runtime.list(**opts)

    async def finish(self, run_id: str, payload: Any) -> None:
        await self.runtime.signal(run_id, "done", payload)

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
