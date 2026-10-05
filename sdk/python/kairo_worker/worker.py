"""A worker: pulls tasks from the runtime and runs a handler for each."""

from __future__ import annotations

import logging
import socket
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from .protocol import Cancel, Chunk, Hello, MsgType, Result, read_frame, write_frame

logger = logging.getLogger(__name__)


@dataclass
class Task:
    """A task as the runtime sends it (task/task.go)."""

    run_id: str = ""
    step_id: str = ""
    act: int = 0
    attempt: int = 0
    idempotency_key: str = ""
    action: str = ""
    destination: str = ""
    tenant: str = ""
    effect: str = ""
    input: Any = None
    params: Any = None
    seq: int = 0
    depth: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_body(cls, b: dict[str, Any]) -> Task:
        known = {f for f in cls.__dataclass_fields__ if f != "extra"}
        t = cls(**{k: v for k, v in b.items() if k in known})
        t.extra = {k: v for k, v in b.items() if k not in known}
        return t

    @property
    def node_id(self) -> str:
        """The plan node of the step: its step id without the iteration path."""
        return self.step_id.split("[", 1)[0]


class TaskContext:
    """What a handler gets besides the task: whether the step was abandoned
    (timeout, run cancelled; ADR 0026) and a way to stream output."""

    def __init__(self, emit: Callable[[bytes], None]) -> None:
        self.cancelled = threading.Event()
        self._emit = emit

    def emit(self, data: bytes | str) -> None:
        self._emit(data.encode() if isinstance(data, str) else data)


Handler = Callable[[Task, TaskContext], Result]


class Worker:
    """Serves actions with handler, concurrency tasks at a time.

    ``run`` connects (a UNIX socket path, or (host, port)), says hello and
    serves until the connection closes or ``stop`` is called. The handler
    must return a Result, also when cancelled: that releases the task's
    concurrency slot and credit at the runtime.
    """

    def __init__(self, name: str, actions: list[str], handler: Handler, concurrency: int = 4) -> None:
        self.name = name
        self.actions = actions
        self.handler = handler
        self.concurrency = max(concurrency, 1)
        self._wlock = threading.Lock()
        self._sock: socket.socket | None = None
        self._cancels: dict[int, TaskContext] = {}
        self._clock = threading.Lock()

    def _send(self, w: Any, t: MsgType, body: dict[str, Any]) -> None:
        with self._wlock:
            write_frame(w, t, body)
            w.flush()

    def run(self, addr: str | tuple[str, int]) -> None:
        if isinstance(addr, str):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(addr)
        self._sock = sock
        try:
            self._run_on(sock.makefile("rb"), sock.makefile("wb"))
        finally:
            sock.close()

    def _run_on(self, r: Any, w: Any) -> None:
        self._send(w, MsgType.HELLO, Hello(self.name, self.actions, self.concurrency).body())
        with ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix=self.name) as pool:
            try:
                while True:
                    try:
                        typ, body = read_frame(r)
                    except (EOFError, OSError):
                        break
                    if typ == MsgType.TASK:
                        task = Task.from_body(body)
                        ctx = TaskContext(lambda d, s=task.seq: self._send(w, MsgType.CHUNK, Chunk(s, d).body()))
                        with self._clock:
                            self._cancels[task.seq] = ctx
                        pool.submit(self._serve, w, task, ctx)
                    elif typ == MsgType.CANCEL:
                        c = Cancel(body.get("seq", 0))
                        with self._clock:
                            ctx = self._cancels.get(c.seq)
                        if ctx is not None:
                            ctx.cancelled.set()
                    # Unknown types are skipped.
            finally:
                with self._clock:
                    for ctx in self._cancels.values():
                        ctx.cancelled.set()

    def _serve(self, w: Any, task: Task, ctx: TaskContext) -> None:
        try:
            res = self.handler(task, ctx)
        except Exception as e:  # a handler bug must not lose the task's slot
            logger.exception("task %s failed", task.step_id)
            res = Result(error=f"{type(e).__name__}: {e}", error_type=type(e).__name__, retryable=False)
        res.seq = task.seq
        with self._clock:
            self._cancels.pop(task.seq, None)
        try:
            self._send(w, MsgType.RESULT, res.body())
        except OSError:
            pass

    def stop(self) -> None:
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
