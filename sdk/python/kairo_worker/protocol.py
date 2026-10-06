"""The kairo worker protocol (protocol/protocol.go is the reference).

Every message is a 4-byte big-endian length, a 1-byte type and a JSON body;
the length counts the type byte and the body.

    worker  -> runtime  Hello  {"worker": "py-1", "actions": [...], "credit": 8, "token": "..."}
    runtime -> worker   Task   {task}
    worker  -> runtime  Chunk  {"seq": 17, "data": "<base64>"}
    worker  -> runtime  Result {"seq": 17, "output": {...}}   (grants 1 credit)
    worker  -> runtime  Credit {"n": 4}
    runtime -> worker   Cancel {"seq": 17}                    (ADR 0026)
"""

from __future__ import annotations

import base64
import enum
import json
import struct
from dataclasses import dataclass, field
from typing import Any, BinaryIO

MAX_FRAME = 64 << 20


class MsgType(enum.IntEnum):
    HELLO = 1
    TASK = 2
    RESULT = 3
    CREDIT = 4
    CHUNK = 5
    CANCEL = 6


class ProtocolError(Exception):
    pass


@dataclass
class Hello:
    worker: str
    actions: list[str]
    credit: int
    token: str = ""  # the runtime's worker token, if it has one (ADR 0037)

    def body(self) -> dict[str, Any]:
        b: dict[str, Any] = {"worker": self.worker, "actions": self.actions, "credit": self.credit}
        if self.token:
            b["token"] = self.token
        return b


@dataclass
class Result:
    """A task's result. ``unknown``: the outcome is not known (a timeout, a
    broken connection); never treated as success. ``retryable``: a definite
    failure that may be retried. ``meta`` is passed to the step's trace
    (ADR 0034). ``rate_limited``: the destination refused the task for its
    limits; it lowers the destination's concurrency (ADR 0039)."""

    seq: int = 0
    output: Any = None
    error: str = ""
    retryable: bool = False
    unknown: bool = False
    tokens: int = 0
    error_type: str = ""
    meta: Any = None
    rate_limited: bool = False

    def body(self) -> dict[str, Any]:
        b: dict[str, Any] = {"seq": self.seq}
        if self.error or self.unknown:
            b["error"] = self.error
        else:
            b["output"] = self.output
        for k in ("retryable", "unknown", "tokens", "error_type", "meta", "rate_limited"):
            v = getattr(self, k)
            if v:
                b[k] = v
        return b


@dataclass
class Chunk:
    seq: int
    data: bytes = field(default=b"")

    def body(self) -> dict[str, Any]:
        return {"seq": self.seq, "data": base64.b64encode(self.data).decode()}


@dataclass
class Credit:
    n: int

    def body(self) -> dict[str, Any]:
        return {"n": self.n}


@dataclass
class Cancel:
    seq: int


def write_frame(w: BinaryIO, t: MsgType, body: dict[str, Any]) -> None:
    payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode()
    if len(payload) + 1 > MAX_FRAME:
        raise ProtocolError("frame too large")
    w.write(struct.pack(">IB", len(payload) + 1, int(t)) + payload)


def _read_exact(r: BinaryIO, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = r.read(n - len(buf))
        if not chunk:
            raise EOFError
        buf += chunk
    return buf


def read_frame(r: BinaryIO) -> tuple[int, Any]:
    """Reads one frame: (type, decoded JSON body). Unknown types are
    returned too; callers skip what they do not understand."""
    (n,) = struct.unpack(">I", _read_exact(r, 4))
    if n == 0 or n > MAX_FRAME:
        raise ProtocolError("frame too large")
    data = _read_exact(r, n)
    return data[0], json.loads(data[1:]) if len(data) > 1 else None
