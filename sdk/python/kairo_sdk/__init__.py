"""SDK for kairo (ADR 0009, 0026, 0028, 0049).

A worker connects to the runtime, announces the actions it serves and how
many tasks it can hold, and pulls tasks. ``Worker`` runs a handler per task
on a pool of threads; ``kairo_sdk.graphon`` runs Dify's workflow nodes
(graphon) as such handlers. ``Client`` is the graph API (kairod's HTTP
API); ``Kairo`` declares actions and workflows written as code.
"""

from .backend import Backend, EmbeddedBackend, HttpBackend
from .client import Client, KairoError
from .http import SignatureError
from .protocol import Cancel, Chunk, Credit, Hello, MsgType, ProtocolError, Result, read_frame, write_frame
from .worker import Task, TaskContext, Worker
from .store import PostgresStore, SQLiteStore
from ._version import __version__
from .workflow import (
    CallError,
    Cancelled,
    Context,
    Kairo,
    ResultLostError,
    RetryableError,
    StoppedError,
    Suspended,
    TimedOutError,
    UnknownOutcomeError,
    WorkflowError,
    tick_handler,
)

__all__ = [
    "__version__",
    "Backend",
    "EmbeddedBackend",
    "HttpBackend",
    "PostgresStore",
    "SQLiteStore",
    "Suspended",
    "Cancel",
    "CallError",
    "Cancelled",
    "Client",
    "Context",
    "Kairo",
    "KairoError",
    "ResultLostError",
    "RetryableError",
    "StoppedError",
    "TimedOutError",
    "UnknownOutcomeError",
    "WorkflowError",
    "Chunk",
    "Credit",
    "Hello",
    "MsgType",
    "ProtocolError",
    "Result",
    "SignatureError",
    "Task",
    "TaskContext",
    "Worker",
    "read_frame",
    "tick_handler",
    "write_frame",
]
