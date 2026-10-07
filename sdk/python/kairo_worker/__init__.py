"""SDK for kairo (ADR 0009, 0026, 0028, 0049).

A worker connects to the runtime, announces the actions it serves and how
many tasks it can hold, and pulls tasks. ``Worker`` runs a handler per task
on a pool of threads; ``kairo_worker.graphon`` runs Dify's workflow nodes
(graphon) as such handlers. ``Client`` is the graph API (kairod's HTTP
API); ``Kairo`` declares actions and workflows written as code.
"""

from .client import Client, KairoError
from .protocol import Cancel, Chunk, Credit, Hello, MsgType, ProtocolError, Result, read_frame, write_frame
from .worker import Task, TaskContext, Worker
from .workflow import Cancelled, Context, Kairo, ResultLostError

__all__ = [
    "Cancel",
    "Cancelled",
    "Client",
    "Context",
    "Kairo",
    "KairoError",
    "ResultLostError",
    "Chunk",
    "Credit",
    "Hello",
    "MsgType",
    "ProtocolError",
    "Result",
    "Task",
    "TaskContext",
    "Worker",
    "read_frame",
    "write_frame",
]
