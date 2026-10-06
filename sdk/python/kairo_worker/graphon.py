"""Runs Dify workflow nodes (graphon 0.7.0) as kairo tasks (ADR 0028).

kairo runs the graph; this runs one node per task. The task's action is
``dify.<node type>``, its params are the node's data and its input maps
selectors ("node.var", "sys.query", ...) to their values, as compat/dify
converts them. The values go into a fresh variable pool, the node is built
by a node factory and run, and its result becomes the task's. Two inputs
are special (ADR 0037): ``__dify``, the host's run context (passed as the
node's run_context), and ``__sys``, all system variables (put in the pool
under "sys").

The result:

- node events streamed while the node runs (chunks, reasoning, retriever
  resources, agent logs) are emitted as the task's live output, one JSON
  object {"event": <class name>, "data": {...}} each;
- outputs become the output; process_data, inputs, metadata and llm_usage
  go to ``meta`` (the step's trace, ADR 0034); token usage to ``tokens``;
- a failure becomes an error with graphon's error_type. A failure of a node
  that reaches the outside (http-request, tool, agent) whose error type says
  the outcome is unknown (a timeout, a broken connection) is reported as
  unknown (ADR 0035).

The node factory decides what a node can reach: Dify passes its own
(DifyNodeFactory, with models, plugins and files). ``slim_factory`` builds
graphon's DSL factory, enough for template-transform, http-request, code
(with a sandbox) and the Slim plugin runtime.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, nullcontext
from typing import Any

from graphon.entities import GraphInitParams
from graphon.file import FILE_MODEL_IDENTITY, File
from graphon.graph_events import node as node_events
from graphon.graph_events.agent import NodeRunAgentLogEvent
from graphon.runtime import GraphRuntimeState, VariablePool

from .protocol import Result
from .worker import Task, TaskContext

FactoryBuilder = Callable[[Mapping[str, Any], GraphInitParams, GraphRuntimeState], Any]

# Nodes with effects outside (ADR 0035): an unknown outcome is not a
# definite failure.
REAL = frozenset({"http-request", "tool", "agent", "knowledge-index"})
# Error types that say the destination refused for its limits (ADR 0039).
RATE_LIMIT_ERRORS = frozenset({"InvokeRateLimitError", "RateLimitError", "TooManyRequests", "RateLimitExceeded"})
# Error types that leave the outcome unknown.
UNKNOWN_ERRORS = frozenset({
    "TimeoutError", "ReadTimeout", "WriteTimeout", "ConnectTimeout", "PoolTimeout", "TimeoutException",
    "RemoteProtocolError", "ReadError", "WriteError", "ConnectionError", "ConnectionResetError",
})


def slim_factory(credentials: Mapping[str, Any] | None = None) -> FactoryBuilder:
    """graphon's DSL node factory, for running nodes without Dify."""
    from graphon.dsl.entities import DslCredentials
    from graphon.dsl.node_factory import SlimDslNodeFactory

    creds = DslCredentials.model_validate(dict(credentials or {}))

    def build(graph_config: Mapping[str, Any], init: GraphInitParams, state: GraphRuntimeState) -> Any:
        return SlimDslNodeFactory(
            graph_config=graph_config,
            graph_init_params=init,
            graph_runtime_state=state,
            credentials=creds,
            dependencies=[],
        )

    return build


def plain(v: Any) -> Any:
    """graphon values (segments, files, models) as JSON values."""
    if hasattr(v, "to_object"):
        return plain(v.to_object())
    if hasattr(v, "model_dump"):
        return plain(v.model_dump(mode="json"))
    if isinstance(v, Mapping):
        return {str(k): plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [plain(x) for x in v]
    return v


RUN_CONTEXT = "__dify"
SYS = "__sys"


def variable_pool(inputs: Mapping[str, Any] | None) -> VariablePool:
    """A pool holding the task's inputs. A name "a.b.c" is the value at
    selector [a, b, c]: it is stored as {"c": value} under [a, b]. The
    system variables of ``__sys`` go under ["sys", name]."""
    pool = VariablePool()
    nested: dict[tuple[str, str], Any] = {}
    sys_vars = (inputs or {}).get(SYS)
    if isinstance(sys_vars, Mapping):
        for k, v in sys_vars.items():
            nested[("sys", str(k))] = v
    for name, value in (inputs or {}).items():
        sel = name.split(".")
        if len(sel) < 2:
            continue
        key = (sel[0], sel[1])
        if len(sel) == 2:
            nested[key] = value
            continue
        obj = nested.setdefault(key, {})
        if not isinstance(obj, dict):
            continue
        for part in sel[2:-1]:
            obj = obj.setdefault(part, {})
        obj[sel[-1]] = value
    for (node, var), value in nested.items():
        pool.add([node, var], decode_value(value))
    return pool


def execution_id(run_id: str, step_id: str) -> str:
    """The node execution id of a step, as hosts derive it too: the same for
    every attempt, like graphon's."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"kairo:{run_id}/{step_id}"))


def decode_value(v: Any) -> Any:
    """JSON values back into graphon values: Dify's file objects become Files."""
    if isinstance(v, Mapping):
        if v.get("dify_model_identity") == FILE_MODEL_IDENTITY:
            try:
                return File.model_validate(dict(v))
            except Exception:
                return dict(v)
        return {k: decode_value(x) for k, x in v.items()}
    if isinstance(v, list):
        return [decode_value(x) for x in v]
    return v


# Node events streamed to the host while a node runs, as live output: one
# JSON object {"event": <class name>, "data": {...}} per chunk (ADR 0037).
LIVE_EVENTS = (
    node_events.NodeRunStreamChunkEvent,
    node_events.NodeRunReasoningChunkEvent,
    node_events.NodeRunRetrieverResourceEvent,
    node_events.NodeRunModelPollingProgressEvent,
    NodeRunAgentLogEvent,
)
# Fields of node events the host sets itself.
BASE_FIELDS = frozenset({"id", "node_id", "node_type", "in_iteration_id", "in_loop_id", "node_version", "node_run_result"})


class GraphonNodeRunner:
    """A Worker handler that runs graphon nodes.

    ``around`` wraps each task (e.g. an application context). The run
    context given in ``__dify`` may carry "workflow_id", the workflow the
    node belongs to; it is taken out of the run context.
    """

    def __init__(
        self,
        factory: FactoryBuilder,
        *,
        run_context: Mapping[str, Any] | None = None,
        around: Callable[[], AbstractContextManager[Any]] | None = None,
    ) -> None:
        self.factory = factory
        self.run_context = dict(run_context or {})
        self.around = around or nullcontext

    def __call__(self, task: Task, ctx: TaskContext) -> Result:
        with self.around():
            return self._run(task, ctx)

    def _run(self, task: Task, ctx: TaskContext) -> Result:
        node_type = task.action.removeprefix("dify.")
        data = dict(task.params or {})
        data.setdefault("type", node_type)
        node_config = {"id": task.node_id, "data": data}
        graph_config = {"nodes": [node_config], "edges": []}
        run_context = dict(self.run_context)
        given = (task.input or {}).get(RUN_CONTEXT) if isinstance(task.input, Mapping) else None
        if isinstance(given, Mapping):
            run_context.update(given)
        workflow_id = str(run_context.pop("workflow_id", "") or task.run_id)
        init = GraphInitParams(
            workflow_id=workflow_id,
            graph_config=graph_config,
            run_context=run_context,
            call_depth=task.depth,
        )
        state = GraphRuntimeState(variable_pool=variable_pool(task.input), start_at=time.perf_counter())
        node = self.factory(graph_config, init, state).create_node(node_config)
        node.bind_execution_id(execution_id(task.run_id, task.step_id))
        for ev in node.run():
            if ctx.cancelled.is_set():
                # The step was abandoned: what it returns is ignored.
                return Result(error="cancelled", retryable=True, error_type="Cancelled")
            if isinstance(ev, node_events.NodeRunSucceededEvent):
                return self._result(node_type, ev.node_run_result, ok=True)
            if isinstance(ev, node_events.NodeRunFailedEvent | node_events.NodeRunExceptionEvent):
                return self._result(node_type, ev.node_run_result, ok=False, error=ev.error)
            if isinstance(ev, LIVE_EVENTS):
                if isinstance(ev, node_events.NodeRunStreamChunkEvent) and not ev.chunk and not ev.is_final:
                    continue
                body = {"event": type(ev).__name__, "data": ev.model_dump(mode="json", exclude=set(BASE_FIELDS))}
                ctx.emit(json.dumps(body, ensure_ascii=False))
        return Result(error="node finished without a result", error_type="ProtocolError")

    def _result(self, node_type: str, r: Any, *, ok: bool, error: str = "") -> Result:
        meta = {k: plain(getattr(r, k, None) or {}) for k in ("inputs", "process_data", "metadata")}
        usage = getattr(r, "llm_usage", None)
        tokens = int(getattr(usage, "total_tokens", 0) or 0)
        if tokens:
            meta["llm_usage"] = plain(usage)
        meta = {k: v for k, v in meta.items() if v} or None
        if ok:
            return Result(output=plain(dict(r.outputs or {})), meta=meta, tokens=tokens)
        error_type = getattr(r, "error_type", "") or "NodeError"
        unknown = node_type in REAL and error_type in UNKNOWN_ERRORS
        return Result(error=error or getattr(r, "error", "") or "failed", error_type=error_type,
                      retryable=not unknown, unknown=unknown, meta=meta, tokens=tokens,
                      rate_limited=error_type in RATE_LIMIT_ERRORS or "429" in (error or ""))
