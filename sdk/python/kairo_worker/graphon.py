"""Runs Dify workflow nodes (graphon 0.7.0) as kairo tasks (ADR 0028).

kairo runs the graph; this runs one node per task. The task's action is
``dify.<node type>``, its params are the node's data and its input maps
selectors ("node.var", "sys.query", ...) to their values, as compat/dify
converts them. The values go into a fresh variable pool, the node is built
by a node factory and run, and its result becomes the task's:

- streamed chunks are emitted as the task's live output;
- outputs become the output; process_data, inputs and metadata go to
  ``meta`` (the step's trace, ADR 0034); token usage to ``tokens``;
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

import time
from collections.abc import Callable, Mapping
from typing import Any

from graphon.entities import GraphInitParams
from graphon.graph_events import node as node_events
from graphon.runtime import GraphRuntimeState, VariablePool

from .protocol import Result
from .worker import Task, TaskContext

FactoryBuilder = Callable[[Mapping[str, Any], GraphInitParams, GraphRuntimeState], Any]

# Nodes with effects outside (ADR 0035): an unknown outcome is not a
# definite failure.
REAL = frozenset({"http-request", "tool", "agent", "knowledge-index"})
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


def variable_pool(inputs: Mapping[str, Any] | None) -> VariablePool:
    """A pool holding the task's inputs. A name "a.b.c" is the value at
    selector [a, b, c]: it is stored as {"c": value} under [a, b]."""
    pool = VariablePool()
    nested: dict[tuple[str, str], Any] = {}
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
        pool.add([node, var], value)
    return pool


class GraphonNodeRunner:
    """A Worker handler that runs graphon nodes."""

    def __init__(self, factory: FactoryBuilder, *, run_context: Mapping[str, Any] | None = None) -> None:
        self.factory = factory
        self.run_context = dict(run_context or {})

    def __call__(self, task: Task, ctx: TaskContext) -> Result:
        node_type = task.action.removeprefix("dify.")
        data = dict(task.params or {})
        data.setdefault("type", node_type)
        node_config = {"id": task.node_id, "data": data}
        graph_config = {"nodes": [node_config], "edges": []}
        init = GraphInitParams(
            workflow_id=task.run_id,
            graph_config=graph_config,
            run_context=self.run_context,
            call_depth=task.depth,
        )
        state = GraphRuntimeState(variable_pool=variable_pool(task.input), start_at=time.perf_counter())
        node = self.factory(graph_config, init, state).create_node(node_config)
        node.bind_execution_id(f"{task.run_id}/{task.step_id}#{task.attempt}")
        for ev in node.run():
            if ctx.cancelled.is_set():
                # The step was abandoned: what it returns is ignored.
                return Result(error="cancelled", retryable=True, error_type="Cancelled")
            if isinstance(ev, node_events.NodeRunStreamChunkEvent):
                if ev.chunk:
                    ctx.emit(ev.chunk)
            elif isinstance(ev, node_events.NodeRunSucceededEvent):
                return self._result(node_type, ev.node_run_result, ok=True)
            elif isinstance(ev, node_events.NodeRunFailedEvent | node_events.NodeRunExceptionEvent):
                return self._result(node_type, ev.node_run_result, ok=False, error=ev.error)
        return Result(error="node finished without a result", error_type="ProtocolError")

    def _result(self, node_type: str, r: Any, *, ok: bool, error: str = "") -> Result:
        meta = {k: plain(getattr(r, k, None) or {}) for k in ("inputs", "process_data", "metadata")}
        meta = {k: v for k, v in meta.items() if v} or None
        usage = getattr(r, "llm_usage", None)
        tokens = int(getattr(usage, "total_tokens", 0) or 0)
        if ok:
            return Result(output=plain(dict(r.outputs or {})), meta=meta, tokens=tokens)
        error_type = getattr(r, "error_type", "") or "NodeError"
        unknown = node_type in REAL and error_type in UNKNOWN_ERRORS
        return Result(error=error or getattr(r, "error", "") or "failed", error_type=error_type,
                      retryable=not unknown, unknown=unknown, meta=meta, tokens=tokens)
