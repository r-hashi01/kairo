"""Run Dify workflow DSL fixtures on graphon 0.7.0 with mocked external
nodes and print a normalized trace (ADR 0028's differential harness).

Nodes that reach the outside (llm, code, http-request, tool,
template-transform, question-classifier, parameter-extractor, agent,
knowledge-retrieval, document-extractor) are replaced by deterministic
mocks: their outputs come from the case file, or a default derived from the
node id. Control nodes (if-else, iteration, loop, assigner, aggregator,
list-operator, answer, end, start) run as graphon implements them.

    python graphon_trace.py <fixture.yml> [case.json]  > trace.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import yaml

from graphon.dsl import loads
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.graph_events import graph as graph_events
from graphon.graph_events import node as node_events
from graphon.node_events.base import NodeRunResult

MOCKED = {
    "llm": "graphon.nodes.llm.node:LLMNode",
    "code": "graphon.nodes.code.code_node:CodeNode",
    "http-request": "graphon.nodes.http_request.node:HttpRequestNode",
    "tool": "graphon.nodes.tool.tool_node:ToolNode",
    "template-transform": "graphon.nodes.template_transform.template_transform_node:TemplateTransformNode",
    "question-classifier": "graphon.nodes.question_classifier.question_classifier_node:QuestionClassifierNode",
    "parameter-extractor": "graphon.nodes.parameter_extractor.parameter_extractor_node:ParameterExtractorNode",
}


def default_outputs(node_type: str, node_id: str, data: dict[str, Any]) -> dict[str, Any]:
    """The mock output of a node, the same one the kairo side uses."""
    if node_type == "llm":
        return {"text": f"llm:{node_id}"}
    if node_type == "template-transform":
        return {"output": f"tpl:{node_id}"}
    if node_type == "http-request":
        return {"status_code": 200, "body": f"http:{node_id}", "headers": {}, "files": []}
    if node_type == "tool":
        return {"text": f"tool:{node_id}", "files": [], "json": []}
    if node_type == "question-classifier":
        first = (data.get("classes") or [{"id": ""}])[0]["id"]
        return {"class_id": first, "class_name": f"qc:{node_id}"}
    if node_type == "parameter-extractor":
        return {"__is_success": 1, "__reason": None}
    if node_type == "code":
        out: dict[str, Any] = {}
        for name, spec in (data.get("outputs") or {}).items():
            t = (spec or {}).get("type", "string")
            out[name] = {
                "string": f"code:{node_id}",
                "number": 1,
                "boolean": True,
                "object": {"node": node_id},
                "array[string]": [f"code:{node_id}"],
                "array[number]": [1],
                "array[object]": [{"node": node_id}],
                "array[boolean]": [True],
            }.get(t, f"code:{node_id}")
        return out
    return {}


def install_mocks(case: dict[str, Any]) -> None:
    import importlib
    from unittest.mock import MagicMock

    import graphon.dsl.node_factory as nf

    # External runtimes are never called (the nodes' _run is mocked); give
    # the factory stand-ins so it does not look for models and plugins.
    nf.SlimDslNodeFactory._create_slim_llm_runtime = lambda self, *, node_id, data, node_type_label: (dict(data), MagicMock())
    nf.SlimDslNodeFactory._create_tool_runtime = lambda self, *a, **k: MagicMock()

    # Human input: graphon asks a callback; the case decides as the person
    # would (the first action by default, or {"expired": true}).
    from graphon.nodes.human_input.entities import Completed, Expired, HumanInputNodeData
    from graphon.nodes.human_input.human_input_node import HumanInputNode
    from graphon.variables.factory import build_segment

    human = case.get("human", {})

    def _create_human_input_node(self, request):  # noqa: ANN001, ANN202
        actions = request.data_payload.get("user_actions") or [{"id": ""}]
        decision = human.get(request.node_id, {"handle": actions[0]["id"], "outputs": {}})

        def callback(ctx):  # noqa: ANN001, ANN202
            if decision.get("expired"):
                return Expired(selected_handle="__timeout", outputs={})
            outputs = {k: build_segment(v) for k, v in decision.get("outputs", {}).items()}
            return Completed(selected_handle=decision["handle"], inputs={}, outputs=outputs)

        return HumanInputNode(
            node_id=request.node_id,
            data=HumanInputNodeData.model_validate({"type": "human-input", "title": request.data_payload.get("title", "")}),
            graph_init_params=self.graph_init_params,
            graph_runtime_state=self.graph_runtime_state,
            hitl_callback=callback,
        )

    import graphon.dsl.importer as importer

    builders = {**nf.SlimDslNodeFactory.NODE_BUILDERS, "human-input": _create_human_input_node}
    nf.SlimDslNodeFactory.NODE_BUILDERS = builders
    importer.SUPPORTED_DEFAULT_FACTORY_NODE_TYPES = frozenset(builders)

    overrides = case.get("outputs", {})
    errors = case.get("errors", {})
    for node_type, path in MOCKED.items():
        mod, cls = path.split(":")
        klass = getattr(importlib.import_module(mod), cls)

        def _run(self, _t=node_type):  # noqa: ANN001
            if self._node_id in errors:
                return NodeRunResult(status=WorkflowNodeExecutionStatus.FAILED, error=errors[self._node_id],
                                     error_type="MockError")
            data = self.node_data.model_dump(mode="json") if hasattr(self.node_data, "model_dump") else {}
            outputs = overrides.get(self._node_id, default_outputs(_t, self._node_id, data))
            # A classifier routes by the class it chose.
            handle = outputs.get("class_id") if _t == "question-classifier" else None
            return NodeRunResult(status=WorkflowNodeExecutionStatus.SUCCEEDED, outputs=outputs,
                                 edge_source_handle=handle or "source")

        klass._run = _run  # type: ignore[method-assign]


def default_inputs(doc: dict[str, Any]) -> dict[str, Any]:
    """Inputs for the start node's variables: the case's, else by type."""
    out: dict[str, Any] = {"query": "hello"}  # sys.query of chatflows
    for node in doc.get("workflow", {}).get("graph", {}).get("nodes", []):
        data = node.get("data", {})
        if data.get("type") != "start":
            continue
        for v in data.get("variables", []):
            t = v.get("type")
            if t == "number":
                out[v["variable"]] = 1
            elif t == "select":
                opts = v.get("options") or ["a"]
                out[v["variable"]] = opts[0]
            elif t in ("checkbox", "boolean"):
                out[v["variable"]] = True
            elif t in ("file-list", "file"):
                continue
            else:
                out[v["variable"]] = "hello"
    return out


def plain(v: Any) -> Any:
    """Segments and other graphon values as plain JSON values."""
    if hasattr(v, "to_object"):
        return plain(v.to_object())
    if hasattr(v, "model_dump"):
        return plain(v.model_dump(mode="json"))
    if isinstance(v, dict):
        return {str(k): plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [plain(x) for x in v]
    return v


def main() -> None:
    fixture = Path(sys.argv[1])
    case = json.loads(Path(sys.argv[2]).read_text()) if len(sys.argv) > 2 else {}
    install_mocks(case)
    dsl = fixture.read_text()
    inputs = default_inputs(yaml.safe_load(dsl))
    inputs.update(case.get("inputs", {}))
    engine = loads(dsl, start_inputs=inputs, run_context={"workflow_execution_id": "harness-run"})
    result_inputs = inputs
    trace: list[dict[str, Any]] = []
    result: dict[str, Any] = {"fixture": fixture.name, "inputs": result_inputs, "case": case,
                              "workflow": yaml.safe_load(dsl).get("workflow", {})}
    try:
        _collect(engine.run(), trace, result)
    except Exception as e:  # graphon raises a failed run's error after its events
        result.setdefault("status", "failed")
        result.setdefault("error", str(e))
    result["trace"] = trace
    json.dump(result, sys.stdout, ensure_ascii=False, indent=1, default=str)
    print()


def _collect(events: Any, trace: list[dict[str, Any]], result: dict[str, Any]) -> None:
    for ev in events:
        name = type(ev).__name__
        rec: dict[str, Any] = {"event": name}
        if isinstance(ev, node_events.GraphNodeEventBase):
            rec["node"] = ev.node_id
        if isinstance(ev, node_events.NodeRunSucceededEvent | node_events.NodeRunExceptionEvent):
            rec["outputs"] = plain(dict(ev.node_run_result.outputs))
        if isinstance(ev, graph_events.GraphRunSucceededEvent | graph_events.GraphRunPartialSucceededEvent):
            result["status"] = "succeeded"
            result["outputs"] = plain(dict(ev.outputs))
        if isinstance(ev, graph_events.GraphRunFailedEvent):
            result["status"] = "failed"
            result["error"] = ev.error
        trace.append(rec)


if __name__ == "__main__":
    main()
