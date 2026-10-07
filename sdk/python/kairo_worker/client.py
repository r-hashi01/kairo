"""The graph API (ADR 0049): a thin client of kairod's HTTP API (/v1/...)."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DONE = ("completed", "failed", "cancelled")


class KairoError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


def finished(run: dict[str, Any]) -> bool:
    """Whether a run (engine.RunInfo) has finished."""
    return run.get("status") in DONE


class Client:
    """Calls kairod's HTTP API. Blocking; the workflow API runs it on threads."""

    def __init__(self, url: str = "http://127.0.0.1:8420", timeout: float = 90.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.url + path, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as res:
                status, text = res.status, res.read()
        except urllib.error.HTTPError as e:
            with e:
                status, text = e.code, e.read()
        out = json.loads(text) if text else None
        if status >= 400 and status != 504:
            msg = out.get("error") if isinstance(out, dict) else None
            raise KairoError(status, msg or f"{method} {path}: {status}")
        return status, out

    @staticmethod
    def _id(run_id: str) -> str:
        return urllib.parse.quote(run_id, safe="")

    def register_nodes(self, specs: list[dict[str, Any]]) -> None:
        """Registers node specs (ir.NodeSpec): effect is "real" (the default) or "unprotected"."""
        self._call("POST", "/v1/nodes", specs)

    def register_plan(self, definition: dict[str, Any]) -> dict[str, Any]:
        """Registers a plan (a workflow definition, ir.Definition)."""
        return self._call("POST", "/v1/plans", definition)[1]

    def run(
        self,
        plan: str,
        input: Any,
        *,
        run_id: str = "",
        tenant: str = "default",
        tier: str = "",
        vars: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Starts a run of plan once its start is durable. With a run id already
        running or recently finished, nothing new starts ("existing")."""
        body: dict[str, Any] = {"plan": plan, "input": input, "tenant": tenant}
        if run_id:
            body["run_id"] = run_id
        if tier:
            body["tier"] = tier
        if vars:
            body["vars"] = vars
        status, out = self._call("POST", "/v1/runs", body)
        if status == 504:
            raise KairoError(504, f"run {out.get('run_id')}: start not confirmed")
        return out

    def get(self, run_id: str) -> dict[str, Any]:
        return self._call("GET", f"/v1/runs/{self._id(run_id)}")[1]

    def wait(self, run_id: str, poll: str = "10s") -> dict[str, Any]:
        """Waits until the run has finished."""
        while True:
            status, out = self._call("GET", f"/v1/runs/{self._id(run_id)}/wait?timeout={poll}")
            if status == 200:
                return out

    def wait_once(self, run_id: str, poll: str = "10s") -> dict[str, Any] | None:
        """The finished run, or None if it is still going after poll."""
        status, out = self._call("GET", f"/v1/runs/{self._id(run_id)}/wait?timeout={poll}")
        return out if status == 200 else None

    def signal(self, run_id: str, name: str, payload: Any = None) -> None:
        self._call("POST", f"/v1/runs/{self._id(run_id)}/signals/{urllib.parse.quote(name, safe='')}", payload)

    def cancel(self, run_id: str) -> None:
        self._call("POST", f"/v1/runs/{self._id(run_id)}/cancel")
