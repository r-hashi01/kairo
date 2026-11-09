"""The graph API (ADR 0049): a thin client of kairod's HTTP API (/v1/...)."""

from __future__ import annotations

import json
import http.client
import email.utils
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DONE = ("completed", "failed", "cancelled")


def _retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        at = email.utils.parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError, OverflowError):
        return None
    return max(0.0, at - time.time())


class KairoError(Exception):
    def __init__(self, status: int, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


class KairoTransportError(Exception):
    """A request may have reached kairod, but its response was not read."""


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
                status, text, headers = res.status, res.read(), res.headers
        except urllib.error.HTTPError as e:
            with e:
                status, text, headers = e.code, e.read(), e.headers
        except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as e:
            raise KairoTransportError(str(e)) from e
        out = json.loads(text) if text else None
        if status >= 400 and status != 504:
            msg = out.get("error") if isinstance(out, dict) else None
            raise KairoError(status, msg or f"{method} {path}: {status}", _retry_after(headers.get("Retry-After")))
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
        keep_output: bool = False,
        parent: str | None = None,
        workflow: str | None = None,
        meta: dict[str, Any] | None = None,
        drive: bool = False,
    ) -> dict[str, Any]:
        """Starts a run of plan once its start is durable. With a run id already
        running or recently finished, nothing new starts ("existing").
        keep_output: its output stays after it finishes, across restarts, for
        the idempotency period (ADR 0050)."""
        body: dict[str, Any] = {"plan": plan, "input": input, "tenant": tenant}
        if run_id:
            body["run_id"] = run_id
        if tier:
            body["tier"] = tier
        if vars:
            body["vars"] = vars
        if keep_output:
            body["keep_output"] = True
        if parent:
            body["parent"] = parent
        if workflow:
            body["workflow"] = workflow
        if meta is not None:
            body["meta"] = meta
        if drive:
            body["drive"] = True
        status, out = self._call("POST", "/v1/runs", body)
        if status == 504:
            raise KairoError(504, f"run {out.get('run_id')}: start not confirmed")
        return out

    def get(self, run_id: str, *, input: bool = False) -> dict[str, Any]:
        """With input: the run's input too, as it was submitted (a run not
        finished, ADR 0060)."""
        return self._call("GET", f"/v1/runs/{self._id(run_id)}" + ("?input=true" if input else ""))[1]

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

    def wait_quiescent(self, run_id: str, poll: str = "60s") -> dict[str, Any]:
        """Waits until a run is terminal or has no executor work in flight."""
        while True:
            status, out = self._call("GET", f"/v1/runs/{self._id(run_id)}/quiescent?timeout={poll}")
            if status == 200:
                return out

    def claim_workflow(
        self, *, owner: str, lease_ms: int, workflows: dict[str, list[str]], run_id: str = "", timeout_ms: int = 0
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"owner": owner, "lease_ms": lease_ms, "timeout_ms": timeout_ms, "limit": 1, "workflows": workflows}
        if run_id:
            body["run_id"] = run_id
        return self._call("POST", "/v1/workflow-drives/claim", body)[1]

    def renew_workflow(self, run_id: str, *, owner: str, token: str, lease_ms: int) -> None:
        self._call("POST", f"/v1/workflow-drives/{self._id(run_id)}/renew", {"owner": owner, "token": token, "lease_ms": lease_ms})

    def release_workflow(self, run_id: str, *, owner: str, token: str, retry: bool = False) -> None:
        self._call("POST", f"/v1/workflow-drives/{self._id(run_id)}/release", {"owner": owner, "token": token, "retry": retry})

    def signal(self, run_id: str, name: str, payload: Any = None) -> None:
        self._call("POST", f"/v1/runs/{self._id(run_id)}/signals/{urllib.parse.quote(name, safe='')}", payload)

    def cancel(self, run_id: str) -> None:
        self._call("POST", f"/v1/runs/{self._id(run_id)}/cancel")

    def children(self, run_id: str) -> list[dict[str, Any]]:
        return self._call("GET", f"/v1/runs/{self._id(run_id)}/children")[1]

    def list(
        self,
        *,
        workflow: str | None = None,
        status: str | None = None,
        since: int | None = None,
        until: int | None = None,
        after: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        q = urllib.parse.urlencode(
            {k: v for k, v in {"workflow": workflow, "status": status, "since": since, "until": until, "after": after, "limit": limit}.items() if v is not None and v != ""}
        )
        return self._call("GET", "/v1/runs" + (f"?{q}" if q else ""))[1]

    def finish(self, run_id: str, payload: Any) -> None:
        self._call("POST", f"/v1/runs/{self._id(run_id)}/finish", payload)

    def resolve(self, run_id: str, act: int, output: Any = None, error: str | None = None) -> None:
        """Settles one step stopped for review; kairod accepts it asynchronously."""
        body: dict[str, Any] = {"act": act, "output": output}
        if error:
            body["error"] = error
        self._call("POST", f"/v1/runs/{self._id(run_id)}/resolve", body)
