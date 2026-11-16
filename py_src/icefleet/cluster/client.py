"""The coordinator's side of the control plane: an HTTP client per worker (urllib, decision 26).

The handshake is `GET /ping`: a worker whose protocol, icefleet or DuckDB version differs
from the coordinator's is refused, because DuckDB's `hash()` -- partition identity from
Phase 2 on -- is not promised stable across versions (PLAN §4.1).
"""

from __future__ import annotations

import contextlib
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from http.client import HTTPException
from typing import TYPE_CHECKING, Any

from icefleet._versions import mismatches, runtime_versions
from icefleet.errors import ClusterError, WorkerError
from icefleet.plan.tasks import TaskResult

if TYPE_CHECKING:
    from icefleet.cluster.launcher import LocalCluster
    from icefleet.plan.tasks import TaskSpec

__all__ = ["Cluster", "WorkerClient", "WorkerInfo", "handshake"]


class WorkerClient:
    """Talks to one worker."""

    def __init__(self, endpoint: str, *, task_timeout: float | None = None) -> None:
        self.endpoint = endpoint
        self._base = f"http://{endpoint}"
        self._task_timeout = task_timeout

    def __repr__(self) -> str:
        return f"WorkerClient({self.endpoint})"

    def _request(
        self, method: str, path: str, body: dict[str, Any] | None, timeout: float | None
    ) -> dict[str, Any]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self._base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"} if data is not None else {},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as exc:
            try:
                detail = json.loads(exc.read() or b"{}")
            except ValueError:
                detail = {}
            message = detail.get("error") or f"HTTP {exc.code}"
            raise WorkerError(str(message), worker=self.endpoint) from exc
        except (urllib.error.URLError, HTTPException, OSError, ValueError) as exc:
            reason = getattr(exc, "reason", exc)
            raise WorkerError(
                f"lost contact with the worker ({type(exc).__name__}: {reason})",
                worker=self.endpoint,
            ) from exc
        if not isinstance(payload, dict):
            raise WorkerError("the worker answered with something other than an object")
        return payload

    def ping(self, timeout: float = 5.0) -> dict[str, Any]:
        return self._request("GET", "/ping", None, timeout)

    def run_task(self, spec: TaskSpec) -> TaskResult:
        """Run one task; blocks until it ends. Raises `WorkerError` naming worker and task."""
        try:
            payload = self._request("POST", "/tasks", spec.to_json(), self._task_timeout)
        except WorkerError as exc:
            raise WorkerError(exc.detail, worker=self.endpoint, task=spec.key) from exc
        try:
            return TaskResult.from_json(payload)
        except (ValueError, TypeError) as exc:
            raise WorkerError(
                f"malformed TaskResult: {exc}", worker=self.endpoint, task=spec.key
            ) from exc

    def cancel(self, query_id: str) -> None:
        """Best effort: a worker that cannot be reached has nothing running for us anyway."""
        with contextlib.suppress(WorkerError):
            self._request("POST", "/cancel", {"query_id": query_id}, 5.0)


@dataclass(frozen=True)
class WorkerInfo:
    """A worker that passed the handshake."""

    client: WorkerClient
    slots: int
    versions: dict[str, str]

    @property
    def endpoint(self) -> str:
        return self.client.endpoint


def handshake(client: WorkerClient) -> WorkerInfo:
    """Ping `client` and refuse it if its versions differ where they must not."""
    try:
        reply = client.ping()
    except WorkerError as exc:
        raise ClusterError(f"worker {client.endpoint} did not answer the handshake: {exc}") from exc
    versions = {str(k): str(v) for k, v in reply.items()}
    wrong = mismatches(runtime_versions(), versions)
    if wrong:
        raise ClusterError(
            f"worker {client.endpoint} refused: version mismatch ({'; '.join(wrong)}). Every "
            f"worker must run the coordinator's icefleet and DuckDB -- DuckDB's hash() is not "
            f"stable across versions, so a mixed fleet could partition rows inconsistently."
        )
    slots = int(reply.get("slots", 1))
    return WorkerInfo(client=client, slots=max(slots, 1), versions=versions)


class Cluster:
    """The workers a session runs on, after the handshake."""

    def __init__(
        self, workers: list[WorkerInfo], *, description: str, launcher: LocalCluster | None = None
    ) -> None:
        if not workers:
            raise ClusterError(f"{description}: no workers.")
        self.workers = workers
        self.description = description
        self._launcher = launcher

    @property
    def total_slots(self) -> int:
        return sum(w.slots for w in self.workers)

    def slot_clients(self) -> list[WorkerClient]:
        """One entry per slot, interleaved across workers so tasks spread evenly."""
        out: list[WorkerClient] = []
        for round_ in range(max(w.slots for w in self.workers)):
            out += [w.client for w in self.workers if w.slots > round_]
        return out

    def summary(self) -> str:
        slots = {w.slots for w in self.workers}
        per = f"{slots.pop()} slots" if len(slots) == 1 else f"{self.total_slots} slots in all"
        return f"{self.description} · {len(self.workers)} workers x {per}"

    @property
    def processes(self) -> list[Any]:
        """The local worker processes, for `local[N]` (tests kill one to prove failure)."""
        return [] if self._launcher is None else self._launcher.processes

    def close(self) -> None:
        if self._launcher is not None:
            self._launcher.stop()
            self._launcher = None
