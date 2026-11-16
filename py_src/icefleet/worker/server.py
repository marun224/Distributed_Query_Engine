"""The worker's HTTP control plane: stdlib only (decision 26).

    GET  /ping     versions and slot use -- the handshake
    POST /tasks    a TaskSpec; blocks until the task ends; 200 + TaskResult, or an error
    POST /cancel   {"query_id": ...}; interrupts that query's running tasks

A thread per connection (`ThreadingHTTPServer`), and a semaphore of `slots`: a task that
arrives when every slot is busy gets 503 rather than a queue, because the coordinator
does the slot accounting and a 503 means it got that wrong.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from icefleet._versions import runtime_versions
from icefleet.plan.tasks import TaskSpec
from icefleet.worker.runner import TaskCancelled, TaskFailed, TaskRunner

__all__ = ["WorkerServer"]

logger = logging.getLogger(__name__)

#: A task spec is SQL plus file lists; 256 MiB is far beyond any real one.
MAX_BODY_BYTES = 256 * 1024**2


class WorkerServer(ThreadingHTTPServer):
    """One worker: an HTTP server in front of a `TaskRunner`."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], runner: TaskRunner, slots: int) -> None:
        super().__init__(address, _Handler)
        self.runner = runner
        self.slots = slots
        self._free = threading.BoundedSemaphore(slots)
        self.versions = runtime_versions()

    @property
    def endpoint(self) -> str:
        host, port = self.server_address[:2]
        return f"{host!s}:{port}"

    def try_acquire(self) -> bool:
        return self._free.acquire(blocking=False)

    def release(self) -> None:
        self._free.release()

    def ping(self) -> dict[str, Any]:
        return {
            **self.versions,
            "slots": self.slots,
            "busy": self.runner.running(),
            "pid": os.getpid(),
            "worker": self.runner.worker_id,
        }


class _Handler(BaseHTTPRequestHandler):
    server: WorkerServer
    server_version = "icefleet-worker"

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("%s - %s", self.address_string(), format % args)

    def _reply(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: HTTPStatus, message: str, kind: str) -> None:
        self._reply(status, {"error": message, "type": kind})

    def _body(self) -> dict[str, Any] | None:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_BODY_BYTES:
            self._error(HTTPStatus.BAD_REQUEST, f"body length {length}", "BadRequest")
            return None
        try:
            data = json.loads(self.rfile.read(length))
        except (ValueError, UnicodeDecodeError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, f"not JSON: {exc}", "BadRequest")
            return None
        if not isinstance(data, dict):
            self._error(HTTPStatus.BAD_REQUEST, "expected a JSON object", "BadRequest")
            return None
        return data

    def do_GET(self) -> None:
        if self.path == "/ping":
            self._reply(HTTPStatus.OK, self.server.ping())
        else:
            self._error(HTTPStatus.NOT_FOUND, f"no route {self.path}", "NotFound")

    def do_POST(self) -> None:
        if self.path == "/tasks":
            self._tasks()
        elif self.path == "/cancel":
            data = self._body()
            if data is None:
                return
            query_id = data.get("query_id")
            if not isinstance(query_id, str):
                self._error(HTTPStatus.BAD_REQUEST, "query_id must be a string", "BadRequest")
                return
            self._reply(HTTPStatus.OK, {"cancelled": self.server.runner.cancel(query_id)})
        else:
            self._error(HTTPStatus.NOT_FOUND, f"no route {self.path}", "NotFound")

    def _tasks(self) -> None:
        data = self._body()
        if data is None:
            return
        try:
            spec = TaskSpec.from_json(data)
        except (ValueError, TypeError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, f"bad task spec: {exc}", "BadRequest")
            return
        if not self.server.try_acquire():
            self._error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                f"all {self.server.slots} slots are busy",
                "Busy",
            )
            return
        try:
            result = self.server.runner.run(spec)
        except TaskCancelled as exc:
            self._error(HTTPStatus.CONFLICT, str(exc), "TaskCancelled")
        except TaskFailed as exc:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc), "TaskFailed")
        except Exception as exc:  # anything else is a worker bug; report it, stay up
            logger.exception("task %s crashed", spec.key)
            self._error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                f"{spec.key}: {type(exc).__name__}: {exc}",
                type(exc).__name__,
            )
        else:
            self._reply(HTTPStatus.OK, result.to_json())
        finally:
            self.server.release()
