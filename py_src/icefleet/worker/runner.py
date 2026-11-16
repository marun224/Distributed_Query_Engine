"""Executes a `TaskSpec` on icetl's `DuckDBEngine` (PLAN §3.1, Worker).

One engine per worker process, shared by every slot: DuckDB parallelises inside a task,
and each task runs on its own thread's cursor, which is how DuckDB supports concurrent
queries on one database (decision 19). icetl's engine brings the httpfs loading and the
S3 secret, so a worker reads object storage exactly as a single-node session does.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import duckdb

from icefleet.plan.tasks import TaskResult
from icefleet.worker.sinks import write_result, write_shuffle

if TYPE_CHECKING:
    from icetl.exec.engine import DuckDBEngine

    from icefleet.plan.tasks import TaskSpec

__all__ = ["TaskCancelled", "TaskFailed", "TaskRunner"]

logger = logging.getLogger(__name__)

MAX_REMEMBERED_CANCELLATIONS = 4096


class TaskFailed(Exception):
    """A task's SQL failed. The message is DuckDB's, prefixed with the task."""


class TaskCancelled(TaskFailed):
    """A task was interrupted by `POST /cancel`."""


class TaskRunner:
    """Runs tasks, and can interrupt the ones belonging to a query."""

    def __init__(self, engine: DuckDBEngine, worker_id: str) -> None:
        self._engine = engine
        self.worker_id = worker_id
        self._lock = threading.Lock()
        self._running: dict[str, list[Any]] = defaultdict(list)
        # Insertion-ordered, so the oldest cancellations can be forgotten: a long-lived
        # worker must not grow without bound, and query ids are never reused.
        self._cancelled: dict[str, None] = {}

    def run(self, spec: TaskSpec) -> TaskResult:
        for name, source in spec.inputs.items():
            if not source.paths:
                # An input with no rows is rendered into the stage SQL as a typed empty
                # relation by the coordinator; a bound input without files is its bug.
                raise TaskFailed(f"{spec.key}: input {name!r} has no files")

        locations = [p for source in spec.inputs.values() for p in source.paths]
        self._engine.ensure_object_store([*locations, spec.sink.location])
        cursor = self._engine.cursor()
        parameters = {name: list(source.paths) for name, source in spec.inputs.items()}

        with self._lock:
            if spec.query_id in self._cancelled:
                raise TaskCancelled(f"{spec.key}: the query was cancelled")
            self._running[spec.query_id].append(cursor)
        started = time.perf_counter()
        try:
            if spec.sink.kind == "result":
                manifest = write_result(cursor, spec.sql, parameters, spec.sink)
            elif spec.sink.kind == "shuffle":
                manifest = write_shuffle(cursor, spec.sql, parameters, spec.sink)
            else:  # pragma: no cover - TaskSpec.from_json refuses it
                raise TaskFailed(f"{spec.key}: sink {spec.sink.kind!r} is not implemented")
        except duckdb.Error as exc:
            with self._lock:
                cancelled = spec.query_id in self._cancelled
            if cancelled:
                raise TaskCancelled(f"{spec.key}: cancelled") from exc
            raise TaskFailed(f"{spec.key}: {type(exc).__name__}: {exc}") from exc
        finally:
            with self._lock:
                running = self._running.get(spec.query_id, [])
                if cursor in running:
                    running.remove(cursor)
                if not running:
                    self._running.pop(spec.query_id, None)
        seconds = time.perf_counter() - started
        logger.info("%s done in %.2fs, %d file(s)", spec.key, seconds, len(manifest))
        return TaskResult(
            query_id=spec.query_id,
            stage_id=spec.stage_id,
            task_index=spec.task_index,
            attempt=spec.attempt,
            worker=self.worker_id,
            seconds=seconds,
            manifest=tuple(manifest),
        )

    def cancel(self, query_id: str) -> int:
        """Interrupt every running task of `query_id`; refuse its tasks still to come."""
        with self._lock:
            self._cancelled[query_id] = None
            while len(self._cancelled) > MAX_REMEMBERED_CANCELLATIONS:
                del self._cancelled[next(iter(self._cancelled))]
            cursors = list(self._running.get(query_id, []))
        for cursor in cursors:
            with contextlib.suppress(duckdb.Error):  # already finished: nothing to interrupt
                cursor.interrupt()
        return len(cursors)

    def running(self) -> int:
        with self._lock:
            return sum(len(c) for c in self._running.values())
