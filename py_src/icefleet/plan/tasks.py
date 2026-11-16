"""`TaskSpec` and `TaskResult` -- the wire format between coordinator and worker (PLAN §3.3).

JSON over HTTP, deliberately boring. Bytes travel as base64 (only `schema_ipc`, used when
an input has no files, which Phase 2's empty shuffle partitions need). Workers are
stateless between tasks: everything a task needs is in its spec, except credentials,
which a worker takes from its own environment (decision 20).
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

__all__ = ["InputSpec", "OutputFile", "SinkSpec", "TaskResult", "TaskSpec"]

SINK_KINDS = ("result",)  # "shuffle" arrives in Phase 2, "iceberg_files" in Phase 5


def _require(data: Mapping[str, Any], key: str, kind: type | tuple[type, ...]) -> Any:
    if key not in data:
        raise ValueError(f"missing field {key!r}")
    value = data[key]
    if not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
        raise ValueError(f"field {key!r} has type {type(value).__name__}")
    return value


@dataclass(frozen=True)
class InputSpec:
    """The files bound to one `$placeholder` of a stage's SQL."""

    paths: tuple[str, ...]
    schema_ipc: bytes | None = None

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"paths": list(self.paths)}
        if self.schema_ipc is not None:
            out["schema_ipc"] = base64.b64encode(self.schema_ipc).decode("ascii")
        return out

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> InputSpec:
        paths = _require(data, "paths", list)
        if not all(isinstance(p, str) for p in paths):
            raise ValueError("every input path must be a string")
        raw = data.get("schema_ipc")
        return cls(
            paths=tuple(paths),
            schema_ipc=base64.b64decode(raw, validate=True) if isinstance(raw, str) else None,
        )


@dataclass(frozen=True)
class SinkSpec:
    """Where a task's output goes. Phase 1 has one kind, `result`: parquet under `location`."""

    kind: str
    location: str
    target_file_bytes: int = 256 * 1024**2

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "location": self.location,
            "target_file_bytes": self.target_file_bytes,
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> SinkSpec:
        kind = _require(data, "kind", str)
        if kind not in SINK_KINDS:
            raise ValueError(f"unknown sink kind {kind!r}")
        return cls(
            kind=kind,
            location=_require(data, "location", str),
            target_file_bytes=int(data.get("target_file_bytes", 256 * 1024**2)),
        )


@dataclass(frozen=True)
class TaskSpec:
    """One task: run `sql` with `inputs` bound to its placeholders, write to `sink`."""

    query_id: str
    stage_id: int
    task_index: int
    sql: str
    inputs: Mapping[str, InputSpec]
    sink: SinkSpec
    attempt: int = 0

    @property
    def key(self) -> str:
        """`q-1a2b/stage-0/task-3`, how a task is named in logs and errors."""
        return f"{self.query_id}/stage-{self.stage_id}/task-{self.task_index}"

    def to_json(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "stage_id": self.stage_id,
            "task_index": self.task_index,
            "attempt": self.attempt,
            "sql": self.sql,
            "inputs": {name: spec.to_json() for name, spec in self.inputs.items()},
            "sink": self.sink.to_json(),
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> TaskSpec:
        inputs = _require(data, "inputs", dict)
        return cls(
            query_id=_require(data, "query_id", str),
            stage_id=_require(data, "stage_id", int),
            task_index=_require(data, "task_index", int),
            attempt=int(data.get("attempt", 0)),
            sql=_require(data, "sql", str),
            inputs={str(name): InputSpec.from_json(spec) for name, spec in inputs.items()},
            sink=SinkSpec.from_json(_require(data, "sink", dict)),
        )


@dataclass(frozen=True)
class OutputFile:
    """One file a task wrote, as DuckDB's `RETURN_STATS` reported it."""

    path: str
    bytes: int
    rows: int

    def to_json(self) -> dict[str, Any]:
        return {"path": self.path, "bytes": self.bytes, "rows": self.rows}

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> OutputFile:
        return cls(
            path=_require(data, "path", str),
            bytes=_require(data, "bytes", int),
            rows=_require(data, "rows", int),
        )


@dataclass(frozen=True)
class TaskResult:
    """What a finished task reports: its output manifest and what it cost."""

    query_id: str
    stage_id: int
    task_index: int
    attempt: int
    worker: str
    seconds: float
    manifest: tuple[OutputFile, ...] = field(default_factory=tuple)

    @property
    def rows_out(self) -> int:
        return sum(f.rows for f in self.manifest)

    @property
    def bytes_out(self) -> int:
        return sum(f.bytes for f in self.manifest)

    def to_json(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "stage_id": self.stage_id,
            "task_index": self.task_index,
            "attempt": self.attempt,
            "worker": self.worker,
            "seconds": self.seconds,
            "rows_out": self.rows_out,
            "bytes_out": self.bytes_out,
            "manifest": [f.to_json() for f in self.manifest],
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> TaskResult:
        return cls(
            query_id=_require(data, "query_id", str),
            stage_id=_require(data, "stage_id", int),
            task_index=_require(data, "task_index", int),
            attempt=int(data.get("attempt", 0)),
            worker=str(data.get("worker", "")),
            seconds=float(_require(data, "seconds", (int, float))),
            manifest=tuple(OutputFile.from_json(f) for f in _require(data, "manifest", list)),
        )
