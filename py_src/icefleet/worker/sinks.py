"""Where a task's rows go. Phase 1 has the `result` sink; shuffle and Iceberg files follow.

One `COPY` statement does the work (P4: DuckDB moves the bytes). `RETURN_STATS` hands back
every file written with its row count and size, which is the task's manifest -- no
listing afterwards, even on S3. A task that produces no rows still writes one empty file
carrying the schema, so the coordinator always has something typed to read.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from icetl.paths import is_object_store

from icefleet.plan.tasks import OutputFile

if TYPE_CHECKING:
    from collections.abc import Mapping

    from icefleet.plan.tasks import SinkSpec

__all__ = ["write_result"]


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def write_result(
    cursor: Any, sql: str, parameters: Mapping[str, Any], sink: SinkSpec
) -> list[OutputFile]:
    """Run `sql` and write its rows as parquet files into the directory `sink.location`.

    The directory must not exist yet: DuckDB refuses to write into a non-empty one, which
    is the guard that keeps one attempt from mixing with another's leftovers (P5).
    """
    location = sink.location
    if not is_object_store(location):
        # DuckDB creates the task directory itself, but not its parents.
        Path(location).parent.mkdir(parents=True, exist_ok=True)
    copy = (
        f"COPY ({sql}) TO {_quote(location)} "
        f"(FORMAT parquet, COMPRESSION zstd, FILE_SIZE_BYTES {int(sink.target_file_bytes)}, "
        f"RETURN_STATS true)"
    )
    rows = cursor.execute(copy, dict(parameters)).fetchall()
    return [OutputFile(path=str(row[0]), rows=int(row[1]), bytes=int(row[2])) for row in rows]
