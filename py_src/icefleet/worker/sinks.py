"""Where a task's rows go: the `result` and `shuffle` sinks; Iceberg data files are Phase 5.

One `COPY` statement does the work (P4: DuckDB moves the bytes). `RETURN_STATS` hands back
every file written with its row count and size, which is the task's manifest -- no
listing afterwards, even on S3. A task that produces no rows still writes one empty file
carrying the schema, so the coordinator always has something typed to read.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from icetl.paths import is_object_store

from icefleet.plan.tasks import PARTITION_COLUMN, OutputFile

if TYPE_CHECKING:
    from collections.abc import Mapping

    from icefleet.plan.tasks import SinkSpec

__all__ = ["write_result", "write_shuffle"]

_PARTITION_DIR = re.compile(r"(?:^|/)__part=(\d+)/")


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _prepare(location: str) -> None:
    if not is_object_store(location):
        # DuckDB creates the task directory itself, but not its parents.
        Path(location).parent.mkdir(parents=True, exist_ok=True)


def write_result(
    cursor: Any, sql: str, parameters: Mapping[str, Any], sink: SinkSpec
) -> list[OutputFile]:
    """Run `sql` and write its rows as parquet files into the directory `sink.location`.

    The directory must not exist yet: DuckDB refuses to write into a non-empty one, which
    is the guard that keeps one attempt from mixing with another's leftovers (P5).
    """
    location = sink.location
    _prepare(location)
    copy = (
        f"COPY ({sql}) TO {_quote(location)} "
        f"(FORMAT parquet, COMPRESSION zstd, FILE_SIZE_BYTES {int(sink.target_file_bytes)}, "
        f"RETURN_STATS true)"
    )
    rows = cursor.execute(copy, dict(parameters)).fetchall()
    return [OutputFile(path=str(row[0]), rows=int(row[1]), bytes=int(row[2])) for row in rows]


def write_shuffle(
    cursor: Any, sql: str, parameters: Mapping[str, Any], sink: SinkSpec
) -> list[OutputFile]:
    """Run `sql` and write its rows hash-partitioned under `sink.location` (PLAN §4.4).

    With more than one partition the SQL carries `__part` and DuckDB writes
    `<location>/__part=<p>/data_<n>.parquet`; the partition column itself is not written
    into the files. One partition is a plain copy, every file partition 0. A task with no
    rows writes nothing and reports an empty manifest -- the coordinator skips a partition
    no task wrote to. `FILE_SIZE_BYTES` cannot be combined with `PARTITION_BY` (DuckDB
    1.5.6), so partitioned files are not rolled.
    """
    location = sink.location
    _prepare(location)
    if sink.partitions <= 1:
        copy = (
            f"COPY ({sql}) TO {_quote(location)} "
            f"(FORMAT parquet, COMPRESSION zstd, FILE_SIZE_BYTES {int(sink.target_file_bytes)}, "
            f"RETURN_STATS true)"
        )
        rows = cursor.execute(copy, dict(parameters)).fetchall()
        return [
            OutputFile(path=str(row[0]), rows=int(row[1]), bytes=int(row[2]), partition=0)
            for row in rows
            if int(row[1]) > 0
        ]
    copy = (
        f"COPY ({sql}) TO {_quote(location)} "
        f"(FORMAT parquet, COMPRESSION zstd, PARTITION_BY ({PARTITION_COLUMN}), "
        f"WRITE_PARTITION_COLUMNS false, RETURN_STATS true)"
    )
    rows = cursor.execute(copy, dict(parameters)).fetchall()
    out: list[OutputFile] = []
    for row in rows:
        path = str(row[0]).replace("\\", "/")
        match = _PARTITION_DIR.search(path)
        if match is None:
            raise ValueError(f"a shuffle file outside a {PARTITION_COLUMN}= directory: {row[0]}")
        out.append(
            OutputFile(
                path=str(row[0]), rows=int(row[1]), bytes=int(row[2]), partition=int(match.group(1))
            )
        )
    return out
