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

__all__ = ["write_iceberg_files", "write_result", "write_shuffle"]

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
    written = [OutputFile(path=str(row[0]), rows=int(row[1]), bytes=int(row[2])) for row in rows]
    if sink.sample_columns:
        written.append(_write_sample(cursor, written, sink))
    return written


def _write_sample(cursor: Any, written: list[OutputFile], sink: SinkSpec) -> OutputFile:
    """A reservoir sample of the sort keys, read back from the rows just written (one
    pass over the query, a cheap second one over its key columns) -- a range sort's
    boundaries come from these (`plan/sort.py`)."""
    columns = ", ".join('"' + c.replace('"', '""') + '"' for c in sink.sample_columns)
    files = "[" + ", ".join(_quote(f.path) for f in written) + "]"
    path = sink.location.rstrip("/") + "/sample.parquet"
    copy = (
        f"COPY (SELECT {columns} FROM read_parquet({files}, union_by_name => false, "
        f"hive_partitioning => false) USING SAMPLE reservoir({int(sink.sample_rows)} ROWS) "
        f"REPEATABLE (42)) TO {_quote(path)} (FORMAT parquet, RETURN_STATS true)"
    )
    row = cursor.execute(copy).fetchall()[0]
    return OutputFile(path=str(row[0]), rows=int(row[1]), bytes=int(row[2]), sample=True)


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


def write_iceberg_files(
    cursor: Any, sql: str, parameters: Mapping[str, Any], sink: SinkSpec
) -> list[OutputFile]:
    """Run `sql` and write its rows as Iceberg data files under `sink.location` (Phase 5).

    `sink.field_ids` carries the table's field ids, so the files read back by id, the
    way Iceberg reads (and `add_files` checks them against the table's schema). With
    `partition_columns`, rows are written one directory per value of those computed
    grouping columns, which are left out of the files: every file holds one partition,
    which is what `add_files` reads its partition value from. Without, the rows roll
    into files of `target_file_bytes`. A file with no rows is not reported: it is not
    worth a manifest entry.
    """
    location = sink.location
    _prepare(location)
    options = ["FORMAT parquet", "COMPRESSION zstd"]
    if sink.partition_columns:
        listing = ", ".join('"' + c.replace('"', '""') + '"' for c in sink.partition_columns)
        options += [f"PARTITION_BY ({listing})", "WRITE_PARTITION_COLUMNS false"]
    else:
        options.append(f"FILE_SIZE_BYTES {int(sink.target_file_bytes)}")
    if sink.field_ids:
        options.append(f"FIELD_IDS {sink.field_ids}")
    options.append("RETURN_STATS true")
    copy = f"COPY ({sql}) TO {_quote(location)} ({', '.join(options)})"
    rows = cursor.execute(copy, dict(parameters)).fetchall()
    return [
        OutputFile(path=str(row[0]), rows=int(row[1]), bytes=int(row[2]))
        for row in rows
        if int(row[1]) > 0
    ]
