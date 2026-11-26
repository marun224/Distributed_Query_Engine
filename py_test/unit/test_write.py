"""The write target, the commit and the data-file sink (Phase 5), without a cluster."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyiceberg import transforms as tr
from pyiceberg import types as t
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.schema import Schema

from icefleet.exchange import open_exchange
from icefleet.plan.tasks import SinkSpec
from icefleet.worker.sinks import write_iceberg_files
from icefleet.write.commit import commit_files, partition_filter, replace_files
from icefleet.write.target import _partition_value, duckdb_type, field_ids_of, iceberg_path

# -- partition values: DuckDB's grouping equals Iceberg's transform ----------------------

TRANSFORMS: list[Any] = [
    tr.YearTransform(),
    tr.MonthTransform(),
    tr.DayTransform(),
    tr.HourTransform(),
]
EPOCH = datetime(1970, 1, 1)


@settings(max_examples=60, deadline=None)
@given(
    micros=st.integers(-(10**15), 10**15),
    transform=st.sampled_from(TRANSFORMS),
    tz=st.booleans(),
)
def test_a_time_partition_value_is_iceberg_s(micros: int, transform: Any, tz: bool) -> None:
    """The ordinal DuckDB computes for a row is the one PyIceberg's transform gives --
    years / months / days / hours since 1970, in UTC -- either side of the epoch."""
    field_type = t.TimestamptzType() if tz else t.TimestampType()
    sql = _partition_value(transform, "c", field_type)
    assert sql is not None
    con = duckdb.connect()
    con.execute("SET TimeZone = 'Asia/Kolkata'")  # a session zone must not leak in
    value = EPOCH + timedelta(microseconds=micros)
    if tz:
        con.execute(
            "CREATE TABLE x AS SELECT CAST(? AS TIMESTAMPTZ) AS c", [value.replace(tzinfo=UTC)]
        )
    else:
        con.execute("CREATE TABLE x AS SELECT CAST(? AS TIMESTAMP) AS c", [value])
    row = con.execute(f"SELECT {sql} FROM x").fetchone()
    assert row is not None
    assert row[0] == transform.transform(field_type)(micros)


@pytest.mark.parametrize("transform", TRANSFORMS[:3])
def test_a_date_partition_value_is_iceberg_s(transform: Any) -> None:
    con = duckdb.connect()
    sql = _partition_value(transform, "c", t.DateType())
    assert sql is not None
    for day in (date(1969, 12, 31), date(1970, 1, 1), date(2026, 2, 28), date(1900, 3, 1)):
        row = con.execute(f"SELECT {sql} FROM (SELECT ?::DATE AS c)", [day]).fetchone()
        assert row is not None
        assert row[0] == transform.transform(t.DateType())((day - date(1970, 1, 1)).days)


def test_transforms_duckdb_cannot_compute_are_refused() -> None:
    assert _partition_value(tr.BucketTransform(8), "c", t.LongType()) is None
    assert _partition_value(tr.TruncateTransform(4), "c", t.StringType()) is None
    assert _partition_value(tr.HourTransform(), "c", t.DateType()) is None
    assert _partition_value(tr.IdentityTransform(), "c", t.StringType()) == 'hash("c")'


# -- types and field ids -----------------------------------------------------------------


def test_types_written_and_types_handed_back() -> None:
    assert duckdb_type(t.DecimalType(12, 3)) == "DECIMAL(12,3)"
    assert duckdb_type(t.TimestamptzType()) == "TIMESTAMPTZ"
    nested = t.StructType(
        t.NestedField(5, "a", t.ListType(6, t.LongType())),
        t.NestedField(7, "m", t.MapType(8, t.StringType(), 9, t.DoubleType())),
    )
    assert duckdb_type(nested) == 'STRUCT("a" BIGINT[], "m" MAP(VARCHAR, DOUBLE))'
    for unwritten in (t.TimeType(), t.UUIDType(), t.FixedType(4)):
        assert duckdb_type(unwritten) is None


def test_field_ids_come_from_the_schema_nested_ones_included() -> None:
    schema = Schema(
        t.NestedField(1, "id", t.LongType()),
        t.NestedField(2, "s", t.StructType(t.NestedField(5, "a", t.IntegerType()))),
        t.NestedField(3, "l", t.ListType(6, t.StringType())),
        t.NestedField(4, "m", t.MapType(7, t.StringType(), 8, t.LongType())),
    )
    assert field_ids_of(schema) == (
        '{"id": 1, "s": {__duckdb_field_id: 2, "a": 5}, "l": {__duckdb_field_id: 3, element: 6}, '
        '"m": {__duckdb_field_id: 4, key: 7, value: 8}}'
    )


def test_a_local_path_gets_the_table_s_scheme() -> None:
    assert iceberg_path("/w/t/data/x.parquet", "file:///w/t") == "file:///w/t/data/x.parquet"
    assert iceberg_path("s3://b/t/data/x.parquet", "s3://b/t") == "s3://b/t/data/x.parquet"


# -- the commit ----------------------------------------------------------------------------


class _Transaction:
    def __init__(self, log: list[str], fail: list[bool]) -> None:
        self.log, self.fail = log, fail

    def __enter__(self) -> _Transaction:
        return self

    def __exit__(self, *exc: object) -> None:
        if exc[0] is None and self.fail and self.fail.pop(0):
            raise CommitFailedException("a concurrent writer won")
        self.log.append("commit")

    def delete(self, delete_filter: Any) -> None:
        self.log.append(f"delete {type(delete_filter).__name__}")

    def add_files(self, file_paths: list[str]) -> None:
        self.log.append(f"add {len(file_paths)}")


class _Table:
    def __init__(self, log: list[str], fail: list[bool]) -> None:
        self.log, self.fail = log, fail

    def transaction(self) -> _Transaction:
        return _Transaction(self.log, self.fail)


def test_a_lost_race_reruns_the_whole_transaction() -> None:
    log: list[str] = []
    loads: list[int] = []
    fail = [True]

    def load() -> Any:
        loads.append(1)
        return _Table(log, fail)

    commit_files(load, ["a", "b"], mode="overwrite")
    assert len(loads) == 2  # reloaded for the retry
    assert log == ["delete AlwaysTrue", "add 2", "delete AlwaysTrue", "add 2", "commit"]


def test_nothing_to_append_is_no_commit() -> None:
    def load() -> Any:
        raise AssertionError("no commit expected")

    commit_files(load, [], mode="append")
    commit_files(load, [], mode="dynamic", partitions=[])


def test_the_partitions_a_dynamic_overwrite_replaces() -> None:
    expression = partition_filter([{"r": "east", "d": date(2026, 1, 2)}, {"r": None, "d": None}])
    text = repr(expression)
    assert "Or" in text and "IsNull" in text and "2026-01-02" in text


# -- the sink ------------------------------------------------------------------------------


def test_data_files_carry_field_ids_one_directory_per_partition(tmp_path: Path) -> None:
    con = duckdb.connect()
    sink = SinkSpec(
        kind="iceberg_files",
        location=(tmp_path / "t" / "task-0").as_posix(),
        field_ids='{"id": 1, "r": 2}',
        partition_columns=("__icefleet_p0",),
    )
    value = "['a', 'b'][range % 2 + 1]"
    sql = f"SELECT range::BIGINT AS id, {value} AS r, hash({value}) AS __icefleet_p0 FROM range(10)"
    files = write_iceberg_files(con.cursor(), sql, {}, sink)
    assert len(files) == 2 and sum(f.rows for f in files) == 10
    for output in files:
        schema = con.execute(
            f"SELECT name, field_id FROM parquet_schema('{output.path}')"
        ).fetchall()
        assert ("id", 1) in schema and ("r", 2) in schema
        assert not any(name == "__icefleet_p0" for name, _ in schema)  # the grouping is not written
        values = con.execute(f"SELECT DISTINCT r FROM '{output.path}'").fetchall()
        assert len(values) == 1  # one partition per file


def test_an_empty_task_reports_its_empty_file_for_the_coordinator_to_delete(
    tmp_path: Path,
) -> None:
    con = duckdb.connect()
    sink = SinkSpec(
        kind="iceberg_files", location=(tmp_path / "e").as_posix(), field_ids='{"id": 1}'
    )
    files = write_iceberg_files(con.cursor(), "SELECT 1::BIGINT AS id WHERE FALSE", {}, sink)
    assert [f.rows for f in files] == [0]
    exchange = open_exchange(tmp_path.as_posix())
    exchange.delete_files([f.path for f in files])
    assert list((tmp_path / "e").rglob("*.parquet")) == []
    exchange.delete_files([f.path for f in files])  # already gone: not an error


def _three_file_table(tmp_path: Path) -> Any:
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.types import LongType, NestedField

    catalog = SqlCatalog(
        "unit", uri=f"sqlite:///{tmp_path}/c.db", warehouse=f"file://{tmp_path}/wh"
    )
    catalog.create_namespace("u")
    table = catalog.create_table("u.t", Schema(NestedField(1, "id", LongType(), required=False)))
    for low in (0, 10, 20):
        table.append(pa.table({"id": pa.array(range(low, low + 10), pa.int64())}))
    return catalog, catalog.load_table("u.t")


def _data_file(con: Any, table: Any, name: str, sql: str) -> str:
    folder = next(iter(table.scan().plan_files())).file.file_path.removeprefix("file://")
    path = f"{folder.rsplit('/', 1)[0]}/{name}.parquet"
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet, FIELD_IDS {{id: 1}})")
    return f"file://{path}"


def test_replacing_files_is_one_overwrite_snapshot(tmp_path: Path) -> None:
    catalog, table = _three_file_table(tmp_path)
    victim = min((task.file for task in table.scan().plan_files()), key=lambda f: f.file_path)
    kept = victim.file_path
    new = _data_file(duckdb.connect(), table, "new", "SELECT 1000::BIGINT AS id")
    replace_files(table, [victim], [new])
    after = catalog.load_table("u.t")
    assert len(after.snapshots()) == 4
    assert after.current_snapshot().summary.operation.value == "overwrite"
    paths = {task.file.file_path for task in after.scan().plan_files()}
    assert new in paths and kept not in paths and len(paths) == 3


def test_replacing_files_refuses_a_table_that_moved(tmp_path: Path) -> None:
    import pyarrow as pa

    catalog, planned = _three_file_table(tmp_path)
    catalog.load_table("u.t").append(pa.table({"id": pa.array([99], pa.int64())}))
    victim = next(iter(planned.scan().plan_files())).file
    new = _data_file(duckdb.connect(), planned, "late", "SELECT 1::BIGINT AS id")
    with pytest.raises(CommitFailedException):
        replace_files(planned, [victim], [new])


def test_replacing_nothing_appends_or_commits_nothing(tmp_path: Path) -> None:
    catalog, table = _three_file_table(tmp_path)
    replace_files(table, [], [])
    assert len(catalog.load_table("u.t").snapshots()) == 3
    new = _data_file(duckdb.connect(), table, "added", "SELECT 7::BIGINT AS id")
    replace_files(table, [], [new])
    after = catalog.load_table("u.t")
    assert after.current_snapshot().summary.operation.value == "append"
    assert after.scan().to_arrow().num_rows == 31


def test_the_sink_round_trips_in_a_task_spec() -> None:
    sink = SinkSpec(
        kind="iceberg_files", location="s3://b/t", field_ids='{"a": 1}', partition_columns=("p",)
    )
    assert SinkSpec.from_json(sink.to_json()) == sink
