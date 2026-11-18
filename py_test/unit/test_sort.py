"""The range partitioner of a distributed ORDER BY (`plan/sort.py`), checked in DuckDB.

The one property the sort's correctness rests on: walking the rows in the query's own
order, the partition never goes down -- so concatenating the partitions, each sorted,
is the sorted result -- and equal keys share a partition. It must hold for any
boundaries at all (a sample only balances the partitions), for every direction and
NULL placement, over several key types.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from icefleet.plan.sort import (
    RangeSpec,
    SortKey,
    boundaries,
    range_expression,
    sample_query,
    sortable_by_range,
)
from icefleet.plan.tasks import OutputFile, SinkSpec
from icefleet.worker.sinks import write_result


def partitions_in_order(
    con: duckdb.DuckDBPyConnection, spec: RangeSpec, bounds: list[tuple[str | None, ...]]
) -> list[tuple[int, tuple[object, ...]]]:
    expression = range_expression(spec, bounds)
    # Keys come back as text: equal keys are equal text, and no Python time zone is needed.
    columns = ", ".join(f'CAST("{k.column}" AS VARCHAR)' for k in spec.keys)
    rows = con.execute(
        f"SELECT {expression} AS p, {columns} FROM t ORDER BY {spec.order_sql()}"
    ).fetchall()
    return [(int(row[0]), tuple(row[1:])) for row in rows]


def check_monotone(con: duckdb.DuckDBPyConnection, spec: RangeSpec, sample_size: int) -> None:
    sample_sql = (
        "SELECT "
        + ", ".join(f'CAST("{k.column}" AS VARCHAR)' for k in spec.keys)
        + f" FROM (SELECT * FROM t USING SAMPLE {sample_size} ROWS) ORDER BY {spec.order_sql()}"
    )
    rows = con.execute(sample_sql).fetchall()
    bounds = boundaries(rows, spec.partitions)
    assigned = partitions_in_order(con, spec, bounds)
    seen: dict[tuple[object, ...], int] = {}
    previous = 0
    for partition, key in assigned:
        assert 0 <= partition <= len(bounds)
        assert partition >= previous, (partition, previous, key, bounds)
        previous = partition
        assert seen.setdefault(key, partition) == partition  # equal keys, one partition


KEY = st.one_of(st.none(), st.integers(-50, 50))


@settings(max_examples=40, deadline=None)
@given(
    rows=st.lists(
        st.tuples(KEY, st.one_of(st.none(), st.text("abc", max_size=3))), min_size=1, max_size=80
    ),
    desc=st.tuples(st.booleans(), st.booleans()),
    nulls_first=st.tuples(st.booleans(), st.booleans()),
    partitions=st.integers(1, 9),
    sample_size=st.integers(1, 30),
)
def test_the_partition_never_goes_down_in_sort_order(
    rows: list[tuple[int | None, str | None]],
    desc: tuple[bool, bool],
    nulls_first: tuple[bool, bool],
    partitions: int,
    sample_size: int,
) -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE t (a INTEGER, b VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?, ?)", rows)
    spec = RangeSpec(
        keys=(
            SortKey("a", "INTEGER", desc[0], nulls_first[0]),
            SortKey("b", "VARCHAR", desc[1], nulls_first[1]),
        ),
        partitions=partitions,
        sample_stages=(0,),
    )
    check_monotone(con, spec, sample_size)


@pytest.mark.parametrize(
    ("kind", "values"),
    [
        ("DOUBLE", "(1.5), (-0.0), (0.0), ('nan'::DOUBLE), (NULL), ('inf'::DOUBLE), (1e-300)"),
        ("DECIMAL(10,2)", "(1.25), (-3.50), (NULL), (99999999.99), (0.01)"),
        ("TIMESTAMP", "('2026-01-01 08:30:00.123456'), ('1970-01-01'), (NULL), ('2026-01-01')"),
        (
            "TIMESTAMP WITH TIME ZONE",
            "('2026-01-01 08:30:00+02'), ('2026-01-01 06:30:00+00'), (NULL)",
        ),
        ("DATE", "('2026-02-28'), ('1999-12-31'), (NULL)"),
        ("VARCHAR", "('it''s'), (''), ('Z'), ('a'), (NULL), ('ü')"),
        ("BLOB", "('\\x00\\xFF'::BLOB), (''::BLOB), (NULL), ('abc'::BLOB)"),
        ("BOOLEAN", "(true), (false), (NULL)"),
    ],
)
@pytest.mark.parametrize("desc", [False, True])
@pytest.mark.parametrize("nulls_first", [False, True])
def test_boundaries_round_trip_through_their_text(
    kind: str, values: str, desc: bool, nulls_first: bool
) -> None:
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t AS SELECT CAST(v AS {kind}) AS k FROM (VALUES {values}) x(v)")
    con.execute("INSERT INTO t SELECT * FROM t")  # every key twice: ties across partitions
    spec = RangeSpec(
        keys=(SortKey("k", kind, desc, nulls_first),), partitions=4, sample_stages=(0,)
    )
    check_monotone(con, spec, 100)


def test_no_sample_is_one_partition() -> None:
    spec = RangeSpec(keys=(SortKey("k", "INTEGER", False, False),), partitions=8, sample_stages=())
    assert boundaries([], 8) == []
    assert range_expression(spec, []) == "0"


def test_repeated_boundaries_collapse() -> None:
    rows: list[tuple[str | None, ...]] = [("1",) for _ in range(10)]
    rows.append(("2",))
    assert boundaries(rows, 4) == [("1",)]


def test_nested_keys_keep_the_coordinator_sort() -> None:
    assert sortable_by_range([SortKey("k", "INTEGER", False, False)])
    assert not sortable_by_range([SortKey("k", "INTEGER[]", False, False)])
    assert not sortable_by_range([SortKey("k", "STRUCT(a INTEGER)", False, False)])


def test_a_result_sink_writes_a_sample_beside_the_rows(tmp_path: Path) -> None:
    con = duckdb.connect()
    sink = SinkSpec(
        kind="result",
        location=(tmp_path / "task-0").as_posix(),
        sample_columns=("k",),
        sample_rows=10,
    )
    written = write_result(con.cursor(), "SELECT range AS k, 'x' AS v FROM range(500)", {}, sink)
    data = [f for f in written if not f.sample]
    (sample,) = [f for f in written if f.sample]
    assert sum(f.rows for f in data) == 500 and sample.rows == 10
    assert con.execute(f"SELECT count(*) FROM '{sample.path}'").fetchone() == (10,)
    sql, parameters = sample_query(
        RangeSpec(keys=(SortKey("k", "BIGINT", False, False),), partitions=2, sample_stages=(0,)),
        [sample.path],
    )
    assert len(con.execute(sql, parameters).fetchall()) == 10


def test_a_sample_file_round_trips_in_a_manifest() -> None:
    item = OutputFile(path="s3://b/q/stage-0/task-0/sample.parquet", bytes=10, rows=3, sample=True)
    assert OutputFile.from_json(item.to_json()) == item
    assert "sample" not in OutputFile(path="p", bytes=1, rows=1).to_json()
