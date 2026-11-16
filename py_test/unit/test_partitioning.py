"""The hash partitioner (PLAN §4.1, §4.4): total, consistent, and blind to integer width."""

from __future__ import annotations

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlglot import exp

from icefleet.plan.aggregates import partition_expression

SIGNED = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT")
UNSIGNED = ("UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT")


def partition_of(
    con: duckdb.DuckDBPyConnection, keys: list[tuple[str, str]], partitions: int
) -> int:
    """The partition of one row whose keys are `(SQL literal, DuckDB type)` pairs."""
    expression = partition_expression(
        [exp.maybe_parse(f"CAST({value} AS {kind})", dialect="duckdb") for value, kind in keys],
        partitions,
        [kind for _, kind in keys],
    )
    row = con.execute(f"SELECT {expression.sql(dialect='duckdb')}").fetchone()
    assert row is not None
    return int(row[0])


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def test_duckdb_alone_hashes_a_negative_integer_by_width(con: duckdb.DuckDBPyConnection) -> None:
    """Why keys are cast before hashing. Measured on DuckDB 1.5.6; if a later DuckDB
    hashes by value, the cast is harmless and this test says so."""
    row = con.execute(
        "SELECT hash((-1)::INTEGER), hash((-1)::BIGINT), hash(1::INTEGER), hash(1::BIGINT)"
    ).fetchone()
    assert row is not None
    assert row[0] != row[1]
    assert row[2] == row[3]


@pytest.mark.parametrize("value", [0, 1, -1, 42, 127, -128])
@pytest.mark.parametrize("partitions", [2, 7, 64])
def test_an_integer_key_lands_in_one_partition_whatever_its_width(
    con: duckdb.DuckDBPyConnection, value: int, partitions: int
) -> None:
    """An INTEGER key and a BIGINT key co-partition (Phase 2 "done when"; Phase 3's joins
    rely on it)."""
    kinds = SIGNED + (UNSIGNED if value >= 0 else ())
    seen = {partition_of(con, [(f"({value})", kind)], partitions) for kind in kinds}
    assert len(seen) == 1, seen


def test_float_and_double_keys_co_partition(con: duckdb.DuckDBPyConnection) -> None:
    assert partition_of(con, [("1.5", "FLOAT")], 64) == partition_of(con, [("1.5", "DOUBLE")], 64)


def test_signed_zero_and_null_are_one_key_each(con: duckdb.DuckDBPyConnection) -> None:
    assert partition_of(con, [("-0.0", "DOUBLE")], 64) == partition_of(con, [("0.0", "DOUBLE")], 64)
    assert partition_of(con, [("NULL", "INTEGER")], 64) == partition_of(
        con, [("NULL", "BIGINT")], 64
    )


@settings(max_examples=40, deadline=None)
@given(
    rows=st.lists(
        st.tuples(st.one_of(st.none(), st.integers(-(2**40), 2**40)), st.text(max_size=4)),
        min_size=1,
        max_size=60,
    ),
    partitions=st.integers(1, 97),
)
def test_every_row_goes_to_exactly_one_partition_and_equal_keys_meet(
    rows: list[tuple[int | None, str]], partitions: int
) -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE t (a BIGINT, b VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?, ?)", rows)
    expression = partition_expression(
        [exp.column("a"), exp.column("b")], partitions, ["BIGINT", "VARCHAR"]
    )
    assigned = con.execute(
        f"SELECT a, b, {expression.sql(dialect='duckdb')} AS p FROM t"
    ).fetchall()
    assert len(assigned) == len(rows)  # total: no row lost or duplicated
    by_key: dict[tuple[int | None, str], set[int]] = {}
    for a, b, p in assigned:
        assert 0 <= p < partitions
        by_key.setdefault((a, b), set()).add(int(p))
    assert all(len(ps) == 1 for ps in by_key.values())  # consistent: equal keys, one partition
