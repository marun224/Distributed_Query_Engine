"""The hash partitioner (PLAN §4.1, §4.4): total, consistent, and blind to integer width
-- and, for a join, to the two sides' types."""

from __future__ import annotations

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlglot import exp

from icefleet.plan.aggregates import partition_expression
from icefleet.plan.partitioning import join_hash_type

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


# -- join keys of two types (Phase 3, kickoff answer 5) ------------------------------------


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        ("INTEGER", "INTEGER", "HUGEINT"),
        ("INTEGER", "BIGINT", "HUGEINT"),
        ("UTINYINT", "SMALLINT", "HUGEINT"),
        ("VARCHAR", "VARCHAR", "VARCHAR"),
        ("DECIMAL(10,2)", "DECIMAL(10,2)", "DECIMAL(10,2)"),
        ("FLOAT", "FLOAT", "DOUBLE"),
        ("INTEGER", "DOUBLE", "DOUBLE"),
        ("DECIMAL(10,2)", "FLOAT", "DOUBLE"),
        ("BIGINT", "DECIMAL(10,2)", "DECIMAL(38,2)"),
        ("DECIMAL(10,2)", "DECIMAL(18,4)", "DECIMAL(38,4)"),
        ("HUGEINT", "DECIMAL(10,2)", None),  # 39 digits and 2 decimals do not fit 38
        ("UHUGEINT", "BIGINT", None),
        ("VARCHAR", "INTEGER", None),
        ("DATE", "TIMESTAMP", None),
    ],
)
def test_the_type_two_join_keys_are_hashed_as(left: str, right: str, expected: str | None) -> None:
    assert join_hash_type(left, right) == expected
    assert join_hash_type(right, left) == expected


def join_partition(con: duckdb.DuckDBPyConnection, value: str, kind: str, hashed: str) -> int:
    expression = partition_expression(
        [exp.maybe_parse(f"CAST({value} AS {kind})", dialect="duckdb")], 64, hash_as=[hashed]
    )
    row = con.execute(f"SELECT {expression.sql(dialect='duckdb')}").fetchone()
    assert row is not None
    return int(row[0])


MIXED = [
    ("INTEGER", "BIGINT"),
    ("SMALLINT", "HUGEINT"),
    ("INTEGER", "DOUBLE"),
    ("BIGINT", "DECIMAL(12,2)"),
    ("DECIMAL(10,2)", "DECIMAL(18,4)"),
    ("DECIMAL(9,3)", "DOUBLE"),
]


@settings(max_examples=60, deadline=None)
@given(value=st.integers(-30000, 30000), pair=st.sampled_from(MIXED))
def test_join_keys_that_compare_equal_meet_in_one_partition(
    value: int, pair: tuple[str, str]
) -> None:
    """Whatever DuckDB's comparison casts to, two values it calls equal hash alike."""
    con = duckdb.connect()
    left, right = pair
    hashed = join_hash_type(left, right)
    assert hashed is not None
    literal = f"({value})" if "DECIMAL" not in left + right else f"({value} / 4)"
    equal = con.execute(f"SELECT CAST({literal} AS {left}) = CAST({literal} AS {right})").fetchone()
    assert equal is not None
    if equal[0]:
        assert join_partition(con, literal, left, hashed) == join_partition(
            con, literal, right, hashed
        )
