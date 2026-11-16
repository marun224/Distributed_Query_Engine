"""The partial/final rewrite (PLAN §4.2), checked in plain DuckDB without a cluster.

Each case runs the original aggregate over a table, then the cut: the partial SQL over
three "splits" of that table through the worker's real shuffle sink (parquet, hash
partitioned), then the final SQL once per partition over exactly that partition's files.
The union of the finals must equal the original -- in rows and in Arrow types.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa
import pytest
import sqlglot
from sqlglot import exp

from icefleet.errors import Uncuttable
from icefleet.plan.aggregates import INPUT_TABLE, AggregateCut, cut_aggregate
from icefleet.plan.rel import Query, from_sqlglot
from icefleet.plan.tasks import SinkSpec
from icefleet.worker.sinks import write_shuffle
from py_test.oracle import assert_same_rows

SOURCES = cast(Any, {"fx.t": object()})
SPLITS = 3
#: DuckDB's aggregate function names, filled by the `con` fixture.
AGGREGATES: set[str] = set()

TABLE = """
CREATE SCHEMA fx;
CREATE TABLE fx.t AS
SELECT
    i::BIGINT AS id,
    CASE WHEN i % 11 = 0 THEN NULL ELSE (i * 7) % 23 - 11 END::INTEGER AS a,
    CASE WHEN i % 13 = 0 THEN NULL ELSE 'k' || (i % 5) END AS b,
    CASE WHEN i % 17 = 0 THEN NULL ELSE i * 1.37 - 40 END::DOUBLE AS d,
    (i % 97 * 3.25)::DECIMAL(10, 2) AS p,
    CASE WHEN i % 19 = 0 THEN NULL ELSE i % 4 = 0 END AS f,
    (i % 6)::HUGEINT AS h,
    (TIMESTAMP '2026-01-01' + to_minutes(i * 37))::TIMESTAMP_S AS ts
FROM range(1, 301) r(i);
"""


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute(TABLE)
    for k in range(SPLITS):
        connection.execute(
            f"CREATE TABLE split_{k} AS SELECT * FROM fx.t WHERE id % {SPLITS} = {k}"
        )
    names = connection.execute(
        "SELECT DISTINCT lower(function_name) FROM duckdb_functions() "
        "WHERE function_type = 'aggregate'"
    ).fetchall()
    AGGREGATES.update(str(row[0]) for row in names)
    return connection


def describe(con: duckdb.DuckDBPyConnection, tree: exp.Expression) -> list[tuple[str, str]]:
    rows = con.execute(f"DESCRIBE SELECT * FROM ({tree.sql(dialect='duckdb')}) AS x").fetchall()
    return [(str(r[0]), str(r[1])) for r in rows]


def cut_of(
    con: duckdb.DuckDBPyConnection, sql: str, partitions: int = 4
) -> tuple[Query, AggregateCut]:
    names = AGGREGATES
    tree = cast(exp.Expression, sqlglot.parse_one(sql, dialect="duckdb"))
    query = from_sqlglot(tree, SOURCES, aggregate_names=names)
    assert isinstance(query, Query), query
    assert query.aggregate is not None
    cut = cut_aggregate(
        query.aggregate,
        partitions=partitions,
        describe=lambda t: describe(con, t),
        aggregate_names=names,
    )
    assert isinstance(cut, AggregateCut), cut
    return query, cut


def run_cut(
    con: duckdb.DuckDBPyConnection, sql: str, tmp_path: Path, partitions: int = 4
) -> tuple[pa.Table, AggregateCut]:
    query, cut = cut_of(con, sql, partitions)
    partial = cut.partial.sql(dialect="duckdb")
    files: dict[int, list[str]] = {}
    for k in range(SPLITS):
        sink = SinkSpec(
            kind="shuffle", location=str(tmp_path / f"task-{k}"), partitions=cut.partitions
        )
        for output in write_shuffle(
            con.cursor(), partial.replace('"fx"."t"', f'"split_{k}"'), {}, sink
        ):
            files.setdefault(output.partition or 0, []).append(output.path)

    aggregate = query.aggregate
    assert aggregate is not None
    if aggregate.select is query.tree:
        final_tree: exp.Expression = cut.final
    else:
        aggregate.select.replace(cut.final)
        final_tree = query.tree
    final = final_tree.sql(dialect="duckdb")
    placeholder = exp.to_identifier(INPUT_TABLE, quoted=True).sql(dialect="duckdb")
    assert final.count(placeholder) == 1

    tables = []
    for partition in sorted(files) or [0]:
        paths = files.get(partition, [])
        if paths:
            listing = ", ".join(f"'{p}'" for p in paths)
            source = f"read_parquet([{listing}])"
        else:
            source = "(" + _empty(cut) + ")"
        tables.append(con.execute(final.replace(placeholder, source)).to_arrow_table())
    return pa.concat_tables(tables), cut


def _empty(cut: AggregateCut) -> str:
    columns = ", ".join(f'CAST(NULL AS {kind}) AS "{name}"' for name, kind in cut.shuffle_columns)
    return f"SELECT {columns} WHERE FALSE"


def check(con: duckdb.DuckDBPyConnection, sql: str, tmp_path: Path, strategy: str) -> AggregateCut:
    actual, cut = run_cut(con, sql, tmp_path)
    assert cut.strategy == strategy, cut.why_raw
    expected = con.execute(sql).to_arrow_table()
    assert_same_rows(actual, expected)
    return cut


DECOMPOSABLE = {
    "count": (
        'SELECT "t"."b" AS "b", COUNT(*) AS "n", COUNT("t"."a") AS "na" '
        'FROM "fx"."t" AS "t" GROUP BY "t"."b"'
    ),
    "sum of every width": (
        'SELECT "t"."b" AS "b", SUM("t"."a") AS "sa", SUM("t"."id") AS "si", '
        'SUM("t"."d") AS "sd", SUM("t"."p") AS "sp", SUM("t"."h") AS "sh" '
        'FROM "fx"."t" AS "t" GROUP BY "t"."b"'
    ),
    "min and max": (
        'SELECT "t"."f" AS "f", MIN("t"."a") AS "mn", MAX("t"."b") AS "mx", '
        'MIN("t"."ts") AS "mts", MAX("t"."p") AS "mp", MAX("t"."h") AS "mh" '
        'FROM "fx"."t" AS "t" GROUP BY "t"."f"'
    ),
    "avg": (
        'SELECT "t"."b" AS "b", AVG("t"."a") AS "aa", AVG("t"."d") AS "ad", AVG("t"."p") AS "ap", '
        'AVG("t"."h") AS "ah" FROM "fx"."t" AS "t" GROUP BY "t"."b"'
    ),
    "bool and bit": (
        'SELECT "t"."b" AS "b", BOOL_AND("t"."f") AS "ba", BOOL_OR("t"."f") AS "bo", '
        'BIT_AND("t"."a") AS "x1", BIT_OR("t"."a") AS "x2", BIT_XOR("t"."a") AS "x3" '
        'FROM "fx"."t" AS "t" GROUP BY "t"."b"'
    ),
    "global": (
        'SELECT COUNT(*) AS "n", SUM("t"."a") AS "s", AVG("t"."d") AS "a", MIN("t"."ts") AS "m" '
        'FROM "fx"."t" AS "t"'
    ),
    "global over nothing": (
        'SELECT COUNT(*) AS "n", SUM("t"."a") AS "s", AVG("t"."d") AS "a", MAX("t"."b") AS "m" '
        'FROM "fx"."t" AS "t" WHERE "t"."id" < 0'
    ),
    "grouped over nothing": (
        'SELECT "t"."b" AS "b", COUNT(*) AS "n" FROM "fx"."t" AS "t" '
        'WHERE "t"."id" < 0 GROUP BY "t"."b"'
    ),
    "where and having": (
        'SELECT "t"."b" AS "b", SUM("t"."a") AS "s" FROM "fx"."t" AS "t" WHERE "t"."id" > 40 '
        'GROUP BY "t"."b" HAVING COUNT(*) > 50 AND MAX("t"."d") > 0'
    ),
    "expressions over aggregates and keys": (
        'SELECT UPPER("t"."b") AS "u", SUM("t"."d") / COUNT(*) AS "r", COUNT(*) + 1 AS "n1" '
        'FROM "fx"."t" AS "t" GROUP BY "t"."b"'
    ),
    "expression keys": (
        'SELECT "t"."a" % 3 AS "m", YEAR("t"."ts") AS "y", COUNT(*) AS "n" FROM "fx"."t" AS "t" '
        'GROUP BY "t"."a" % 3, YEAR("t"."ts")'
    ),
    "several keys with NULLs": (
        'SELECT "t"."b" AS "b", "t"."f" AS "f", SUM("t"."p") AS "s" FROM "fx"."t" AS "t" '
        'GROUP BY "t"."b", "t"."f"'
    ),
    "a HUGEINT key and a TIMESTAMP_S key": (
        'SELECT "t"."h" AS "h", "t"."ts" AS "ts", COUNT(*) AS "n" FROM "fx"."t" AS "t" '
        'GROUP BY "t"."h", "t"."ts"'
    ),
    "distinct": 'SELECT DISTINCT "t"."b" AS "b", "t"."f" AS "f" FROM "fx"."t" AS "t"',
    "filter over an aggregate subquery": (
        'SELECT "q"."b" AS "b", "q"."s" * 2 AS "s2" FROM (SELECT "t"."b" AS "b", '
        'SUM("t"."a") AS "s" FROM "fx"."t" AS "t" GROUP BY "t"."b") AS "q" WHERE "q"."s" > 0'
    ),
    "aggregate over a projection subquery": (
        'SELECT "q"."y" AS "y", MAX("q"."d") AS "m" FROM (SELECT "t"."d" AS "d", '
        '"t"."id" % 7 AS "y" FROM "fx"."t" AS "t") AS "q" GROUP BY "q"."y"'
    ),
}

RAW = {
    "count distinct": (
        'SELECT "t"."f" AS "f", COUNT(DISTINCT "t"."b") AS "nb", SUM("t"."a") AS "s" '
        'FROM "fx"."t" AS "t" GROUP BY "t"."f"'
    ),
    "string_agg, median, list": (
        'SELECT "t"."b" AS "b", '
        'STRING_AGG(CAST("t"."id" AS TEXT), \',\' ORDER BY "t"."id") AS "ids", '
        'MEDIAN("t"."d") AS "md", LIST("t"."id" ORDER BY "t"."id") AS "l" '
        'FROM "fx"."t" AS "t" GROUP BY "t"."b"'
    ),
    "filter clause": (
        'SELECT "t"."b" AS "b", COUNT(*) FILTER(WHERE "t"."a" > 0) AS "p" FROM "fx"."t" AS "t" '
        'GROUP BY "t"."b"'
    ),
    "global raw": (
        'SELECT COUNT(DISTINCT "t"."b") AS "n", LIST("t"."h" ORDER BY "t"."id") AS "l" '
        'FROM "fx"."t" AS "t"'
    ),
    "global raw over nothing": (
        'SELECT COUNT(DISTINCT "t"."b") AS "n", SUM("t"."a") AS "s" FROM "fx"."t" AS "t" '
        'WHERE "t"."id" < 0'
    ),
    "raw with a HUGEINT column": (
        'SELECT "t"."b" AS "b", COUNT(DISTINCT "t"."h") AS "n", MAX("t"."ts") AS "m" '
        'FROM "fx"."t" AS "t" GROUP BY "t"."b"'
    ),
}


@pytest.mark.parametrize("name", list(DECOMPOSABLE))
def test_partial_path_equals_the_original(
    name: str, con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    check(con, DECOMPOSABLE[name], tmp_path, "partial")


@pytest.mark.parametrize("name", list(RAW))
def test_raw_path_equals_the_original(
    name: str, con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    check(con, RAW[name], tmp_path, "raw")


def test_a_global_aggregate_has_one_partition(con: duckdb.DuckDBPyConnection) -> None:
    _, cut = cut_of(con, DECOMPOSABLE["global"], partitions=64)
    assert cut.partitions == 1
    assert "__part" not in cut.partial.sql(dialect="duckdb")


def test_the_partial_stage_reads_only_what_it_aggregates(con: duckdb.DuckDBPyConnection) -> None:
    _, cut = cut_of(con, DECOMPOSABLE["avg"])
    assert [name for name, _ in cut.shuffle_columns] == [
        "__k0",
        "__p0",
        "__p1",
        "__p2",
        "__p3",
        "__p4",
        "__p5",
        "__p6",
        "__p7",
    ]
    assert "HASH(" in cut.partial.sql(dialect="duckdb").upper()
    # avg travels as sum + count; a HUGEINT sum travels as DECIMAL(38,0).
    assert dict(cut.shuffle_columns)["__p6"] == "DECIMAL(38,0)"


def test_stand_ins_are_cast_back_in_the_final(con: duckdb.DuckDBPyConnection) -> None:
    _, cut = cut_of(con, DECOMPOSABLE["a HUGEINT key and a TIMESTAMP_S key"])
    written = dict(cut.shuffle_columns)
    assert written["__k0"] == "DECIMAL(38,0)" and written["__k1"] == "TIMESTAMP"
    final = cut.final.sql(dialect="duckdb")
    # sqlglot spells HUGEINT as DuckDB's alias INT128.
    assert 'CAST("__k0" AS INT128)' in final and 'CAST("__k1" AS TIMESTAMP_S)' in final


def test_a_partial_path_explains_itself(con: duckdb.DuckDBPyConnection) -> None:
    _, cut = cut_of(con, DECOMPOSABLE["count"])
    assert cut.keys == ('"b"',)
    assert cut.partials == ("COUNT(*)", 'COUNT("a")')
    assert cut.finals == ('COALESCE(SUM("__p0"), 0)', 'COALESCE(SUM("__p1"), 0)')


def test_the_raw_path_says_why(con: duckdb.DuckDBPyConnection) -> None:
    _, cut = cut_of(con, RAW["count distinct"])
    assert cut.why_raw is not None and "DISTINCT" in cut.why_raw


def test_grouping_sets_wait_for_phase_4(con: duckdb.DuckDBPyConnection) -> None:
    names = AGGREGATES
    sql = 'SELECT "t"."b", COUNT(*) FROM "fx"."t" AS "t" GROUP BY ROLLUP ("t"."b")'
    query = from_sqlglot(
        cast(exp.Expression, sqlglot.parse_one(sql, dialect="duckdb")),
        SOURCES,
        aggregate_names=names,
    )
    assert isinstance(query, Query) and query.aggregate is not None
    cut = cut_aggregate(query.aggregate, partitions=4, describe=lambda t: describe(con, t))
    assert isinstance(cut, Uncuttable) and "Phase 4" in cut.reason
