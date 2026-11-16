"""The shape recogniser, on hand-written trees shaped like icetl's optimized ones."""

from __future__ import annotations

import re
from typing import Any, cast

import pytest
import sqlglot
from sqlglot import exp

from icefleet.errors import Uncuttable
from icefleet.plan.rel import Aggregate, Project, Query, Scan, from_sqlglot, is_aggregate_call

SOURCES = cast(Any, {"fx.t": object(), "fx.u": object()})
DUCKDB_AGGREGATES = ("list", "sum", "count", "string_agg", "histogram")


def recognise(
    sql: str, udfs: tuple[str, ...] = (), aggregates: tuple[str, ...] = DUCKDB_AGGREGATES
) -> Query | Uncuttable:
    tree = cast(exp.Expression, sqlglot.parse_one(sql, dialect="duckdb"))
    return from_sqlglot(tree, SOURCES, udf_names=udfs, aggregate_names=aggregates)


def shape(query: Query) -> list[str]:
    return [type(node).__name__ for node in query.nodes()]


@pytest.mark.parametrize(
    ("sql", "limit"),
    [
        ('SELECT "t"."a" AS "a" FROM "fx"."t" AS "t"', None),
        (
            'SELECT UPPER("t"."b") AS "b", "t"."a" + 1 AS "c" '
            'FROM "fx"."t" AS "t" WHERE "t"."a" > 5',
            None,
        ),
        ('SELECT "t"."a" AS "a" FROM "fx"."t" AS "t" WHERE "t"."b" LIKE \'x%\' LIMIT 10', 10),
        ('SELECT CASE WHEN "t"."a" > 0 THEN 1 ELSE 0 END AS "s" FROM "fx"."t" AS "t"', None),
    ],
)
def test_project_over_scan(sql: str, limit: int | None) -> None:
    query = recognise(sql)
    assert isinstance(query, Query), query
    assert shape(query) == ["Project", "Scan"]
    assert query.scan == Scan("fx.t")
    assert query.limit == limit
    assert query.aggregate is None
    assert query.tree.args.get("limit") is None  # lifted off the tree


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ('SELECT "t"."b", COUNT(*) FROM "fx"."t" AS "t" GROUP BY "t"."b"', ["Aggregate", "Scan"]),
        ('SELECT SUM("t"."a") FROM "fx"."t" AS "t"', ["Aggregate", "Scan"]),
        ('SELECT DISTINCT "t"."a" FROM "fx"."t" AS "t"', ["Aggregate", "Scan"]),
        (
            'SELECT "t"."b" FROM "fx"."t" AS "t" GROUP BY "t"."b" HAVING COUNT(*) > 1',
            ["Aggregate", "Scan"],
        ),
        # A filter over an aggregate subquery: how a DataFrame .filter() after .agg() looks.
        (
            'SELECT "q"."b" AS "b", "q"."s" AS "s" FROM (SELECT "t"."b" AS "b", '
            'SUM("t"."a") AS "s" FROM "fx"."t" AS "t" GROUP BY "t"."b") AS "q" WHERE "q"."s" > 1',
            ["Project", "Aggregate", "Scan"],
        ),
        # An aggregate over a projection subquery: .withColumn() then .groupBy().
        (
            'SELECT "q"."y" AS "y", MAX("q"."a") AS "m" FROM (SELECT "t"."a" AS "a", '
            'YEAR("t"."d") AS "y" FROM "fx"."t" AS "t") AS "q" GROUP BY "q"."y"',
            ["Aggregate", "Project", "Scan"],
        ),
    ],
)
def test_aggregates_and_chains(sql: str, expected: list[str]) -> None:
    query = recognise(sql)
    assert isinstance(query, Query), query
    assert shape(query) == expected
    assert isinstance(query.aggregate, Aggregate)


def test_an_aggregate_sqlglot_does_not_know_is_still_an_aggregate() -> None:
    """`list(x)` parses as a scalar `exp.List`. Taken for row-wise work, it would return
    one list per split -- so DuckDB's catalog of aggregate names is consulted too."""
    # icetl's trees carry `exp.List` for list(x); DuckDB's own parser would say ArrayAgg.
    call = exp.List(expressions=[exp.column("a", table="t", quoted=True)])
    assert not isinstance(call, exp.AggFunc)
    assert call.sql(dialect="duckdb") == 'LIST("t"."a")'
    assert is_aggregate_call(call, {"list"})
    assert not is_aggregate_call(call, ())
    tree = exp.select(exp.alias_(call, "l", quoted=True)).from_(
        exp.Table(
            this=exp.to_identifier("t", quoted=True),
            db=exp.to_identifier("fx", quoted=True),
            alias=exp.TableAlias(this=exp.to_identifier("t", quoted=True)),
        )
    )
    query = from_sqlglot(tree, SOURCES, aggregate_names={"list"})
    assert isinstance(query, Query) and isinstance(query.aggregate, Aggregate)
    # Without DuckDB's catalog it would have been taken for row-at-a-time work.
    unaware = from_sqlglot(tree, SOURCES)
    assert isinstance(unaware, Query) and unaware.aggregate is None


def test_an_operator_starting_with_an_aggregate_is_not_a_call() -> None:
    having = cast(
        exp.Expression, sqlglot.parse_one("COUNT(*) > 50 AND MAX(d) > 0", dialect="duckdb")
    )
    assert not is_aggregate_call(having, {"count", "max"})


def test_order_by_a_result_column_is_lifted_to_the_coordinator() -> None:
    query = recognise(
        'SELECT "t"."b" AS "b", SUM("t"."a") AS "s" FROM "fx"."t" AS "t" GROUP BY "t"."b" '
        'ORDER BY "s" DESC NULLS FIRST, "t"."b" LIMIT 3'
    )
    assert isinstance(query, Query)
    assert [item.sql(dialect="duckdb") for item in query.order] == ['"s" DESC NULLS FIRST', '"b"']
    assert query.limit == 3 and query.hidden == ()
    assert query.tree.args.get("order") is None


def test_order_by_something_else_becomes_a_hidden_result_column() -> None:
    query = recognise('SELECT "t"."b" AS "b" FROM "fx"."t" AS "t" ORDER BY "t"."a" DESC LIMIT 2')
    assert isinstance(query, Query)
    assert query.hidden == ("__icefleet_order_0",)
    assert [p.alias_or_name for p in query.tree.expressions] == ["b", "__icefleet_order_0"]
    assert query.order[0].sql(dialect="duckdb") == '"__icefleet_order_0" DESC'


def test_positional_order_by() -> None:
    query = recognise('SELECT "t"."a" AS "a", "t"."b" AS "b" FROM "fx"."t" AS "t" ORDER BY 2')
    assert isinstance(query, Query) and query.order[0].this.name == "b"


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ('SELECT * FROM "fx"."t" AS "t" JOIN "fx"."u" AS "u" ON "t"."a" = "u"."a"', "join"),
        ('SELECT ROW_NUMBER() OVER (ORDER BY "t"."a") FROM "fx"."t" AS "t"', "window"),
        ('SELECT "t"."a" FROM "fx"."t" AS "t" WHERE "t"."a" IN (SELECT 1)', "subquery"),
        ('SELECT "a" FROM "fx"."t" UNION ALL SELECT "a" FROM "fx"."u"', "set operation"),
        ('SELECT "t"."a" FROM "fx"."t" AS "t" LIMIT 5 OFFSET 2', "OFFSET"),
        ('SELECT "c"."x" FROM "icetl_cache_3" AS "c"', "locally"),
        ("SELECT \"a\" FROM read_parquet('x.parquet')", "single Iceberg table"),
        ("INSERT INTO t VALUES (1)", "not a SELECT"),
        (
            'SELECT SUM("q"."s") FROM (SELECT "t"."b" AS "b", SUM("t"."a") AS "s" '
            'FROM "fx"."t" AS "t" GROUP BY "t"."b") AS "q"',
            "aggregate over an aggregate",
        ),
        (
            'SELECT "q"."a" FROM (SELECT "t"."a" AS "a" FROM "fx"."t" AS "t" LIMIT 3) AS "q"',
            "inside a subquery",
        ),
        ('SELECT DISTINCT "t"."b" AS "b" FROM "fx"."t" AS "t" ORDER BY "t"."a"', "DISTINCT"),
        ('SELECT DISTINCT SUM("t"."a") FROM "fx"."t" AS "t" GROUP BY "t"."b"', "DISTINCT over"),
        ('SELECT DISTINCT ON ("t"."a") "t"."b" FROM "fx"."t" AS "t"', "DISTINCT ON"),
    ],
)
def test_everything_else_is_uncuttable_with_a_reason(sql: str, reason: str) -> None:
    query = recognise(sql)
    assert isinstance(query, Uncuttable), query
    assert re.search(reason, query.reason), query.reason


def test_a_udf_call_is_uncuttable() -> None:
    sql = 'SELECT slen("t"."b") AS "n" FROM "fx"."t" AS "t"'
    assert isinstance(recognise(sql), Query)
    query = recognise(sql, udfs=("SLEN",))
    assert isinstance(query, Uncuttable) and "Phase 8" in query.reason


def test_the_original_tree_is_not_touched() -> None:
    sql = 'SELECT "t"."b" AS "b" FROM "fx"."t" AS "t" ORDER BY "t"."a" LIMIT 2'
    tree = cast(exp.Expression, sqlglot.parse_one(sql, dialect="duckdb"))
    before = tree.sql(dialect="duckdb")
    query = from_sqlglot(tree, SOURCES)
    assert isinstance(query, Query) and isinstance(query.root, Project)
    assert tree.sql(dialect="duckdb") == before
