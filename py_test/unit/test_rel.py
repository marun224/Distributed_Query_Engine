"""The shape recogniser, on hand-written trees shaped like icetl's optimized ones."""

from __future__ import annotations

import re
from typing import Any, cast

import pytest
import sqlglot
from sqlglot import exp

from icefleet.errors import Uncuttable
from icefleet.plan.rel import (
    JOIN_ALIAS,
    Aggregate,
    Join,
    Project,
    Query,
    Scan,
    from_of,
    from_sqlglot,
    is_aggregate_call,
    join_kind,
)

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
        (
            'SELECT * FROM "fx"."t" AS "t" JOIN "fx"."u" AS "u" ON "t"."a" = "u"."a"',
            r"\* over a join",
        ),
        (
            'SELECT "t"."a" FROM "fx"."t" AS "t" ASOF JOIN "fx"."u" AS "u" ON "t"."a" >= "u"."a"',
            "ASOF join",
        ),
        (
            'SELECT "t"."a" FROM "fx"."t" AS "t" WHERE "t"."a" NOT IN (SELECT "u"."a" AS "a" '
            'FROM "fx"."u" AS "u")',
            "subquery",
        ),
        (
            'SELECT "t"."a" FROM "fx"."t" AS "t" WHERE EXISTS (SELECT 1 FROM "fx"."u" AS "u" '
            'WHERE "u"."a" = "t"."a")',
            "subquery",
        ),
        (
            'SELECT "t"."a" FROM "fx"."t" AS "t" WHERE "t"."a" IN (SELECT "u"."a" AS "a" '
            'FROM "fx"."u" AS "u" WHERE "u"."b" = "t"."b")',
            "subquery",
        ),
        (
            'SELECT "t"."a" FROM "fx"."t" AS "t" WHERE "t"."a" > 1 OR "t"."a" IN '
            '(SELECT "u"."a" AS "a" FROM "fx"."u" AS "u")',
            "subquery",
        ),
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


# -- joins (Phase 3) ---------------------------------------------------------------------

JOIN_ON = 'FROM "fx"."t" AS "t" JOIN "fx"."u" AS "u" ON "t"."a" = "u"."a"'


def test_a_join_is_a_join_over_its_relations() -> None:
    query = recognise(f'SELECT "t"."a" AS "a", "u"."b" AS "b" {JOIN_ON} WHERE "t"."b" > 1')
    assert isinstance(query, Query), query
    assert shape(query) == ["Join", "Scan", "Scan"]
    assert [s.key for s in query.scans] == ["fx.t", "fx.u"]


def test_a_self_join_has_a_scan_per_reference() -> None:
    query = recognise(
        'SELECT "a"."a" AS "x" FROM "fx"."t" AS "a" JOIN "fx"."t" AS "b" ON "a"."a" = "b"."a" + 1'
    )
    assert isinstance(query, Query), query
    scans = query.scans
    assert [s.key for s in scans] == ["fx.t", "fx.t"]
    assert [s.table.alias_or_name for s in scans if s.table is not None] == ["a", "b"]


def test_an_aggregating_join_is_split_into_an_aggregate_over_the_join() -> None:
    query = recognise(
        f'SELECT "u"."b" AS "b", SUM("t"."a") AS "s", COUNT(*) AS "n" {JOIN_ON} GROUP BY "u"."b"'
    )
    assert isinstance(query, Query), query
    assert shape(query) == ["Aggregate", "Join", "Scan", "Scan"]
    aggregate = query.root
    assert isinstance(aggregate, Aggregate)
    relation = from_of(aggregate.select)
    assert relation is not None and relation.alias_or_name == JOIN_ALIAS
    assert aggregate.select.sql(dialect="duckdb").startswith(
        f'SELECT "{JOIN_ALIAS}"."b" AS "b", SUM("{JOIN_ALIAS}"."a") AS "s"'
    )
    join = aggregate.child
    assert isinstance(join, Join)
    assert [p.alias_or_name for p in join.select.expressions] == ["b", "a"]


def test_columns_of_the_same_name_are_told_apart_when_split() -> None:
    query = recognise(f'SELECT "t"."a" AS "x", MAX("u"."a") AS "m" {JOIN_ON} GROUP BY "t"."a"')
    assert isinstance(query, Query)
    join = query.aggregate.child if query.aggregate else None
    assert isinstance(join, Join)
    assert [p.alias_or_name for p in join.select.expressions] == ["t_a", "u_a"]


def test_in_a_subquery_becomes_a_semi_join() -> None:
    query = recognise(
        'SELECT "t"."a" AS "a" FROM "fx"."t" AS "t" WHERE "t"."b" > 1 AND "t"."a" IN '
        '(SELECT "u"."a" AS "a" FROM "fx"."u" AS "u")'
    )
    assert isinstance(query, Query), query
    assert shape(query) == ["Join", "Scan", "Project", "Scan"]
    sql = query.tree.sql(dialect="duckdb")
    assert 'SEMI JOIN (SELECT "u"."a" AS "a" FROM "fx"."u" AS "u") AS "__icefleet_in0"' in sql
    assert 'ON "t"."a" = "__icefleet_in0"."a"' in sql
    assert 'WHERE "t"."b" > 1' in sql and " IN " not in sql


def test_the_semi_join_returns_what_in_returns_nulls_included() -> None:
    """In a WHERE, NULL and false both drop a row: `x IN (...)` and the semi join agree
    when x is NULL, when the subquery yields NULL, and when nothing matches."""
    import duckdb

    con = duckdb.connect()
    con.execute("CREATE SCHEMA fx")
    con.execute(
        "CREATE TABLE fx.t AS SELECT * FROM (VALUES (1, 5), (2, 5), (NULL, 5), (4, 0)) v(a, b)"
    )
    con.execute("CREATE TABLE fx.u AS SELECT * FROM (VALUES (1), (1), (NULL), (4)) v(a)")
    sql = (
        'SELECT "t"."a" AS "a" FROM "fx"."t" AS "t" WHERE "t"."b" > 1 AND "t"."a" IN '
        '(SELECT "u"."a" AS "a" FROM "fx"."u" AS "u")'
    )
    query = recognise(sql)
    assert isinstance(query, Query)
    original = sorted(con.execute(sql).fetchall(), key=str)
    rewritten = sorted(con.execute(query.tree.sql(dialect="duckdb")).fetchall(), key=str)
    assert rewritten == original == [(1,)]


@pytest.mark.parametrize(
    ("join", "kind"),
    [
        ("JOIN", "inner"),
        ("INNER JOIN", "inner"),
        ("LEFT JOIN", "left"),
        ("LEFT OUTER JOIN", "left"),
        ("RIGHT JOIN", "right"),
        ("FULL OUTER JOIN", "full"),
        ("SEMI JOIN", "semi"),
        ("ANTI JOIN", "anti"),
    ],
)
def test_join_kinds(join: str, kind: str) -> None:
    tree = cast(
        exp.Select,
        sqlglot.parse_one(
            f'SELECT 1 FROM "fx"."t" AS "t" {join} "fx"."u" AS "u" ON TRUE', dialect="duckdb"
        ),
    )
    assert join_kind(tree.args["joins"][0]) == kind


def test_a_cross_join_and_a_comma_are_cross() -> None:
    for sql in (
        'SELECT 1 FROM "fx"."t" AS "t" CROSS JOIN "fx"."u" AS "u"',
        'SELECT 1 FROM "fx"."t" AS "t", "fx"."u" AS "u"',
    ):
        tree = cast(exp.Select, sqlglot.parse_one(sql, dialect="duckdb"))
        assert join_kind(tree.args["joins"][0]) == "cross"
