"""The Phase 1 shape recogniser, on hand-written trees shaped like icetl's optimized ones."""

from __future__ import annotations

import re
from typing import Any, cast

import pytest
import sqlglot
from sqlglot import exp

from icefleet.errors import Uncuttable
from icefleet.plan.rel import Project, from_sqlglot

SOURCES = cast(Any, {"fx.t": object(), "fx.u": object()})


def recognise(sql: str, udfs: tuple[str, ...] = ()) -> Project | Uncuttable:
    tree = cast(exp.Expression, sqlglot.parse_one(sql, dialect="duckdb"))
    return from_sqlglot(tree, SOURCES, udf_names=udfs)


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
def test_project_over_scan_is_cuttable(sql: str, limit: int | None) -> None:
    shape = recognise(sql)
    assert isinstance(shape, Project), shape
    assert shape.child.key == "fx.t"
    assert shape.limit == limit


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ('SELECT "t"."b", COUNT(*) FROM "fx"."t" AS "t" GROUP BY "t"."b"', "Phase 2"),
        ('SELECT SUM("t"."a") FROM "fx"."t" AS "t"', "aggregate"),
        ('SELECT DISTINCT "t"."a" FROM "fx"."t" AS "t"', "DISTINCT"),
        ('SELECT * FROM "fx"."t" AS "t" JOIN "fx"."u" AS "u" ON "t"."a" = "u"."a"', "join"),
        ('SELECT "t"."a" FROM "fx"."t" AS "t" ORDER BY "t"."a"', "ORDER BY"),
        ('SELECT ROW_NUMBER() OVER (ORDER BY "t"."a") FROM "fx"."t" AS "t"', "aggregate|window"),
        ('SELECT "t"."a" FROM "fx"."t" AS "t" WHERE "t"."a" IN (SELECT 1)', "subquery"),
        ('SELECT "a" FROM "fx"."t" UNION ALL SELECT "a" FROM "fx"."u"', "set operation"),
        ('SELECT "t"."a" FROM "fx"."t" AS "t" LIMIT 5 OFFSET 2', "OFFSET"),
        ('SELECT "c"."x" FROM "icetl_cache_3" AS "c"', "locally"),
        ("SELECT \"a\" FROM read_parquet('x.parquet')", "single Iceberg table"),
        ("INSERT INTO t VALUES (1)", "not a SELECT"),
    ],
)
def test_everything_else_is_uncuttable_with_a_reason(sql: str, reason: str) -> None:
    shape = recognise(sql)
    assert isinstance(shape, Uncuttable)
    assert re.search(reason, shape.reason), shape.reason


def test_a_udf_call_is_uncuttable() -> None:
    sql = 'SELECT slen("t"."b") AS "n" FROM "fx"."t" AS "t"'
    assert isinstance(recognise(sql), Project)
    shape = recognise(sql, udfs=("SLEN",))
    assert isinstance(shape, Uncuttable) and "Phase 8" in shape.reason
