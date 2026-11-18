"""The set-operation suite (Phase 4): UNION [ALL], INTERSECT [ALL], EXCEPT [ALL] equal icetl.

UNION ALL takes no stage of its own: each branch runs in its own stages and whatever
reads the union reads them all. The other set operations hash every column of both sides,
cast to the result's type, and run per partition.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F

from icefleet.session import ClusterSession
from py_test.oracle import assert_same_ordered_rows, assert_same_rows

Query = Callable[[Session], DataFrame]


def many(s: Session) -> DataFrame:
    return s.table("fx.many")


def part(s: Session) -> DataFrame:
    return s.table("fx.partitioned")


SETOPS: dict[str, Query] = {
    "DataFrame union (UNION ALL, by position)": lambda s: (
        many(s).select("id", "name").union(s.table("fx.plain").select("id", "vendor"))
    ),
    "unionByName": lambda s: (
        many(s)
        .select("id", "name")
        .unionByName(s.table("fx.plain").select(F.col("vendor").alias("name"), "id"))
    ),
    "UNION ALL of three": lambda s: s.sql(
        "SELECT id, amount FROM fx.many WHERE qty > 40 UNION ALL "
        "SELECT id, amount FROM fx.partitioned UNION ALL SELECT id, amount FROM fx.plain"
    ),
    "UNION distinct": lambda s: s.sql("SELECT id FROM fx.many UNION SELECT id FROM fx.partitioned"),
    "INTERSECT": lambda s: many(s).select("id").intersect(part(s).select("id")),
    "EXCEPT": lambda s: many(s).select("id").subtract(part(s).select("id")),
    "EXCEPT ALL": lambda s: (
        many(s).select("flag").exceptAll(part(s).select(F.lit(True).alias("flag")))
    ),
    "INTERSECT ALL": lambda s: s.sql(
        "SELECT flag FROM fx.many INTERSECT ALL SELECT flag FROM fx.many WHERE id < 10"
    ),
    "NULLs in a set operation": lambda s: s.sql(
        "SELECT name FROM fx.many INTERSECT SELECT name FROM fx.many WHERE id < 200"
    ),
    "INTEGER and BIGINT branches": lambda s: s.sql(
        "SELECT qty AS v FROM fx.many UNION SELECT id AS v FROM fx.plain"
    ),
    "nested: (A UNION B) EXCEPT C": lambda s: s.sql(
        "(SELECT id FROM fx.plain UNION SELECT id FROM fx.partitioned) "
        "EXCEPT SELECT id FROM fx.many WHERE id < 3"
    ),
    "a UNION ALL under an aggregate": lambda s: (
        many(s)
        .select("id", "name")
        .union(s.table("fx.plain").select("id", "vendor"))
        .groupBy("name")
        .count()
    ),
    "a UNION ALL joined to a table": lambda s: s.sql(
        "SELECT u.id, u.v, p.vendor FROM (SELECT id, amount AS v FROM fx.many WHERE id < 50 "
        "UNION ALL SELECT id, amount FROM fx.partitioned) u JOIN fx.plain p ON u.id = p.id"
    ),
    "a branch with a top-n of its own": lambda s: s.sql(
        "(SELECT id FROM fx.many ORDER BY id DESC LIMIT 3) UNION ALL SELECT id FROM fx.plain"
    ),
    "a branch that aggregates": lambda s: s.sql(
        "SELECT name, count(*) AS n FROM fx.many GROUP BY name "
        "UNION ALL SELECT vendor, count(*) FROM fx.plain GROUP BY vendor"
    ),
}

ORDERED: dict[str, Query] = {
    "UNION ALL, ordered and limited": lambda s: s.sql(
        "SELECT id FROM fx.many UNION ALL SELECT id FROM fx.plain ORDER BY id DESC LIMIT 6"
    ),
    "UNION, ordered": lambda s: s.sql(
        "SELECT id FROM fx.partitioned UNION SELECT id FROM fx.plain ORDER BY 1"
    ),
}


def _distributed(query: Query, cluster: ClusterSession) -> object:
    actual = query(cluster).toArrow()
    run = cluster._last_run
    assert run is not None and run.mode == "distributed", run
    return actual


@pytest.mark.parametrize("name", list(SETOPS))
def test_set_operations_equal_icetl(name: str, cluster: ClusterSession, oracle: Session) -> None:
    actual = _distributed(SETOPS[name], cluster)
    assert_same_rows(actual, SETOPS[name](oracle).toArrow())


@pytest.mark.parametrize("name", list(ORDERED))
def test_ordered_set_operations_equal_icetl(
    name: str, cluster: ClusterSession, oracle: Session
) -> None:
    actual = _distributed(ORDERED[name], cluster)
    assert_same_ordered_rows(actual, ORDERED[name](oracle).toArrow())


def test_union_all_takes_no_stage_of_its_own(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    SETOPS["UNION ALL of three"](cluster).toArrow()
    run = cluster._last_run
    assert run is not None and run.stages == 3, run  # one per branch, nothing more
    SETOPS["UNION ALL of three"](cluster).explain()
    assert "concatenates stages 0, 1, 2" in capsys.readouterr().out


def test_a_distinct_set_operation_hashes_every_column(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    SETOPS["INTERSECT"](cluster).explain()
    out = capsys.readouterr().out
    assert out.count("-> shuffle hash(every column)") == 2, out
    assert "intersect · per partition of hash(every column)" in out
