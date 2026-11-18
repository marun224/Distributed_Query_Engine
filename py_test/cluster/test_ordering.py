"""Ordering (Phase 4): the range sort, OFFSET, top-n subqueries, an order that climbs
through projections, grouping sets and an aggregate over an aggregate -- all equal icetl.

The range sort is forced with `icefleet.sort.coordinatorBytes=0` (the fixtures are far
under the 64 MiB default). Where keys tie, the order among the tied rows is not defined
(divergence.md, 4): those cases check the result is sorted and holds icetl's rows.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator
from typing import Any

import pyarrow as pa
import pytest
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F

from icefleet.session import ClusterSession
from py_test.oracle import assert_same_ordered_rows, assert_same_rows

Query = Callable[[Session], DataFrame]
COORDINATOR_BYTES = "icefleet.sort.coordinatorBytes"


def many(s: Session) -> DataFrame:
    return s.table("fx.many")


@pytest.fixture
def ranged(cluster: ClusterSession) -> Iterator[ClusterSession]:
    """The cluster with every top-level ORDER BY sorted by range partitions."""
    cluster.conf.set(COORDINATOR_BYTES, "0")
    yield cluster
    cluster.conf.unset(COORDINATOR_BYTES)


#: Unique keys: the order is the answer, compared row by row.
UNIQUE: dict[str, Query] = {
    "by id": lambda s: many(s).select("id", "name").orderBy("id"),
    "descending, NULLs last": lambda s: (
        many(s).select("id", "amount").orderBy(F.col("amount").desc_nulls_last(), "id")
    ),
    "NULLs first": lambda s: (
        many(s).select("id", "name").orderBy(F.col("name").asc_nulls_first(), "id")
    ),
    "a timestamp and a decimal": lambda s: (
        many(s).select("id", "ts", "price").orderBy("ts", "price", "id")
    ),
    "a string, descending": lambda s: s.sql(
        "SELECT id, name FROM fx.many ORDER BY name DESC NULLS FIRST, id DESC"
    ),
    "a column not selected": lambda s: s.sql(
        "SELECT name, qty FROM fx.many WHERE qty IS NOT NULL ORDER BY id DESC"
    ),
    "over a join": lambda s: s.sql(
        "SELECT a.id, b.amount FROM fx.many a JOIN fx.many b ON a.id = b.id + 1 ORDER BY a.id"
    ),
    "over a UNION ALL": lambda s: s.sql(
        "SELECT id FROM fx.many UNION ALL SELECT id + 5000 FROM fx.partitioned ORDER BY 1"
    ),
    "with an OFFSET": lambda s: s.sql("SELECT id FROM fx.many ORDER BY id DESC OFFSET 1990"),
    "an aggregate's result": lambda s: s.sql(
        "SELECT name, sum(qty) AS s FROM fx.many GROUP BY name ORDER BY s DESC, name"
    ),
}

#: Ties: sorted, and the same rows.
TIED: dict[str, tuple[Query, list[tuple[str, bool, bool]]]] = {
    # A DataFrame orderBy follows the reference engine: ascending puts NULLs first.
    "a boolean": (lambda s: many(s).select("id", "flag").orderBy("flag"), [("flag", False, True)]),
    "a name, descending": (
        lambda s: s.sql("SELECT name, id FROM fx.many ORDER BY name DESC"),
        [("name", True, False)],
    ),
}


def _distributed(query: Query, cluster: ClusterSession) -> pa.Table:
    actual = query(cluster).toArrow()
    run = cluster._last_run
    assert run is not None and run.mode == "distributed", run
    return actual


@pytest.mark.parametrize("name", list(UNIQUE))
def test_a_range_sort_equals_icetl_in_order(
    name: str, ranged: ClusterSession, oracle: Session, capsys: pytest.CaptureFixture[str]
) -> None:
    actual = _distributed(UNIQUE[name], ranged)
    assert_same_ordered_rows(actual, UNIQUE[name](oracle).toArrow())
    UNIQUE[name](ranged).explain()
    out = capsys.readouterr().out
    assert "range partition by" in out and "reads the range partitions in order" in out, out


def _sorted(rows: list[dict[str, Any]], keys: list[tuple[str, bool, bool]]) -> bool:
    def before(a: Any, b: Any, desc: bool, nulls_first: bool) -> int:
        if a == b:
            return 0
        if a is None:
            return -1 if nulls_first else 1
        if b is None:
            return 1 if nulls_first else -1
        return (-1 if a < b else 1) * (-1 if desc else 1)

    for left, right in itertools.pairwise(rows):
        for column, desc, nulls_first in keys:
            order = before(left[column], right[column], desc, nulls_first)
            if order < 0:
                break
            if order > 0:
                return False
    return True


@pytest.mark.parametrize("name", list(TIED))
def test_a_range_sort_with_ties_is_sorted(
    name: str, ranged: ClusterSession, oracle: Session
) -> None:
    query, keys = TIED[name]
    actual = _distributed(query, ranged)
    assert _sorted(actual.to_pylist(), keys)
    assert_same_rows(actual, query(oracle).toArrow())


def test_a_small_result_is_still_sorted_by_the_coordinator(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    UNIQUE["by id"](cluster).explain()
    out = capsys.readouterr().out
    assert "range partition" not in out and "coordinator  order by" in out, out


def test_a_range_sort_samples_then_partitions_then_sorts(ranged: ClusterSession) -> None:
    UNIQUE["by id"](ranged).toArrow()
    run = ranged._last_run
    assert run is not None and run.stages == 3, run
    scan, ranger, sorter = run.stage_tasks
    assert ranger == scan  # a range task per task of the stage it reads
    assert sorter >= 2  # the sample spread the rows over several partitions


# -- OFFSET, top-n subqueries, orders that climb, aggregates over aggregates -------------

ORDERED: dict[str, Query] = {
    "LIMIT and OFFSET": lambda s: s.sql("SELECT id, qty FROM fx.many ORDER BY id LIMIT 4 OFFSET 7"),
    "orderBy, then select": lambda s: (
        many(s).orderBy(F.col("amount").desc(), "id").select("id", "name")
    ),
    "orderBy, then filter and select": lambda s: (
        many(s).orderBy("id").filter(F.col("qty") > 10).select("id", (F.col("qty") * 2).alias("q2"))
    ),
}

ANY_ORDER: dict[str, Query] = {
    "a top-n subquery under an aggregate": lambda s: (
        many(s).orderBy(F.col("amount").desc_nulls_last(), "id").limit(10).groupBy("flag").count()
    ),
    "a top-n subquery with an OFFSET": lambda s: s.sql(
        "SELECT q.id FROM (SELECT id FROM fx.many ORDER BY id DESC LIMIT 5 OFFSET 2) q"
    ),
    "a LIMIT without ORDER BY in a subquery, then a global count": lambda s: s.sql(
        "SELECT count(*) AS n FROM (SELECT id FROM fx.many LIMIT 30) q"
    ),
    "orderBy, then groupBy (the order means nothing)": lambda s: (
        many(s).orderBy("id").groupBy("flag").count()
    ),
    "an aggregate over an aggregate": lambda s: (
        many(s)
        .groupBy("name")
        .agg(F.sum("qty").alias("s"))
        .groupBy()
        .agg(F.max("s").alias("m"), F.count("*").alias("n"))
    ),
    "an aggregate over an aggregate on the same key": lambda s: s.sql(
        "SELECT name, max(s) AS m FROM (SELECT name, flag, sum(qty) AS s FROM fx.many "
        "GROUP BY name, flag) q GROUP BY name"
    ),
}

GROUPING: dict[str, Query] = {
    "rollup": lambda s: many(s).rollup("flag", "name").agg(F.sum("qty").alias("s")),
    "cube": lambda s: many(s).cube("flag").agg(F.count("*").alias("n"), F.avg("amount").alias("a")),
    "grouping sets with grouping()": lambda s: s.sql(
        "SELECT flag, name, grouping(flag, name) AS g, sum(qty) AS s FROM fx.many "
        "GROUP BY GROUPING SETS ((flag), (name), ())"
    ),
    "a rollup over a raw aggregate": lambda s: s.sql(
        "SELECT flag, count(DISTINCT name) AS n FROM fx.many GROUP BY ROLLUP (flag)"
    ),
    "a key and a rollup, filtered": lambda s: s.sql(
        "SELECT flag, name, count(*) AS n FROM fx.many WHERE id < 500 "
        "GROUP BY flag, ROLLUP (name) HAVING count(*) > 2"
    ),
}


@pytest.mark.parametrize("name", list(ORDERED))
def test_ordered_queries_equal_icetl_in_order(
    name: str, cluster: ClusterSession, oracle: Session
) -> None:
    actual = _distributed(ORDERED[name], cluster)
    assert_same_ordered_rows(actual, ORDERED[name](oracle).toArrow())


@pytest.mark.parametrize("name", list(ANY_ORDER))
def test_subquery_orders_and_aggregates_equal_icetl(
    name: str, cluster: ClusterSession, oracle: Session
) -> None:
    actual = _distributed(ANY_ORDER[name], cluster)
    assert_same_rows(actual, ANY_ORDER[name](oracle).toArrow())


@pytest.mark.parametrize("name", list(GROUPING))
def test_grouping_sets_equal_icetl(
    name: str, cluster: ClusterSession, oracle: Session, capsys: pytest.CaptureFixture[str]
) -> None:
    actual = _distributed(GROUPING[name], cluster)
    assert_same_rows(actual, GROUPING[name](oracle).toArrow())
    GROUPING[name](cluster).explain()
    out = capsys.readouterr().out
    assert "grouping sets, one shuffle" in out and out.count("-> shuffle hash(") == 1, out
