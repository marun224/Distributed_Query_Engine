"""The aggregation suite (Phase 2): every query runs as two stages and equals icetl.

`fx.many` has 8 files and a NULL in every column somewhere, so NULL group keys, empty
groups and the stand-in types (`sum()` of an INTEGER is a HUGEINT) all come up.
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


#: Every aggregate in the decomposition table (PLAN §4.2), grouped and global.
PARTIAL: dict[str, Query] = {
    "count, sum, avg, min, max by a boolean": lambda s: (
        many(s)
        .groupBy("flag")
        .agg(
            F.count("*").alias("n"),
            F.count("name").alias("n_name"),
            F.sum("qty").alias("sum_qty"),
            F.sum("amount").alias("sum_amount"),
            F.sum("price").alias("sum_price"),
            F.avg("qty").alias("avg_qty"),
            F.avg("amount").alias("avg_amount"),
            F.avg("price").alias("avg_price"),
            F.min("ts").alias("first_ts"),
            F.max("day").alias("last_day"),
            F.min("name").alias("min_name"),
            F.max("price").alias("max_price"),
        )
    ),
    "global": lambda s: many(s).agg(
        F.count("*").alias("n"),
        F.sum("qty").alias("s"),
        F.avg("amount").alias("a"),
        F.max("tstz").alias("m"),
    ),
    "bool and bit aggregates": lambda s: s.sql(
        "SELECT name, bool_and(flag) AS a, bool_or(flag) AS o, bit_and(qty) AS ba, "
        "bit_or(qty) AS bo, bit_xor(qty) AS bx FROM fx.many GROUP BY name"
    ),
    "having": lambda s: s.sql(
        "SELECT name, sum(qty) AS s FROM fx.many WHERE id > 100 GROUP BY name "
        "HAVING count(*) > 30 AND max(amount) > 0"
    ),
    "global having": lambda s: s.sql("SELECT sum(qty) AS s FROM fx.many HAVING count(*) > 10"),
    "a filter after the aggregate": lambda s: (
        many(s).groupBy("name").agg(F.sum("amount").alias("s")).filter(F.col("s") > 100)
    ),
    "a derived key": lambda s: (
        many(s)
        .withColumn("year", F.year("day"))
        .groupBy("year", "flag")
        .agg(F.count("*").alias("n"), F.max("ts").alias("m"))
    ),
    "expressions over aggregates": lambda s: s.sql(
        "SELECT upper(name) AS u, sum(amount) / count(*) AS mean, count(*) * 2 AS twice "
        "FROM fx.many GROUP BY name"
    ),
    "groupBy().count()": lambda s: many(s).groupBy("day").count(),
    "distinct": lambda s: many(s).select("flag", "name").distinct(),
    "sql distinct": lambda s: s.sql("SELECT DISTINCT flag, year(day) AS y FROM fx.many"),
    "empty groups": lambda s: (
        many(s).filter(F.upper("name") == "NOPE").groupBy("flag").agg(F.sum("qty").alias("s"))
    ),
    "global over no rows": lambda s: (
        many(s)
        .filter(F.upper("name") == "NOPE")
        .agg(F.count("*").alias("n"), F.sum("qty").alias("s"), F.avg("amount").alias("a"))
    ),
    "partitioned table": lambda s: (
        s.table("fx.partitioned")
        .groupBy("as_at_date")
        .agg(F.sum("amount").alias("s"), F.count("*").alias("n"))
    ),
}

#: Not decomposable: raw rows are shuffled by the keys and the aggregate runs as written.
RAW: dict[str, Query] = {
    "count distinct": lambda s: (
        many(s).groupBy("flag").agg(F.countDistinct("name").alias("names"), F.sum("qty").alias("s"))
    ),
    "global count distinct": lambda s: many(s).agg(F.countDistinct("day").alias("days")),
    "ordered list and string_agg": lambda s: s.sql(
        "SELECT flag, list(id ORDER BY id) AS ids, "
        "string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) AS joined FROM fx.many GROUP BY flag"
    ),
    "median and stddev": lambda s: s.sql(
        "SELECT flag, median(amount) AS md, stddev_samp(amount) AS sd FROM fx.many GROUP BY flag"
    ),
    "a FILTER clause": lambda s: s.sql(
        "SELECT flag, count(*) FILTER (WHERE qty > 0) AS positive FROM fx.many GROUP BY flag"
    ),
    "a list with no GROUP BY": lambda s: s.sql(
        "SELECT list(id ORDER BY id) AS ids FROM fx.many WHERE id < 50"
    ),
}

ORDERED: dict[str, Query] = {
    "top groups": lambda s: (
        many(s)
        .groupBy("name")
        .agg(F.sum("amount").alias("s"))
        .filter(F.col("name").isNotNull())
        .orderBy(F.col("s").desc(), "name")
        .limit(5)
    ),
    "sql order by an aggregate not selected": lambda s: s.sql(
        "SELECT name FROM fx.many WHERE name IS NOT NULL GROUP BY name "
        "ORDER BY count(*) DESC, name LIMIT 4"
    ),
    "order by the key, nulls first": lambda s: many(s).groupBy("flag").count().orderBy("flag"),
}


def _run(query: Query, cluster: ClusterSession, oracle: Session) -> tuple[object, object]:
    actual = query(cluster).toArrow()
    run = cluster._last_run
    assert run is not None and run.mode == "distributed", run
    assert run.stages == 2, run
    return actual, query(oracle).toArrow()


@pytest.mark.parametrize("name", list(PARTIAL))
def test_partial_aggregates_equal_icetl(
    name: str, cluster: ClusterSession, oracle: Session, capsys: pytest.CaptureFixture[str]
) -> None:
    actual, expected = _run(PARTIAL[name], cluster, oracle)
    assert_same_rows(actual, expected)
    PARTIAL[name](cluster).explain()
    assert "partial:" in capsys.readouterr().out


@pytest.mark.parametrize("name", list(RAW))
def test_raw_aggregates_equal_icetl(
    name: str, cluster: ClusterSession, oracle: Session, capsys: pytest.CaptureFixture[str]
) -> None:
    actual, expected = _run(RAW[name], cluster, oracle)
    assert_same_rows(actual, expected)
    RAW[name](cluster).explain()
    assert "raw rows, no partial aggregate" in capsys.readouterr().out


@pytest.mark.parametrize("name", list(ORDERED))
def test_ordered_aggregates_equal_icetl_in_order(
    name: str, cluster: ClusterSession, oracle: Session
) -> None:
    actual, expected = _run(ORDERED[name], cluster, oracle)
    assert_same_ordered_rows(actual, expected)


def test_null_keys_meet_in_one_group(cluster: ClusterSession, oracle: Session) -> None:
    def query(s: Session) -> DataFrame:
        return many(s).groupBy("name").count()

    actual = query(cluster).toArrow()
    nulls = [row for row in actual.to_pylist() if row["name"] is None]
    assert len(nulls) == 1
    assert_same_rows(actual, query(oracle).toArrow())


def test_toArrowBatches_streams_an_aggregate(cluster: ClusterSession, oracle: Session) -> None:
    import pyarrow as pa

    query = PARTIAL["count, sum, avg, min, max by a boolean"]
    batches = list(query(cluster).toArrowBatches(batchSize=1))
    assert cluster._last_run is not None and cluster._last_run.stages == 2
    assert_same_rows(pa.Table.from_batches(batches), query(oracle).toArrow())


def test_only_partitions_with_rows_get_a_task(cluster: ClusterSession) -> None:
    """Three flag values (true, false, NULL) reach at most three of the four partitions."""
    many(cluster).groupBy("flag").count().toArrow()
    run = cluster._last_run
    assert run is not None and run.stages == 2
    scan_tasks, final_tasks = run.stage_tasks
    assert scan_tasks >= 2, run
    assert 1 <= final_tasks <= 3, run
