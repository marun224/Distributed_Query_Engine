"""The oracle suite: every query on `local[2]` and `local[3]` equals icetl's answer.

Each case also asserts *how* it ran. A plan that quietly stopped distributing would still
return the right rows, so the mode is part of what is tested.
"""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa
import pytest
from icetl.errors import UnsupportedFeatureError
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F

from icefleet.session import ClusterSession
from py_test.oracle import assert_same_rows, sorted_rows

Query = Callable[[Session], DataFrame]

DISTRIBUTED: dict[str, Query] = {
    "every type, every row": lambda s: s.table("fx.many"),
    "filter and expressions": lambda s: (
        s.table("fx.many")
        .filter((F.col("qty") > 0) & F.col("name").isNotNull())
        .select(
            "id",
            (F.col("amount") * 2).alias("double_amount"),
            F.upper("name").alias("loud"),
            F.when(F.col("flag"), "yes").otherwise("no").alias("flag_text"),
            F.col("price").cast("double").alias("price_d"),
            F.year("day").alias("year"),
            F.concat(F.col("name"), F.lit("/"), F.col("qty").cast("string")).alias("label"),
        )
    ),
    "sql surface": lambda s: s.sql(
        "SELECT id, name, tstz FROM fx.many WHERE qty % 3 = 0 AND name IS NOT NULL"
    ),
    # Not prunable from file statistics, so every file is read and no row survives: the
    # empty answer must still carry its schema.
    "filter matching nothing": lambda s: s.table("fx.many").filter(F.upper("name") == "NOPE"),
    "partition pruning": lambda s: s.table("fx.partitioned").filter(
        F.col("as_at_date") >= "2026-08-16"
    ),
    "wide table, few columns": lambda s: s.table("fx.wide").select("id", "col_007", "col_150"),
    "nested types": lambda s: s.table("fx.nested").select(
        "id", "person", "tags", "scores", F.col("person.name").alias("who")
    ),
    "plain": lambda s: s.table("fx.plain"),
    "limit above the row count": lambda s: (
        s.table("fx.many").filter(F.col("qty") > 40).limit(100_000)
    ),
}

SINGLE_NODE: dict[str, tuple[Query, str]] = {
    "group by": (lambda s: s.table("fx.many").groupBy("flag").agg(F.sum("qty")), "Phase 2"),
    "order by": (lambda s: s.table("fx.many").orderBy("id"), "Phase 4"),
    "join": (
        lambda s: s.table("fx.plain").join(s.table("fx.plain").select("id"), "id"),
        "Phase 3",
    ),
    "distinct": (lambda s: s.table("fx.many").select("flag").distinct(), "Phase 2"),
    "renamed columns": (lambda s: s.table("fx.renamed"), "Phase 7"),
    "no data files": (lambda s: s.table("fx.empty"), "no data files"),
    # Iceberg's column statistics prune every file before anything is distributed.
    "pruned to nothing": (lambda s: s.table("fx.many").filter(F.col("id") < 0), "no data files"),
}


@pytest.mark.parametrize("name", list(DISTRIBUTED))
def test_distributed_equals_icetl(name: str, cluster: ClusterSession, oracle: Session) -> None:
    query = DISTRIBUTED[name]
    actual = query(cluster).toArrow()
    run = cluster._last_run
    assert run is not None and run.mode == "distributed", run
    assert_same_rows(actual, query(oracle).toArrow())


def test_many_is_cut_into_several_tasks(cluster: ClusterSession) -> None:
    cluster.table("fx.many").toArrow()
    run = cluster._last_run
    assert run is not None and run.mode == "distributed"
    assert run.tasks >= 2
    assert run.rows == 8 * 250


@pytest.mark.parametrize("name", list(SINGLE_NODE))
def test_uncuttable_runs_single_node_and_equals_icetl(
    name: str, cluster: ClusterSession, oracle: Session
) -> None:
    query, reason = SINGLE_NODE[name]
    actual = query(cluster).toArrow()
    run = cluster._last_run
    assert run is not None and run.mode == "single-node", run
    assert reason in (run.reason or "")
    expected = query(oracle).toArrow()
    if name == "order by":
        assert actual.equals(expected)  # the order itself is the answer
    else:
        assert_same_rows(actual, expected)


def test_limit_returns_that_many_rows_of_the_answer(
    cluster: ClusterSession, oracle: Session
) -> None:
    """LIMIT without ORDER BY returns *some* n rows, as the reference does (PLAN §4.7)."""
    actual = cluster.table("fx.many").filter(F.col("qty") > 0).limit(25).toArrow()
    run = cluster._last_run
    assert run is not None and run.mode == "distributed"
    assert actual.num_rows == 25
    everything = {repr(r) for r in sorted_rows(oracle.table("fx.many").toArrow())}
    assert all(repr(r) in everything for r in sorted_rows(actual))
    assert all(row["qty"] > 0 for row in actual.to_pylist())


def test_stream_equals_icetl(cluster: ClusterSession, oracle: Session) -> None:
    query = DISTRIBUTED["filter and expressions"]
    batches = list(query(cluster).toArrowBatches(batchSize=100))
    run = cluster._last_run
    assert run is not None and run.mode == "distributed"
    actual = pa.Table.from_batches(batches)
    assert_same_rows(actual, query(oracle).toArrow())


def test_collect_and_count_equal_icetl(cluster: ClusterSession, oracle: Session) -> None:
    def frame(s: Session) -> DataFrame:
        return s.table("fx.plain").filter(F.col("amount") > 15)

    assert sorted(map(repr, frame(cluster).collect())) == sorted(map(repr, frame(oracle).collect()))
    assert frame(cluster).count() == frame(oracle).count()
    assert cluster.table("fx.many").count() == 2000
    assert cluster._last_run is not None and cluster._last_run.mode == "metadata"


def test_merge_on_read_is_refused_exactly_as_icetl_refuses_it(cluster: ClusterSession) -> None:
    with pytest.raises(UnsupportedFeatureError, match="merge-on-read"):
        cluster.table("fx.mor").toArrow()
