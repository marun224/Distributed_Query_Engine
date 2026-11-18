"""The join suite (Phase 3): every join equals icetl, broadcast and shuffled.

Each case runs twice on `local[2]` and `local[3]`: with the default broadcast threshold,
where the fixtures are small enough that every side a join may broadcast is broadcast,
and with `icefleet.broadcast.thresholdBytes=0`, where every join is a shuffle hash join.
`fx.many` (2000 rows, 8 files, a NULL in every column somewhere) plays the large side,
self-joined for large x large; `fx.plain` (5 rows) and `fx.partitioned` (12 rows) the
small ones.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F

from icefleet.session import ClusterSession
from py_test.oracle import assert_same_ordered_rows, assert_same_rows

Query = Callable[[Session], DataFrame]

THRESHOLD = "icefleet.broadcast.thresholdBytes"


def many(s: Session) -> DataFrame:
    return s.table("fx.many")


def plain(s: Session) -> DataFrame:
    return s.table("fx.plain")


#: Distributed whatever the threshold: broadcast when a side may be, else shuffled.
JOINS: dict[str, Query] = {
    "inner, DataFrame on a name": lambda s: many(s).join(plain(s), "id"),
    "inner, sql": lambda s: s.sql(
        "SELECT a.id, a.amount, b.vendor FROM fx.many a JOIN fx.plain b ON a.id = b.id"
    ),
    "left": lambda s: s.sql(
        "SELECT a.id, a.qty, b.vendor FROM fx.many a LEFT JOIN fx.plain b ON a.id = b.id "
        "WHERE a.qty > 0"
    ),
    "right": lambda s: s.sql(
        "SELECT a.id, b.id AS bid, b.vendor FROM fx.many a RIGHT JOIN fx.plain b ON a.id = b.id + 2"
    ),
    "full": lambda s: s.sql(
        "SELECT a.id, b.id AS bid, b.vendor FROM fx.partitioned a FULL JOIN fx.plain b "
        "ON a.id = b.id"
    ),
    "left semi": lambda s: many(s).join(plain(s), "id", "left_semi"),
    "left anti": lambda s: many(s).join(plain(s), "id", "left_anti"),
    "large x large on an expression": lambda s: s.sql(
        "SELECT a.id, a.name, b.amount FROM fx.many a JOIN fx.many b ON a.id = b.id + 1"
    ),
    "three-way": lambda s: s.sql(
        "SELECT a.id, b.vendor, c.as_at_date, c.amount FROM fx.many a "
        "JOIN fx.plain b ON a.id = b.id JOIN fx.partitioned c ON c.id = a.id"
    ),
    "three-way, the same key": lambda s: s.sql(
        "SELECT a.id, b.qty, c.name FROM fx.many a JOIN fx.many b ON a.id = b.id "
        "JOIN fx.many c ON c.id = a.id WHERE a.qty > 10"
    ),
    "INTEGER = BIGINT, negative values": lambda s: s.sql(
        "SELECT a.id, a.qty, b.id AS bid FROM fx.many a JOIN fx.many b ON a.qty = b.id - 60 "
        "WHERE a.id < 400"
    ),
    "DECIMAL = BIGINT": lambda s: s.sql(
        "SELECT a.id, a.price, b.id AS bid FROM fx.many a JOIN fx.many b ON a.price = b.id"
    ),
    "DOUBLE = BIGINT": lambda s: s.sql(
        "SELECT a.id, a.amount, b.id AS bid FROM fx.many a JOIN fx.many b ON a.amount = b.id"
    ),
    "string keys with NULLs": lambda s: s.sql(
        "SELECT a.id, b.id AS bid, a.name FROM fx.many a JOIN fx.many b ON a.name = b.name "
        "WHERE a.id < 60 AND b.id < 300"
    ),
    "NULL keys kept by a left join": lambda s: s.sql(
        "SELECT a.id, a.name, b.id AS bid FROM fx.many a LEFT JOIN fx.many b "
        "ON a.name = b.name AND b.id < 100 WHERE a.id < 80"
    ),
    "null-safe equality": lambda s: s.sql(
        "SELECT a.id, b.id AS bid FROM fx.many a JOIN fx.many b "
        "ON a.name IS NOT DISTINCT FROM b.name WHERE a.id < 120 AND b.id < 120"
    ),
    "an extra condition in ON": lambda s: s.sql(
        "SELECT a.id, a.amount, b.amount AS bamount FROM fx.many a JOIN fx.plain b "
        "ON a.id = b.id AND a.amount > b.amount"
    ),
    "a join after an aggregate": lambda s: s.sql(
        "SELECT x.name, x.s, b.id, b.amount FROM "
        "(SELECT name, sum(amount) AS s FROM fx.many GROUP BY name) x "
        "JOIN fx.many b ON x.name = b.name WHERE b.id < 200"
    ),
    "two aggregates joined on their keys": lambda s: s.sql(
        "SELECT x.name, x.n, y.m FROM (SELECT name, count(*) AS n FROM fx.many GROUP BY name) x "
        "JOIN (SELECT name, max(qty) AS m FROM fx.many WHERE flag GROUP BY name) y "
        "ON x.name = y.name"
    ),
    "an aggregate after a join": lambda s: s.sql(
        "SELECT b.vendor, sum(a.amount) AS s, count(*) AS n, avg(a.qty) AS q "
        "FROM fx.many a JOIN fx.plain b ON a.id = b.id GROUP BY b.vendor"
    ),
    "an aggregate on the join key": lambda s: s.sql(
        "SELECT a.id, count(*) AS n, sum(b.amount) AS s, max(b.name) AS m "
        "FROM fx.many a JOIN fx.many b ON a.id = b.id + 7 GROUP BY a.id"
    ),
    "a global aggregate over a join": lambda s: s.sql(
        "SELECT count(*) AS n, sum(b.qty) AS s FROM fx.many a JOIN fx.many b ON a.id = b.id"
    ),
    "DataFrame join, filter, aggregate": lambda s: (
        many(s)
        .join(plain(s), "id")
        .filter(F.col("qty") > -100)
        .groupBy("vendor")
        .agg(F.sum("amount").alias("s"), F.count("*").alias("n"))
    ),
    "IN (subquery)": lambda s: s.sql(
        "SELECT id, name FROM fx.many WHERE id IN (SELECT id FROM fx.partitioned) AND qty > 0"
    ),
    "IN (subquery) over NULLs": lambda s: s.sql(
        "SELECT id, name FROM fx.many WHERE name IN (SELECT name FROM fx.many WHERE id < 30)"
    ),
    "distinct over a join": lambda s: s.sql(
        "SELECT DISTINCT b.vendor, a.flag FROM fx.many a JOIN fx.plain b ON a.id = b.id"
    ),
    "a join of a filtered projection": lambda s: (
        many(s)
        .filter(F.col("qty") > 0)
        .select("id", (F.col("amount") * 2).alias("double_amount"))
        .join(plain(s).select("id", "vendor"), "id", "left")
    ),
}

#: Distributed only by broadcasting a side: no equality to shuffle by.
BROADCAST_ONLY: dict[str, Query] = {
    "cross": lambda s: plain(s).crossJoin(s.table("fx.partitioned").select("as_at_date")),
    "non-equi inner": lambda s: s.sql(
        "SELECT a.id, b.id AS bid FROM fx.plain a JOIN fx.partitioned b ON a.id < b.id"
    ),
    "keys that do not hash alike (DATE = TIMESTAMP)": lambda s: s.sql(
        "SELECT a.id, b.id AS bid FROM fx.many a JOIN fx.many b ON a.day = b.ts "
        "WHERE a.id < 50 AND b.id < 50"
    ),
}

ORDERED: dict[str, Query] = {
    "order by and limit over a join": lambda s: s.sql(
        "SELECT a.id, b.vendor FROM fx.many a JOIN fx.plain b ON a.id = b.id "
        "ORDER BY a.id DESC LIMIT 3"
    ),
    "order by a column not selected": lambda s: s.sql(
        "SELECT a.name, b.amount FROM fx.many a JOIN fx.many b ON a.id = b.id "
        "WHERE a.id < 40 ORDER BY b.id"
    ),
}


@pytest.fixture(params=["broadcast", "shuffle"])
def strategy(request: pytest.FixtureRequest, cluster: ClusterSession) -> Iterator[str]:
    cluster.conf.set(THRESHOLD, "64MiB" if request.param == "broadcast" else "0")
    yield str(request.param)
    cluster.conf.unset(THRESHOLD)


@pytest.fixture
def shuffled(cluster: ClusterSession) -> Iterator[ClusterSession]:
    """The cluster with broadcasting off: every join with keys is a shuffle hash join."""
    cluster.conf.set(THRESHOLD, "0")
    yield cluster
    cluster.conf.unset(THRESHOLD)


def _distributed(query: Query, cluster: ClusterSession) -> object:
    actual = query(cluster).toArrow()
    run = cluster._last_run
    assert run is not None and run.mode == "distributed", run
    return actual


@pytest.mark.parametrize("name", list(JOINS))
def test_joins_equal_icetl(
    name: str,
    strategy: str,
    cluster: ClusterSession,
    oracle: Session,
    capsys: pytest.CaptureFixture[str],
) -> None:
    actual = _distributed(JOINS[name], cluster)
    assert_same_rows(actual, JOINS[name](oracle).toArrow())
    JOINS[name](cluster).explain()
    out = capsys.readouterr().out
    if strategy == "shuffle":
        assert "shuffle hash join" in out and "broadcast hash join" not in out, out


@pytest.mark.parametrize("name", list(BROADCAST_ONLY))
def test_a_join_without_shuffle_keys_broadcasts_or_runs_single_node(
    name: str, strategy: str, cluster: ClusterSession, oracle: Session
) -> None:
    actual = BROADCAST_ONLY[name](cluster).toArrow()
    run = cluster._last_run
    assert run is not None
    assert run.mode == ("distributed" if strategy == "broadcast" else "single-node"), run
    assert_same_rows(actual, BROADCAST_ONLY[name](oracle).toArrow())


@pytest.mark.parametrize("name", list(ORDERED))
def test_ordered_joins_equal_icetl_in_order(
    name: str, strategy: str, cluster: ClusterSession, oracle: Session
) -> None:
    actual = _distributed(ORDERED[name], cluster)
    assert_same_ordered_rows(actual, ORDERED[name](oracle).toArrow())


# -- strategy, as explain() names it ---------------------------------------------------


def _explain(query: Query, cluster: ClusterSession, capsys: pytest.CaptureFixture[str]) -> str:
    query(cluster).explain()
    return capsys.readouterr().out


def test_a_small_side_is_broadcast(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _explain(JOINS["inner, sql"], cluster, capsys)
    assert "join: inner, broadcast hash join · the right side from stage 0" in out, out
    assert "-> broadcast" in out
    assert "fx.plain as b" in out and "fx.many as a" in out, out
    assert "stage 1  scan " in out
    assert "reads stage 0 (broadcast)" in out


def test_a_left_join_never_broadcasts_its_left_side(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    """`fx.plain` is the small one, but on the preserved side of a left join it cannot be
    broadcast: its unmatched rows would come back once per task."""
    out = _explain(
        lambda s: s.sql("SELECT a.id, b.qty FROM fx.plain a LEFT JOIN fx.many b ON a.id = b.id"),
        cluster,
        capsys,
    )
    assert "broadcast hash join · the right side" in out, out


def test_a_full_join_is_always_shuffled(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _explain(JOINS["full"], cluster, capsys)
    assert "join: full, shuffle hash join" in out, out


def test_large_sides_are_shuffled(
    shuffled: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _explain(JOINS["large x large on an expression"], shuffled, capsys)
    assert "join: inner, shuffle hash join" in out, out
    assert out.count("-> shuffle hash(") == 2, out


def test_a_side_already_partitioned_by_the_keys_is_not_reshuffled(
    shuffled: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _explain(JOINS["three-way, the same key"], shuffled, capsys)
    # a and b are shuffled by id once each; c joins the result without reshuffling it.
    assert out.count("-> shuffle hash(") == 3, out
    assert "the left side already partitioned by the keys" in out, out

    out = _explain(JOINS["two aggregates joined on their keys"], shuffled, capsys)
    assert "both sides already partitioned by the keys" in out, out
    assert out.count("-> shuffle hash(") == 2, out  # the two partials only


def test_an_aggregate_on_the_join_key_runs_in_place(
    shuffled: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _explain(
        lambda s: s.sql(
            "SELECT a.id, count(*) AS n FROM fx.many a JOIN fx.many b ON a.id = b.id GROUP BY a.id"
        ),
        shuffled,
        capsys,
    )
    assert "aggregate in place" in out, out
    assert out.count("-> shuffle hash(") == 2, out  # the join's two sides, nothing more


def test_a_shuffle_join_is_three_stages(shuffled: ClusterSession) -> None:
    """Both sides scan and shuffle (in one wave, `test_scheduler.py`), then the join."""
    JOINS["large x large on an expression"](shuffled).toArrow()
    run = shuffled._last_run
    assert run is not None and run.stages == 3, run
    scan_a, scan_b, join_tasks = run.stage_tasks
    assert scan_a >= 2 and scan_b >= 2 and join_tasks >= 1, run
