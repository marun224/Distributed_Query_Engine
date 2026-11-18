"""The window suite (Phase 4): every window query equals icetl, on `local[2]` / `local[3]`.

Windows with a PARTITION BY are hashed by it; windows sharing some keys take one shuffle,
windows sharing none a chain; a window with no PARTITION BY runs in one partition
(decision 18). icetl's own window cases (`tests/fixture/test_window.py` in the icetl
checkout) run here over the `ties`, `gaps` and `groups` fixture tables, which hold the
frames icetl builds with `createDataFrame`, split over two files each.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F
from icetl.sql.window import Window

from icefleet.session import ClusterSession
from py_test.oracle import assert_same_rows

Query = Callable[[Session], DataFrame]


def many(s: Session) -> DataFrame:
    return s.table("fx.many")


def ties(s: Session) -> DataFrame:
    return s.table("fx.ties")


BY_NAME = Window.partitionBy("name").orderBy("id")

WINDOWS: dict[str, Query] = {
    "row_number per partition": lambda s: many(s).select(
        "id", "name", F.row_number().over(BY_NAME).alias("n")
    ),
    "rank, dense_rank, percent_rank, cume_dist, ntile": lambda s: many(s).select(
        "id",
        F.rank().over(Window.partitionBy("flag").orderBy(F.col("qty").desc(), "id")).alias("r"),
        F.dense_rank().over(Window.partitionBy("flag").orderBy("qty")).alias("d"),
        F.percent_rank().over(Window.partitionBy("flag").orderBy("qty", "id")).alias("p"),
        F.cume_dist().over(Window.partitionBy("flag").orderBy("qty")).alias("c"),
        F.ntile(3).over(Window.partitionBy("flag").orderBy("id")).alias("t"),
    ),
    "running total, the default RANGE frame": lambda s: many(s).select(
        "id", F.sum("qty").over(Window.partitionBy("name").orderBy("qty")).alias("run")
    ),
    "a sliding ROWS frame": lambda s: many(s).select(
        "id",
        F.avg("amount")
        .over(Window.partitionBy("flag").orderBy("id").rowsBetween(-2, 2))
        .alias("a"),
    ),
    "lag and lead with a default": lambda s: many(s).select(
        "id",
        F.lag("qty", 1).over(BY_NAME).alias("prev"),
        F.lead("qty", 2, -1).over(BY_NAME).alias("next2"),
    ),
    "a whole-partition aggregate": lambda s: many(s).select(
        "id", F.max("amount").over(Window.partitionBy("flag")).alias("m")
    ),
    "no PARTITION BY: one partition": lambda s: many(s).select(
        "id", F.sum("qty").over(Window.orderBy("id")).alias("run")
    ),
    "windows sharing a key: one shuffle": lambda s: many(s).select(
        "id",
        F.rank().over(Window.partitionBy("name").orderBy("qty", "id")).alias("r"),
        F.count("*").over(Window.partitionBy("name", "flag")).alias("c"),
    ),
    "windows sharing no key: a chain": lambda s: many(s).select(
        "id",
        F.rank().over(Window.partitionBy("name").orderBy("qty", "id")).alias("r"),
        F.max("amount").over(Window.partitionBy("flag")).alias("m"),
        F.count("*").over(Window.orderBy("id")).alias("c"),
    ),
    "QUALIFY top 2 per key": lambda s: s.sql(
        "SELECT id, name, qty FROM fx.many "
        "QUALIFY row_number() OVER (PARTITION BY name ORDER BY qty DESC, id) <= 2"
    ),
    "QUALIFY with windows in two groups": lambda s: s.sql(
        "SELECT id, max(amount) OVER (PARTITION BY flag) AS m FROM fx.many "
        "QUALIFY rank() OVER (PARTITION BY name ORDER BY id) = 1"
    ),
    "a filter after the window": lambda s: (
        many(s)
        .select("id", "name", F.row_number().over(BY_NAME).alias("n"))
        .filter(F.col("n") <= 3)
    ),
    "a filter before the window": lambda s: (
        many(s).filter(F.col("qty") > 0).select("id", F.row_number().over(BY_NAME).alias("n"))
    ),
    "a window over an aggregate": lambda s: s.sql(
        "SELECT name, sum(qty) AS s, rank() OVER (ORDER BY sum(qty) DESC, name) AS r "
        "FROM fx.many GROUP BY name"
    ),
    "a window over an aggregate, partitioned": lambda s: s.sql(
        "SELECT flag, name, count(*) AS n, "
        "row_number() OVER (PARTITION BY flag ORDER BY count(*) DESC, name) AS r "
        "FROM fx.many GROUP BY flag, name"
    ),
    "a window over a join": lambda s: s.sql(
        "SELECT a.id, b.vendor, row_number() OVER (PARTITION BY b.vendor ORDER BY a.id) AS n "
        "FROM fx.many a JOIN fx.plain b ON a.id = b.id"
    ),
    "a window on the join key: no reshuffle": lambda s: s.sql(
        "SELECT a.id, b.amount, sum(b.qty) OVER (PARTITION BY a.id) AS s "
        "FROM fx.many a JOIN fx.many b ON a.id = b.id"
    ),
    "DISTINCT over a window": lambda s: s.sql(
        "SELECT DISTINCT flag, count(*) OVER (PARTITION BY flag) AS n FROM fx.many"
    ),
    "first_value ignoring NULLs": lambda s: many(s).select(
        "id", F.first_value("name", True).over(Window.partitionBy("flag").orderBy("id")).alias("f")
    ),
    "a window over a UNION ALL": lambda s: (
        many(s)
        .select("id", "name")
        .union(s.table("fx.plain").select("id", "vendor"))
        .select("id", F.row_number().over(Window.partitionBy("name").orderBy("id")).alias("n"))
    ),
    "DISTINCT ON, ordered": lambda s: s.sql(
        "SELECT DISTINCT ON (name) name, id, qty FROM fx.many ORDER BY name, qty DESC, id"
    ),
}

#: icetl's own window cases (tests/fixture/test_window.py), on the fixture tables.
ICETL: dict[str, Query] = {
    "row_number never ties": lambda s: ties(s).select(
        "id", F.row_number().over(Window.orderBy("x", "id")).alias("n")
    ),
    "rank leaves a gap": lambda s: ties(s).select(
        "id", F.rank().over(Window.orderBy("x")).alias("r")
    ),
    "dense_rank leaves none": lambda s: ties(s).select(
        "id", F.dense_rank().over(Window.orderBy("x")).alias("r")
    ),
    "percent_rank": lambda s: ties(s).select(
        "id", F.percent_rank().over(Window.orderBy("x")).alias("p")
    ),
    "cume_dist": lambda s: ties(s).select("id", F.cume_dist().over(Window.orderBy("x")).alias("c")),
    "ntile": lambda s: ties(s).select("id", F.ntile(2).over(Window.orderBy("id")).alias("b")),
    "the default frame includes tied rows": lambda s: ties(s).select(
        "id", F.sum("x").over(Window.orderBy("x")).alias("run")
    ),
    "an explicit ROWS frame climbs": lambda s: ties(s).select(
        "id",
        F.sum("x")
        .over(Window.orderBy("x", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .alias("run"),
    ),
    "no ordering, the whole partition": lambda s: ties(s).select(
        "id", F.sum("x").over(Window.partitionBy(F.lit(1))).alias("total")
    ),
    "RANGE 0..0 is the tied rows": lambda s: ties(s).select(
        "id", F.count(F.lit(1)).over(Window.orderBy("x").rangeBetween(0, 0)).alias("n")
    ),
    "a centred ROWS frame": lambda s: ties(s).select(
        "id", F.sum("x").over(Window.orderBy("id").rowsBetween(-1, 1)).alias("s")
    ),
    "a value range": lambda s: ties(s).select(
        "id", F.count(F.lit(1)).over(Window.orderBy("x").rangeBetween(-10, 10)).alias("n")
    ),
    "lag and lead": lambda s: ties(s).select(
        "id",
        F.lag("x").over(Window.orderBy("id")).alias("prev"),
        F.lead("x").over(Window.orderBy("id")).alias("next"),
    ),
    "lag with a default": lambda s: ties(s).select(
        "id", F.lag("x", 1, -1.0).over(Window.orderBy("id")).alias("p")
    ),
    "last_value and its frame": lambda s: ties(s).select(
        "id",
        F.last_value("x").over(Window.orderBy("id")).alias("dflt"),
        F.last_value("x")
        .over(
            Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        .alias("fixed"),
    ),
    "nth_value": lambda s: ties(s).select(
        "id", F.nth_value("x", 2).over(Window.orderBy("id")).alias("n")
    ),
    "ignoreNulls": lambda s: s.table("fx.gaps").select(
        "id",
        F.first_value("v").over(Window.orderBy("id")).alias("plain"),
        F.first_value("v", True).over(Window.orderBy("id")).alias("skipping"),
    ),
    "each partition on its own": lambda s: s.table("fx.groups").select(
        "g", "x", F.row_number().over(Window.partitionBy("g").orderBy("x")).alias("n")
    ),
    "a running total restarts per partition": lambda s: s.table("fx.groups").select(
        "g", "y", F.sum("y").over(Window.partitionBy("g").orderBy("y")).alias("run")
    ),
    "over a catalog table": lambda s: s.table("fx.plain").select(
        "id", F.row_number().over(Window.partitionBy("vendor").orderBy("id")).alias("n")
    ),
    "NULLs first inside a window": lambda s: s.table("fx.plain").select(
        "vendor", "id", F.row_number().over(Window.orderBy("vendor", "id")).alias("n")
    ),
}


def _distributed(query: Query, cluster: ClusterSession) -> object:
    actual = query(cluster).toArrow()
    run = cluster._last_run
    assert run is not None and run.mode == "distributed", run
    return actual


@pytest.mark.parametrize("name", list(WINDOWS))
def test_windows_equal_icetl(name: str, cluster: ClusterSession, oracle: Session) -> None:
    actual = _distributed(WINDOWS[name], cluster)
    assert_same_rows(actual, WINDOWS[name](oracle).toArrow())


@pytest.mark.parametrize("name", list(ICETL))
def test_icetl_window_cases_equal_icetl(
    name: str, cluster: ClusterSession, oracle: Session
) -> None:
    actual = _distributed(ICETL[name], cluster)
    assert_same_rows(actual, ICETL[name](oracle).toArrow())


def _explain(query: Query, cluster: ClusterSession, capsys: pytest.CaptureFixture[str]) -> str:
    query(cluster).explain()
    return capsys.readouterr().out


def test_a_partitioned_window_is_hashed_by_its_keys(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _explain(WINDOWS["row_number per partition"], cluster, capsys)
    assert 'window: row_number · partition by "name"' in out, out
    assert out.count("-> shuffle hash(") == 1, out


def test_a_window_without_partition_by_says_it_runs_in_one_task(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _explain(WINDOWS["no PARTITION BY: one partition"], cluster, capsys)
    assert "no PARTITION BY: one partition, the whole input in one task (decision 18)" in out


def test_windows_sharing_a_key_take_one_shuffle_and_others_a_chain(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _explain(WINDOWS["windows sharing a key: one shuffle"], cluster, capsys)
    assert out.count("-> shuffle hash(") == 1, out
    out = _explain(WINDOWS["windows sharing no key: a chain"], cluster, capsys)
    assert out.count("-> shuffle hash(") == 2 and "-> shuffle to one partition" in out, out


def test_a_window_on_a_key_already_partitioned_by_is_not_reshuffled(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    cluster.conf.set("icefleet.broadcast.thresholdBytes", "0")
    try:
        out = _explain(WINDOWS["a window on the join key: no reshuffle"], cluster, capsys)
    finally:
        cluster.conf.unset("icefleet.broadcast.thresholdBytes")
    assert "rows already partitioned by hash(" in out, out
    assert out.count("-> shuffle hash(") == 2, out  # the join's two sides only


def test_distinct_on_keeps_the_first_row_of_its_order(
    cluster: ClusterSession, oracle: Session
) -> None:
    """Which row survives is the one the query's own ORDER BY puts first, as single-node."""
    actual = _distributed(WINDOWS["DISTINCT ON, ordered"], cluster)
    expected = WINDOWS["DISTINCT ON, ordered"](oracle).toArrow()
    assert actual.to_pylist() == expected.to_pylist()  # type: ignore[attr-defined]


def test_drop_duplicates_keeps_one_row_per_key(cluster: ClusterSession) -> None:
    """Which row is not defined, here as single-node: only the keys are checked."""
    actual = _distributed(lambda s: many(s).dropDuplicates(["name"]), cluster)
    rows = actual.to_pylist()  # type: ignore[attr-defined]
    names = [row["name"] for row in rows]
    assert len(names) == len(set(names)) == 54  # 53 names and NULL
    every = {tuple(r.values()) for r in many(cluster).toArrow().to_pylist()}
    assert all(tuple(row.values()) in every for row in rows)
