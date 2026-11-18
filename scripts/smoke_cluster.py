"""Proof of life against the dev catalog (PLAN Phases 1 to 4, §3.4).

`local[2]` workers, the exchange in MinIO, queries over `nyc.yellow_tripdata` (41M rows,
62 files), each compared with icetl's own answer:

    projection      a filtered projection: one stage (Phase 1)
    aggregate       the headline query -- revenue, trips and mean distance per VendorID --
                    as two stages: partial aggregates, a hash shuffle through MinIO, final
                    aggregates per partition (Phase 2)
    join-broadcast  long trips joined to their pickup zone's average fare in January
                    2024 (an aggregate of the same table): the zones are aggregated, then
                    broadcast to every task scanning the trips (Phase 3). The threshold is
                    raised to 1 GiB for it: icetl plans one scan for both references to
                    the table, pruned by the union of their filters, so the zone side's
                    size bound is that whole scan (~330 MB), not the ~265 rows it
                    aggregates to (kickoff answer 2: sizes are bounded when planning;
                    measuring them is Phase 7's)
    join-shuffle    the same join with icefleet.broadcast.thresholdBytes=0: the trips are
                    shuffled by zone, and the join runs in the zone aggregate's own final
                    stage, which is already partitioned by zone (Phase 3)
    window          the top 3 fares per pickup zone in January 2024: rank() and QUALIFY,
                    hashed by zone (Phase 4)
    rollup          revenue by ROLLUP (VendorID, payment_type): every grouping set in one
                    shuffle (Phase 4)
    union           trips per vendor in January and February 2024, a UNION ALL of the two
                    months aggregated: a partial per branch, one final (Phase 4)
    sorted          the long trips ordered by fare: over icefleet.sort.coordinatorBytes,
                    so sorted by range partitions on the workers, and the order checked key
                    by key against icetl's (Phase 4)

and each query's exchange prefix checked gone afterwards.

    .\\scripts\\dev.ps1 uv run python scripts/smoke_cluster.py [--only join-shuffle]

Read-only on the catalog: it reads `nyc.yellow_tripdata` and writes nothing to any
namespace. It writes only under `s3://icefleet-shuffle/` (the bucket is created if
missing) and removes what it wrote.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from collections.abc import Callable
from typing import Any

import pyarrow as pa
from icetl.conf import IcetlConf, resolve_settings
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F

from icefleet.exchange.object_store import ObjectStoreExchange
from icefleet.session import ClusterSession

TABLE = "nyc.yellow_tripdata"


def projection(session: Session) -> DataFrame:
    return (
        session.table(TABLE)
        .filter((F.col("trip_distance") > 50) & (F.col("total_amount") > 0))
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "PULocationID",
            "trip_distance",
            (F.col("total_amount") * 100).cast("bigint").alias("total_cents"),
        )
    )


def aggregate(session: Session) -> DataFrame:
    return (
        session.table(TABLE)
        .filter(F.col("tpep_pickup_datetime") >= "2024-01-01")
        .groupBy("VendorID")
        .agg(
            F.sum("total_amount").alias("revenue"),
            F.count("*").alias("trips"),
            F.avg("trip_distance").alias("mean_distance"),
        )
    )


def join(session: Session) -> DataFrame:
    zones = (
        session.table(TABLE)
        .filter(
            (F.col("tpep_pickup_datetime") >= "2024-01-01")
            & (F.col("tpep_pickup_datetime") < "2024-02-01")
        )
        .groupBy("PULocationID")
        .agg(F.avg("total_amount").alias("zone_avg_total"), F.count("*").alias("zone_trips"))
    )
    trips = (
        session.table(TABLE)
        .filter((F.col("trip_distance") > 50) & (F.col("total_amount") > 0))
        .select("VendorID", "tpep_pickup_datetime", "PULocationID", "trip_distance", "total_amount")
    )
    return trips.join(zones, "PULocationID")


JANUARY = "tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01'"
FEBRUARY = "tpep_pickup_datetime >= '2024-02-01' AND tpep_pickup_datetime < '2024-03-01'"


def window(session: Session) -> DataFrame:
    return session.sql(
        "SELECT PULocationID, tpep_pickup_datetime, total_amount FROM nyc.yellow_tripdata "
        f"WHERE {JANUARY} "
        "QUALIFY rank() OVER (PARTITION BY PULocationID ORDER BY total_amount DESC) <= 3"
    )


def rollup(session: Session) -> DataFrame:
    return session.sql(
        "SELECT VendorID, payment_type, grouping(VendorID, payment_type) AS level, "
        "count(*) AS trips, sum(total_amount) AS revenue FROM nyc.yellow_tripdata "
        "WHERE tpep_pickup_datetime >= '2024-01-01' GROUP BY ROLLUP (VendorID, payment_type)"
    )


def union(session: Session) -> DataFrame:
    return session.sql(
        "SELECT month, VendorID, count(*) AS trips, sum(total_amount) AS revenue FROM ("
        f"SELECT 'jan' AS month, VendorID, total_amount FROM nyc.yellow_tripdata WHERE {JANUARY} "
        "UNION ALL "
        f"SELECT 'feb', VendorID, total_amount FROM nyc.yellow_tripdata WHERE {FEBRUARY}"
        ") q GROUP BY month, VendorID"
    )


def ordered(session: Session) -> DataFrame:
    return projection(session).orderBy(F.col("total_cents").desc(), "tpep_pickup_datetime")


#: The queries whose order is part of the answer, and the columns that make it.
ORDER_KEYS: dict[str, tuple[str, ...]] = {"sorted": ("total_cents", "tpep_pickup_datetime")}

#: name -> (query, distributed stages expected, extra icefleet.* settings, explain must say)
QUERIES: dict[str, tuple[Callable[[Session], DataFrame], int, dict[str, str], str]] = {
    "projection": (projection, 1, {}, "-> result"),
    "aggregate": (aggregate, 2, {}, "partial:"),
    "join-broadcast": (
        join,
        3,
        {"icefleet.broadcast.thresholdBytes": "1GiB"},
        "broadcast hash join",
    ),
    "join-shuffle": (
        join,
        3,
        {"icefleet.broadcast.thresholdBytes": "0"},
        "the right side already partitioned by the keys",
    ),
    "window": (window, 2, {}, "window: rank · partition by"),
    "rollup": (rollup, 2, {}, "3 grouping sets, one shuffle"),
    "union": (union, 3, {}, "final: "),
    "sorted": (ordered, 3, {}, "range partition by"),
}


def _close(a: Any, b: Any) -> bool:
    """Exact, except doubles to 1e-9 relative (PLAN §4.7: summation order differs)."""
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)
    return bool(a == b)


def same_rows(actual: pa.Table, expected: pa.Table) -> str | None:
    if not actual.schema.equals(expected.schema):
        return f"schemas differ\n{actual.schema}\n---\n{expected.schema}"
    if actual.num_rows != expected.num_rows:
        return f"{actual.num_rows} rows distributed, {expected.num_rows} from icetl"
    keys = [(name, "ascending") for name in actual.column_names]
    for a, e in zip(
        actual.sort_by(keys).to_pylist(), expected.sort_by(keys).to_pylist(), strict=True
    ):
        if a.keys() != e.keys() or not all(_close(a[k], e[k]) for k in a):
            return f"rows differ: {a} != {e}"
    return None


def check(
    name: str,
    query: Callable[[Session], DataFrame],
    stages: int,
    settings: dict[str, str],
    expect: str,
    cluster: ClusterSession,
    oracle: Session,
    exchange: ObjectStoreExchange,
) -> bool:
    print(f"\n=== {name} ===")
    for key, value in settings.items():
        cluster.conf.set(key, value)
    try:
        return _check(name, query, stages, expect, cluster, oracle, exchange)
    finally:
        for key in settings:
            cluster.conf.unset(key)


def _check(
    name: str,
    query: Callable[[Session], DataFrame],
    stages: int,
    expect: str,
    cluster: ClusterSession,
    oracle: Session,
    exchange: ObjectStoreExchange,
) -> bool:
    plan = query(cluster)._explain_text(verbose=False)
    print(plan.split("\n\n", 1)[0])  # the distributed section; icetl's own follows
    if expect not in plan:
        print(f"FAIL: explain() does not say {expect!r}")
        return False

    started = time.perf_counter()
    actual = query(cluster).toArrow()
    distributed = time.perf_counter() - started
    run = cluster._last_run
    if run is None or run.mode != "distributed" or run.stages != stages:
        print(f"FAIL: expected {stages} distributed stage(s), got {run}")
        return False
    print(
        f"distributed: {actual.num_rows} rows in {distributed:.1f}s "
        f"({run.stages} stage(s), tasks per stage {run.stage_tasks}, {run.files} result files)"
    )

    started = time.perf_counter()
    actual = query(cluster).toArrow()
    print(f"distributed again, workers warm: {time.perf_counter() - started:.1f}s")

    started = time.perf_counter()
    expected = query(oracle).toArrow()
    print(f"icetl single-node: {expected.num_rows} rows in {time.perf_counter() - started:.1f}s")

    problem = same_rows(actual, expected)
    if problem:
        print(f"FAIL: {problem}")
        return False
    keys = ORDER_KEYS.get(name)
    if keys:
        # Ties may come in either order; the keys may not.
        got = [tuple(row[k] for k in keys) for row in actual.select(list(keys)).to_pylist()]
        want = [tuple(row[k] for k in keys) for row in expected.select(list(keys)).to_pylist()]
        if got != want:
            first = next(i for i, (a, b) in enumerate(zip(got, want, strict=True)) if a != b)
            print(f"FAIL: out of order at row {first}: {got[first]} != {want[first]}")
            return False
        print(f"ordered: {len(got)} rows in icetl's key order")
    last = cluster._last_run
    assert last is not None and last.query_id is not None
    if exchange.exists(exchange.query_prefix(last.query_id)):
        print(f"FAIL: {exchange.query_prefix(last.query_id)} was left behind")
        return False
    if name in ("aggregate", "rollup"):
        for row in actual.sort_by([(c, "ascending") for c in actual.column_names[:2]]).to_pylist():
            print(f"  {row}")
    print("OK: identical to icetl, exchange prefix removed")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--exchange", default="s3://icefleet-shuffle/smoke")
    parser.add_argument("--only", choices=sorted(QUERIES))
    args = parser.parse_args()

    settings = resolve_settings(IcetlConf())
    print(f"catalog {settings.catalog.uri} · object store {settings.s3.endpoint}")
    exchange = ObjectStoreExchange(args.exchange, settings.s3)
    if exchange.ensure_bucket():
        print(f"created bucket {exchange.bucket}")

    cluster = ClusterSession(
        settings=settings,
        conf=IcetlConf(
            {"icefleet.master": args.master, "icefleet.exchange.location": args.exchange}
        ),
    )
    oracle = Session(settings=settings)
    try:
        names = [args.only] if args.only else list(QUERIES)
        results = [check(n, *QUERIES[n], cluster, oracle, exchange) for n in names]
        return 0 if all(results) else 1
    finally:
        cluster.stop()
        oracle.stop()


if __name__ == "__main__":
    sys.exit(main())
