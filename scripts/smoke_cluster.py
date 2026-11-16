"""Proof of life against the dev catalog (PLAN Phases 1 and 2, §3.4).

`local[2]` workers, the exchange in MinIO, two queries over `nyc.yellow_tripdata` (41M
rows, 62 files), each compared with icetl's own answer:

    projection  a filtered projection: one stage (Phase 1)
    aggregate   the headline query -- revenue, trips and mean distance per VendorID --
                as two stages: partial aggregates, a hash shuffle through MinIO, final
                aggregates per partition (Phase 2)

and each query's exchange prefix checked gone afterwards.

    .\\scripts\\dev.ps1 uv run python scripts/smoke_cluster.py [--only aggregate]

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


QUERIES: dict[str, tuple[Callable[[Session], DataFrame], int]] = {
    "projection": (projection, 1),
    "aggregate": (aggregate, 2),
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
    cluster: ClusterSession,
    oracle: Session,
    exchange: ObjectStoreExchange,
) -> bool:
    print(f"\n=== {name} ===")
    query(cluster).explain()

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
    last = cluster._last_run
    assert last is not None and last.query_id is not None
    if exchange.exists(exchange.query_prefix(last.query_id)):
        print(f"FAIL: {exchange.query_prefix(last.query_id)} was left behind")
        return False
    if name == "aggregate":
        for row in actual.sort_by("VendorID").to_pylist():
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
