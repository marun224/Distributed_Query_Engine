"""Proof of life against the dev catalog (PLAN Phase 1, §3.4).

`local[2]` workers, the exchange in MinIO, a filtered projection of
`nyc.yellow_tripdata` (41M rows, 62 files) -- compared row for row with icetl's own
answer, the query's exchange prefix checked gone afterwards.

    .\\scripts\\dev.ps1 uv run python scripts/smoke_cluster.py

Read-only on the catalog: it reads `nyc.yellow_tripdata` and writes nothing to any
namespace. It writes only under `s3://icefleet-shuffle/` (the bucket is created if
missing) and removes what it wrote.
"""

from __future__ import annotations

import argparse
import sys
import time

from icetl.conf import IcetlConf, resolve_settings
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F

from icefleet.exchange.object_store import ObjectStoreExchange
from icefleet.session import ClusterSession

TABLE = "nyc.yellow_tripdata"


def query(session: Session) -> DataFrame:
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--exchange", default="s3://icefleet-shuffle/smoke")
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
        query(cluster).explain()

        started = time.perf_counter()
        actual = query(cluster).toArrow()
        distributed = time.perf_counter() - started
        run = cluster._last_run
        if run is None or run.mode != "distributed":
            print(f"FAIL: the query did not distribute: {run}")
            return 1
        print(
            f"distributed: {actual.num_rows} rows in {distributed:.1f}s "
            f"({run.tasks} tasks, {run.files} result files) -- includes starting the workers"
        )

        started = time.perf_counter()
        actual = query(cluster).toArrow()
        print(f"distributed again, workers warm: {time.perf_counter() - started:.1f}s")

        started = time.perf_counter()
        expected = query(oracle).toArrow()
        print(
            f"icetl single-node: {expected.num_rows} rows in {time.perf_counter() - started:.1f}s"
        )

        if not actual.schema.equals(expected.schema):
            print(f"FAIL: schemas differ\n{actual.schema}\n---\n{expected.schema}")
            return 1
        keys = [(name, "ascending") for name in actual.column_names]
        if not actual.sort_by(keys).equals(expected.sort_by(keys)):
            print("FAIL: rows differ")
            return 1
        last = cluster._last_run
        assert last is not None and last.query_id is not None
        if exchange.exists(exchange.query_prefix(last.query_id)):
            print(f"FAIL: {exchange.query_prefix(last.query_id)} was left behind")
            return 1
        print("OK: identical to icetl, exchange prefix removed")
        return 0
    finally:
        cluster.stop()
        oracle.stop()


if __name__ == "__main__":
    sys.exit(main())
