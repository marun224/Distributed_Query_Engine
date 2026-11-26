"""Phase 6's end-to-end check: the 41M-row aggregate, window and join on a worker fleet,
each equal to icetl's own answer (PLAN §6 Phase 6, "Done when").

Run as the coordinator step of the Argo fleet template (model B):

    .\\scripts\\k8s.ps1 run fleet_check.py
    .\\scripts\\k8s.ps1 run fleet_check.py "script-args=--only window"

or anywhere a fleet is reachable (the master and exchange come from the environment, or
the flags):

    .\\scripts\\dev.ps1 uv run python scripts/fleet_check.py --master local[3]

    aggregate  revenue, trips and mean distance per VendorID over every 2024+ trip
               (~41M rows; the headline query): partial, a hash shuffle, final
    window     the top 3 fares per pickup zone over the same rows: rank() and QUALIFY,
               all ~41M rows hashed by zone through the exchange
    join       every trip joined to its zone's average fare (an aggregate of the same
               table), trips and trips above their zone's average per zone: a shuffle
               join under an aggregate (the zone side's size bound is the whole scan,
               so it is not broadcast at the default threshold -- tracker, Known gaps)

Each query's exchange prefix is checked gone. Read-only on the catalog; writes only under
the exchange location and removes it. Exit 0 only if every query matched.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Callable

from icetl.conf import IcetlConf, resolve_settings
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F

from icefleet.cluster.discovery import parse_master, resolve_endpoints
from icefleet.errors import ClusterError
from icefleet.exchange.object_store import ObjectStoreExchange
from icefleet.session import ClusterSession
from smoke_cluster import TABLE, aggregate, same_rows

SINCE_2024 = F.col("tpep_pickup_datetime") >= "2024-01-01"


def window(session: Session) -> DataFrame:
    return session.sql(
        f"SELECT PULocationID, tpep_pickup_datetime, total_amount FROM {TABLE} "
        "WHERE tpep_pickup_datetime >= '2024-01-01' "
        "QUALIFY rank() OVER (PARTITION BY PULocationID ORDER BY total_amount DESC) <= 3"
    )


def join(session: Session) -> DataFrame:
    zones = (
        session.table(TABLE)
        .filter(SINCE_2024)
        .groupBy("PULocationID")
        # Rounded, so a trip a hair from its zone's average is not decided by the
        # summation order (PLAN §4.7) -- the count must be exact.
        .agg(F.round(F.avg("total_amount"), 6).alias("zone_avg"))
    )
    trips = session.table(TABLE).filter(SINCE_2024).select("PULocationID", "total_amount")
    return (
        trips.join(zones, "PULocationID")
        .groupBy("PULocationID")
        .agg(
            F.count("*").alias("trips"),
            F.sum(F.when(F.col("total_amount") > F.col("zone_avg"), 1).otherwise(0)).alias(
                "above_avg"
            ),
        )
    )


#: name -> (query, distributed stages at least, what explain() must say)
QUERIES: dict[str, tuple[Callable[[Session], DataFrame], int, str]] = {
    "aggregate": (aggregate, 2, "partial:"),
    "window": (window, 2, "window: rank · partition by"),
    "join": (join, 3, "shuffle hash join"),
}


def wait_for_workers(master_text: str, expected: int, timeout: float = 60.0) -> list[str]:
    """For k8s://: wait until DNS lists every daemon worker (endpoints can trail the pods)."""
    master = parse_master(master_text)
    if master.kind != "k8s":
        return []
    deadline = time.monotonic() + timeout
    seen: list[str] = []
    while time.monotonic() < deadline:
        try:
            seen = resolve_endpoints(master)
        except ClusterError:
            seen = []
        if len(seen) >= expected:
            return seen
        time.sleep(1)
    raise SystemExit(f"fleet_check: DNS lists {len(seen)} of {expected} workers: {seen}")


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--master", default=os.environ.get("ICEFLEET_MASTER", "local[2]"))
    parser.add_argument(
        "--exchange",
        default=os.environ.get("ICEFLEET_EXCHANGE_LOCATION", "s3://icefleet-shuffle/fleet-check"),
    )
    parser.add_argument("--only", choices=sorted(QUERIES), action="append")
    args = parser.parse_args()

    expected = int(os.environ.get("ICEFLEET_EXPECT_WORKERS", "0") or 0)
    endpoints = wait_for_workers(args.master, expected) if expected else []
    settings = resolve_settings(IcetlConf())
    print(f"fleet_check: master {args.master} {endpoints or ''}")
    print(f"catalog {settings.catalog.uri} · object store {settings.s3.endpoint}")
    exchange = ObjectStoreExchange(args.exchange, settings.s3)
    exchange.ensure_bucket()

    cluster = ClusterSession(
        settings=settings,
        conf=IcetlConf(
            {"icefleet.master": args.master, "icefleet.exchange.location": args.exchange}
        ),
    )
    oracle = Session(settings=settings)
    summary: dict[str, object] = {"master": args.master, "run": os.environ.get("ICEFLEET_RUN_ID")}
    ok = True
    try:
        for name in args.only or list(QUERIES):
            query, stages, expect = QUERIES[name]
            print(f"\n=== {name} ===")
            plan = query(cluster)._explain_text(verbose=False).split("\n\n", 1)[0]
            print(plan)
            started = time.perf_counter()
            actual = query(cluster).toArrow()
            distributed = time.perf_counter() - started
            run = cluster._last_run
            started = time.perf_counter()
            expected_rows = query(oracle).toArrow()
            single = time.perf_counter() - started
            problems = []
            if expect not in plan:
                problems.append(f"explain() does not say {expect!r}")
            if run is None or run.mode != "distributed" or run.stages < stages:
                problems.append(f"expected >= {stages} distributed stages, got {run}")
            difference = same_rows(actual, expected_rows)
            if difference:
                problems.append(difference)
            if (
                run is not None
                and run.query_id
                and exchange.exists(exchange.query_prefix(run.query_id))
            ):
                problems.append(f"{exchange.query_prefix(run.query_id)} was left behind")
            summary[name] = {
                "rows": actual.num_rows,
                "distributed_s": round(distributed, 1),
                "icetl_s": round(single, 1),
                "stages": run.stages if run else None,
                "tasks": list(run.stage_tasks) if run else None,
                "ok": not problems,
            }
            print(
                f"distributed: {actual.num_rows} rows in {distributed:.1f}s "
                f"(stages {run.stages if run else '?'}, tasks {run.stage_tasks if run else '?'}); "
                f"icetl single-node: {expected_rows.num_rows} rows in {single:.1f}s"
            )
            if problems:
                ok = False
                print("FAIL: " + "; ".join(problems))
            else:
                print("OK: identical to icetl, exchange prefix removed")
    finally:
        cluster.stop()
        oracle.stop()
    print("\nfleet_check summary: " + json.dumps(summary))
    print("FLEET CHECK OK" if ok else "FLEET CHECK FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
