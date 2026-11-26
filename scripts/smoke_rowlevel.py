"""Proof of life for DELETE / UPDATE / MERGE against the dev catalog (PLAN Phase 5b, §3.4).

January 2024 of `nyc.yellow_tripdata` (~3M rows) is written twice into `icefleet_it` -- the
only namespace written; `nyc` and `amazon` are refused outright -- as twin tables
partitioned by an identity `pickup_date`. Each statement then runs on one twin through
`local[2]` workers and on the other through plain icetl, and the two tables must agree:

    DELETE   the trips over 50 miles: a few rows in many files, which the probe finds
    UPDATE   a dollar more tip on 2024-01-10's card trips: one partition's files
    MERGE    2024-01-20's fares times 1.1, by zone, from a source read off the table itself;
             a zone with trips on the 21st but none on the 20th is inserted for the 20th
             (its other columns typed NULLs on the cluster; icetl needs them typed by hand)

after each: per-day row counts and sums equal across the twins, the statement distributed
in one snapshot, how many files it replaced. Both tables are dropped at the end (and
before, if a previous run left them).

    .\\scripts\\dev.ps1 uv run python scripts/smoke_rowlevel.py
"""

from __future__ import annotations

import argparse
import contextlib
import math
import sys
import time
from typing import Any

from icetl.conf import IcetlConf, resolve_settings
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F
from icetl.sql.writer import iceberg_ready

from icefleet.exchange.object_store import ObjectStoreExchange
from icefleet.session import ClusterSession

SOURCE = "nyc.yellow_tripdata"
NAMESPACE = "icefleet_it"
PROTECTED = ("nyc", "amazon")
JANUARY = (F.col("tpep_pickup_datetime") >= "2024-01-01") & (
    F.col("tpep_pickup_datetime") < "2024-02-01"
)
MERGE = (
    "MERGE INTO {t} t USING ("
    "SELECT PULocationID AS zone, DATE '2024-01-20' AS d, 1.1 AS factor FROM {t} "
    "WHERE pickup_date IN (DATE '2024-01-20', DATE '2024-01-21') GROUP BY PULocationID) s "
    "ON t.PULocationID = s.zone AND t.pickup_date = s.d "
    "WHEN MATCHED THEN UPDATE SET total_amount = t.total_amount * s.factor "
    "WHEN NOT MATCHED THEN INSERT {insert}"
)
SHORT_INSERT = "(VendorID, PULocationID, pickup_date, total_amount) VALUES (99, s.zone, s.d, 0.0)"
TYPED_INSERT = (
    "(VendorID, PULocationID, pickup_date, total_amount, tpep_pickup_datetime, trip_distance, "
    "payment_type, tip_amount) VALUES (99, s.zone, s.d, 0.0, CAST(NULL AS TIMESTAMP_NTZ), "
    "CAST(NULL AS DOUBLE), CAST(NULL AS BIGINT), CAST(NULL AS DOUBLE))"
)
STATEMENTS = [
    ("delete the trips over 50 miles", "DELETE FROM {t} WHERE trip_distance > 50", None),
    (
        "update the card trips of 2024-01-10",
        "UPDATE {t} SET tip_amount = tip_amount + 1 "
        "WHERE pickup_date = DATE '2024-01-10' AND payment_type = 1",
        None,
    ),
    (
        "merge 2024-01-20's fares by zone, inserting the zones only the 21st has",
        MERGE.replace("{insert}", SHORT_INSERT),
        MERGE.replace("{insert}", TYPED_INSERT),
    ),
]


def guarded(name: str) -> str:
    namespace = name.split(".", 1)[0]
    if namespace in PROTECTED or namespace != NAMESPACE:
        raise SystemExit(f"refusing to write {name}: only {NAMESPACE}.* is written")
    return name


def trips(session: Session) -> DataFrame:
    return (
        session.table(SOURCE)
        .filter(JANUARY)
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "PULocationID",
            "trip_distance",
            "payment_type",
            "tip_amount",
            "total_amount",
            F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        )
    )


def summary(df: DataFrame) -> list[dict[str, Any]]:
    rows = (
        df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("n"),
            F.sum("total_amount").alias("revenue"),
            F.sum("tip_amount").alias("tips"),
            F.sum("PULocationID").alias("zones"),
            F.countDistinct("VendorID").alias("vendors"),
        )
        .toArrow()
        .to_pylist()
    )
    return sorted(rows, key=lambda r: str(r["pickup_date"]))


def same(a: list[dict[str, Any]], b: list[dict[str, Any]]) -> str | None:
    if len(a) != len(b):
        return f"{len(a)} days against {len(b)}"
    for x, y in zip(a, b, strict=True):
        for key in x:
            u, v = x[key], y[key]
            if isinstance(u, float) and isinstance(v, float):
                if not math.isclose(u, v, rel_tol=1e-9):
                    return f"{key} differs on {x['pickup_date']}: {u} != {v}"
            elif u != v:
                return f"{key} differs on {x['pickup_date']}: {u} != {v}"
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--exchange", default="s3://icefleet-shuffle/smoke-rowlevel")
    args = parser.parse_args()

    settings = resolve_settings(IcetlConf())
    print(f"catalog {settings.catalog.uri} · object store {settings.s3.endpoint}")
    ObjectStoreExchange(args.exchange, settings.s3).ensure_bucket()
    cluster = ClusterSession(
        settings=settings,
        conf=IcetlConf(
            {"icefleet.master": args.master, "icefleet.exchange.location": args.exchange}
        ),
    )
    oracle = Session(settings=settings)
    catalog = oracle._registry.get(None)
    ours, theirs = f"{NAMESPACE}.trips_rowlevel", f"{NAMESPACE}.trips_rowlevel_icetl"

    def drop(name: str) -> None:
        if catalog.table_exists(guarded(name)):
            catalog.purge_table(name)

    def read(name: str) -> DataFrame:
        oracle._invalidate_source(oracle._resolver.parse(name))
        return oracle.table(name)

    ok = True
    try:
        for name in (ours, theirs):
            drop(name)
        schema = iceberg_ready(cluster._empty_result(trips(cluster))).schema
        with contextlib.suppress(Exception):
            catalog.create_namespace_if_not_exists(NAMESPACE)
        for name in (ours, theirs):
            table = catalog.create_table(guarded(name), schema=schema)
            with table.update_spec() as spec:
                spec.add_identity("pickup_date")
            started = time.perf_counter()
            trips(cluster).write.insertInto(name)
            files = len(list(catalog.load_table(name).scan().plan_files()))
            print(f"filled {name}: {files} files, {time.perf_counter() - started:.1f}s")

        for label, statement, for_icetl in STATEMENTS:
            print(f"\n=== {label} ===")
            before = catalog.load_table(ours)
            files_before = len(list(before.scan().plan_files()))
            snapshots_before = len(before.snapshots())
            started = time.perf_counter()
            cluster.sql(statement.format(t=ours))
            took_ours = time.perf_counter() - started
            run = cluster._last_write
            started = time.perf_counter()
            oracle.sql((for_icetl or statement).format(t=theirs))
            took_theirs = time.perf_counter() - started
            after = catalog.load_table(ours)
            made = [s.summary for s in after.snapshots()[snapshots_before:] if s.summary]
            replaced = made[-1].get("deleted-data-files") if made else None
            print(
                f"cluster {took_ours:.1f}s ({getattr(run, 'mode', None)}, "
                f"{getattr(run, 'stages', 0)} stages, tasks {getattr(run, 'stage_tasks', ())}); "
                f"icetl {took_theirs:.1f}s"
            )
            print(
                f"snapshots {[s.operation.value for s in made]}; replaced {replaced} of "
                f"{files_before} files, wrote {getattr(run, 'files', 0)} "
                f"({getattr(run, 'rows', 0)} rows)"
            )
            if run is None or run.mode != "distributed" or len(made) != 1:
                print(f"FAIL: not one distributed snapshot: {run}, {len(made)} snapshot(s)")
                ok = False
                continue
            problem = same(summary(read(ours)), summary(read(theirs)))
            if problem:
                print(f"FAIL: {problem}")
                ok = False
                continue
            print("OK: equal to icetl's twin")
        return 0 if ok else 1
    finally:
        for name in (ours, theirs):
            try:
                drop(name)
            except Exception as exc:  # report, do not mask the outcome
                print(f"could not drop {name}: {exc}")
        cluster.stop()
        oracle.stop()


if __name__ == "__main__":
    sys.exit(main())
