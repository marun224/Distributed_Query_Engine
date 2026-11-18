"""Proof of life for distributed writes against the dev catalog (PLAN Phase 5, §3.4).

`local[2]` workers write January 2024 of `nyc.yellow_tripdata` (~3M rows) into tables in
`icefleet_it` -- the only namespace written; `nyc` and `amazon` are refused outright --
and every step is checked against icetl reading the same rows from the source:

    trips_jan          partitioned by an identity column, pickup_date:
                       append, append again, overwrite (VendorID 2 only), and a dynamic
                       overwrite of three days
    trips_jan_by_day   partitioned by day(tpep_pickup_datetime), a transform: append

after each step: per-day row counts and sums equal to icetl's over the source, the
snapshots the step made, one data file per partition value it wrote. Both tables are dropped at
the end (and before, if a previous run left them).

    .\\scripts\\dev.ps1 uv run python scripts/smoke_write.py
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
from pyiceberg.transforms import DayTransform

from icefleet.exchange.object_store import ObjectStoreExchange
from icefleet.session import ClusterSession

SOURCE = "nyc.yellow_tripdata"
NAMESPACE = "icefleet_it"
PROTECTED = ("nyc", "amazon")
JANUARY = (F.col("tpep_pickup_datetime") >= "2024-01-01") & (
    F.col("tpep_pickup_datetime") < "2024-02-01"
)
DAYS = ["2024-01-05", "2024-01-06", "2024-01-07"]


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
            "total_amount",
            F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        )
    )


def summary(df: DataFrame) -> list[dict[str, Any]]:
    """Rows, trips and revenue per day: a check of every row without moving 3M of them."""
    rows = (
        df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("n"),
            F.sum("total_amount").alias("revenue"),
            F.sum("PULocationID").alias("zones"),
            F.countDistinct("VendorID").alias("vendors"),
        )
        .toArrow()
        .to_pylist()
    )
    return sorted(rows, key=lambda r: str(r["pickup_date"]))


def same(a: list[dict[str, Any]], b: list[dict[str, Any]]) -> str | None:
    if len(a) != len(b):
        return f"{len(a)} days written, {len(b)} expected"
    for x, y in zip(a, b, strict=True):
        for key in x:
            u, v = x[key], y[key]
            if isinstance(u, float) and isinstance(v, float):
                if not math.isclose(u, v, rel_tol=1e-9):
                    return f"{key} differs on {x['pickup_date']}: {u} != {v}"
            elif u != v:
                return f"{key} differs on {x['pickup_date']}: {u} != {v}"
    return None


class Checker:
    def __init__(self, cluster: ClusterSession, oracle: Session) -> None:
        self.cluster, self.oracle = cluster, oracle
        self.catalog = oracle._registry.get(None)
        self.ok = True

    def table(self, name: str) -> Any:
        return self.catalog.load_table(name)

    def read(self, name: str) -> DataFrame:
        self.oracle._invalidate_source(self.oracle._resolver.parse(name))
        return self.oracle.table(name)

    def step(
        self, label: str, name: str, write: Any, expected: list[dict[str, Any]], days: int
    ) -> None:
        """Run `write`; check the table against `expected` and that the write made one
        file for each of the `days` partition values it wrote."""
        print(f"\n=== {label} ===")
        before = len(self.table(name).snapshots()) if self.catalog.table_exists(name) else 0
        started = time.perf_counter()
        write()
        took = time.perf_counter() - started
        run = self.cluster._last_write
        if run is None or run.mode != "distributed":
            print(f"FAIL: the write was not distributed: {run}")
            self.ok = False
            return
        table = self.table(name)
        made = [s.summary.operation.value for s in table.snapshots()[before:] if s.summary]
        files = [task.file for task in table.scan().plan_files()]
        partitions = {str(f.partition) for f in files}
        print(
            f"wrote {run.rows} rows in {run.files} files, {took:.1f}s "
            f"({run.stages} stages, tasks {run.stage_tasks}); snapshots {made}"
        )
        print(f"table now: {len(files)} data files, {len(partitions)} partition values")
        problem = same(summary(self.read(name)), expected)
        if problem:
            print(f"FAIL: {problem}")
            self.ok = False
            return
        if run.files != days:
            print(f"FAIL: {run.files} files written for {days} partition values")
            self.ok = False
            return
        print("OK: equal to icetl over the source")


def create(cluster: ClusterSession, checker: Checker, name: str, day_transform: bool) -> None:
    """The table, from the frame's own schema, partitioned the way the step needs."""
    schema = iceberg_ready(cluster._empty_result(trips(cluster))).schema
    with contextlib.suppress(Exception):
        checker.catalog.create_namespace_if_not_exists(NAMESPACE)
    table = checker.catalog.create_table(guarded(name), schema=schema)
    with table.update_spec() as spec:
        if day_transform:
            spec.add_field("tpep_pickup_datetime", DayTransform(), "pickup_day")
        else:
            spec.add_identity("pickup_date")


def drop(checker: Checker, name: str) -> None:
    """Drop the table and its files (a purge), if it is there."""
    if checker.catalog.table_exists(guarded(name)):
        checker.catalog.purge_table(name)


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--exchange", default="s3://icefleet-shuffle/smoke-write")
    args = parser.parse_args()

    settings = resolve_settings(IcetlConf())
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
    checker = Checker(cluster, oracle)
    by_date, by_day = f"{NAMESPACE}.trips_jan", f"{NAMESPACE}.trips_jan_by_day"
    try:
        for name in (by_date, by_day):
            drop(checker, name)
        source = summary(trips(oracle))
        print(f"source: {sum(r['n'] for r in source)} rows over {len(source)} days (icetl)")

        create(cluster, checker, by_date, day_transform=False)
        checker.step(
            "append", by_date, lambda: trips(cluster).write.insertInto(by_date), source, len(source)
        )
        doubled = [
            {**r, "n": r["n"] * 2, "revenue": r["revenue"] * 2, "zones": r["zones"] * 2}
            for r in source
        ]
        checker.step(
            "append again",
            by_date,
            lambda: trips(cluster).write.insertInto(by_date),
            doubled,
            len(source),
        )
        vendor2 = summary(trips(oracle).filter(F.col("VendorID") == 2))
        checker.step(
            "overwrite with VendorID 2 only",
            by_date,
            lambda: (
                trips(cluster)
                .filter(F.col("VendorID") == 2)
                .write.insertInto(by_date, overwrite=True)
            ),
            vendor2,
            len(vendor2),
        )
        everything = {str(r["pickup_date"]): r for r in vendor2}
        for row in summary(trips(oracle).filter(F.col("pickup_date").isin(*DAYS))):
            everything[str(row["pickup_date"])] = row
        checker.step(
            f"dynamic overwrite of {', '.join(DAYS)} with every vendor",
            by_date,
            lambda: (
                trips(cluster)
                .filter(F.col("pickup_date").isin(*DAYS))
                .write.mode("overwrite")
                .option("partitionOverwriteMode", "dynamic")
                .insertInto(by_date)
            ),
            [everything[k] for k in sorted(everything)],
            len(DAYS),
        )

        create(cluster, checker, by_day, day_transform=True)
        checker.step(
            "append into day(tpep_pickup_datetime)",
            by_day,
            lambda: trips(cluster).write.insertInto(by_day),
            source,
            len(source),
        )
        return 0 if checker.ok else 1
    finally:
        for name in (by_date, by_day):
            try:
                drop(checker, name)
            except Exception as exc:  # report, do not mask the outcome
                print(f"could not drop {name}: {exc}")
        cluster.stop()
        oracle.stop()


if __name__ == "__main__":
    sys.exit(main())
