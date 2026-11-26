"""Benchmarks: icetl single-node vs icefleet on `local[N]` vs icefleet on the Argo fleet
(PLAN §6 Phase 6, kickoff answer 10).

The cases are icetl's own benchmark cases (`icetl.diagnostics.benchmark`: count,
filtered, stream, aggregate, join), rewritten for `nyc.yellow_tripdata` -- icetl's run
on a generated 200-column table -- plus the window and join shapes Phase 6's done-when
names. Each case: 1 warm-up and `--repeat` timed runs (3), median and best; every run's
answer (its row count) must equal the warm-up's, so a case that got fast by getting
wrong fails the run.

    .\\scripts\\dev.ps1 uv run python scripts/benchmark.py --engine single --json a.json
    .\\scripts\\dev.ps1 uv run python scripts/benchmark.py --master local[3] --json b.json
    .\\scripts\\k8s.ps1 run benchmark.py "script-args=--engine cluster --label fleet"
    .\\scripts\\dev.ps1 uv run python scripts/benchmark.py --render a.json b.json ...

`--conf key=value` sets an `icefleet.*` key for the run (and labels it), which is how a
default is measured before it is changed. Without `--json` the result is printed as one
`BENCHMARK_JSON {...}` line, which is how a run inside a pod reports. Read-only on the
catalog; the cluster engine writes only under its exchange location and removes it.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from icetl.conf import IcetlConf, resolve_settings
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fleet_check import join as zone_join
from fleet_check import wait_for_workers
from fleet_check import window as zone_window
from smoke_cluster import TABLE, aggregate

JANUARY = (F.col("tpep_pickup_datetime") >= "2024-01-01") & (
    F.col("tpep_pickup_datetime") < "2024-02-01"
)
SINCE_2024 = F.col("tpep_pickup_datetime") >= "2024-01-01"


@dataclass(frozen=True)
class Case:
    name: str
    note: str
    build: Callable[[Session], DataFrame]
    consume: str = "arrow"


def _consume(frame: DataFrame, how: str) -> int:
    if how == "count":
        return int(frame.count())
    if how == "stream":
        return sum(batch.num_rows for batch in frame.toArrowBatches())
    return int(frame.toArrow().num_rows)


CASES: tuple[Case, ...] = (
    Case(
        "count",
        "count(*) -- from manifests, opens no file (icetl `count`)",
        lambda s: s.table(TABLE),
        "count",
    ),
    Case(
        "filtered",
        "2 columns of January 2024, ~3M rows as Arrow -- file pruning (icetl `filtered`)",
        lambda s: s.table(TABLE).filter(JANUARY).select("PULocationID", "total_amount"),
    ),
    Case(
        "stream",
        "2 columns of every 2024+ trip, ~41M rows streamed in batches (icetl `stream`)",
        lambda s: s.table(TABLE).filter(SINCE_2024).select("PULocationID", "total_amount"),
        "stream",
    ),
    Case(
        "aggregate",
        "revenue / trips / mean distance per VendorID over ~41M rows (icetl `aggregate`)",
        aggregate,
    ),
    Case(
        "join",
        "trips x their zone's average fare, aggregated per zone: a shuffle join (icetl `join`)",
        zone_join,
    ),
    Case("window", "top 3 fares per pickup zone over ~41M rows: rank() + QUALIFY", zone_window),
)


def run(args: argparse.Namespace) -> dict[str, Any]:
    settings = resolve_settings(IcetlConf())
    conf = dict(pair.split("=", 1) for pair in args.conf)
    session: Session
    if args.engine == "single":
        session = Session(settings=settings)
        where = "icetl single-node"
    else:
        from icefleet.session import ClusterSession

        expected = int(os.environ.get("ICEFLEET_EXPECT_WORKERS", "0") or 0)
        if expected:
            wait_for_workers(args.master, expected)
        session = ClusterSession(
            settings=settings,
            conf=IcetlConf(
                {
                    "icefleet.master": args.master,
                    "icefleet.exchange.location": args.exchange,
                    **conf,
                }
            ),
        )
        where = f"icefleet {args.master}"
    label = args.label or ("single" if args.engine == "single" else args.master)
    if conf:
        label += (
            " (" + ", ".join(f"{k.removeprefix('icefleet.')}={v}" for k, v in conf.items()) + ")"
        )
    results: dict[str, Any] = {
        "label": label,
        "engine": where,
        "conf": conf,
        "machine": f"{platform.machine()}, {os.cpu_count()} cpus visible",
        "repeat": args.repeat,
        "cases": {},
    }
    try:
        for case in CASES:
            if args.only and case.name not in args.only:
                continue
            answer = _consume(case.build(session), case.consume)  # warm-up
            runs = []
            for attempt in range(args.repeat):
                started = time.perf_counter()
                got = _consume(case.build(session), case.consume)
                runs.append(time.perf_counter() - started)
                if got != answer:
                    raise SystemExit(
                        f"{case.name}: run {attempt + 1} returned {got}, warm-up {answer}"
                    )
            results["cases"][case.name] = {
                "note": case.note,
                "rows": answer,
                "median_s": round(statistics.median(runs), 2),
                "best_s": round(min(runs), 2),
                "runs_s": [round(r, 2) for r in runs],
            }
            print(
                f"{label:28s} {case.name:10s} median {statistics.median(runs):6.2f}s "
                f"best {min(runs):6.2f}s rows {answer}",
                flush=True,
            )
    finally:
        session.stop()
    return results


def render(paths: list[str]) -> str:
    runs = [json.loads(Path(p).read_text(encoding="utf-8")) for p in paths]
    baseline = next((r for r in runs if r["label"] == "single"), runs[0])
    names = [c for c in baseline["cases"]]
    lines = [
        "| case | " + " | ".join(r["label"] for r in runs) + " |",
        "|---|" + "---:|" * len(runs),
    ]
    for name in names:
        base = baseline["cases"][name]["median_s"]
        cells = []
        for r in runs:
            case = r["cases"].get(name)
            if case is None:
                cells.append("--")
                continue
            if case["rows"] != baseline["cases"][name]["rows"]:
                raise SystemExit(
                    f"{name}: {r['label']} returned {case['rows']} rows, "
                    f"{baseline['label']} {baseline['cases'][name]['rows']}"
                )
            ratio = case["median_s"] / base if base else float("nan")
            cells.append(
                f"{case['median_s']:.2f} s" + ("" if r is baseline else f" ({ratio:.1f}x)")
            )
        lines.append(f"| {name} | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append(
        "Median of 3 timed runs after 1 warm-up; in brackets, the ratio to "
        f"`{baseline['label']}` (above 1 = slower). Cases:"
    )
    lines.append("")
    for name in names:
        lines.append(
            f"- `{name}` ({baseline['cases'][name]['rows']:,} rows): "
            f"{baseline['cases'][name]['note']}"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--engine", choices=["single", "cluster"], default="cluster")
    parser.add_argument("--master", default=os.environ.get("ICEFLEET_MASTER", "local[3]"))
    parser.add_argument(
        "--exchange",
        default=os.environ.get("ICEFLEET_EXCHANGE_LOCATION", "s3://icefleet-shuffle/benchmark"),
    )
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--only", action="append", choices=[c.name for c in CASES])
    parser.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--label")
    parser.add_argument("--json", help="write the result here instead of printing it")
    parser.add_argument("--render", nargs="+", metavar="JSON", help="tabulate earlier results")
    parser.add_argument("--markdown", help="with --render: write the table here")
    args = parser.parse_args()

    if args.render:
        table = render(args.render)
        if args.markdown:
            Path(args.markdown).write_text(table, encoding="utf-8")
        print(table)
        return 0
    results = run(args)
    if args.json:
        Path(args.json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json).write_text(json.dumps(results, indent=2), encoding="utf-8")
    else:
        print("BENCHMARK_JSON " + json.dumps(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
