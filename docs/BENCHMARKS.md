# Benchmarks

icetl single-node against icefleet on `local[3]` and on an Argo fleet (PLAN §6 Phase 6,
kickoff answer 10), over `nyc.yellow_tripdata` in the dev catalog (41,169,720 rows, 62
files; the 2024+ cases read 39 of them). Produced by [scripts/benchmark.py](../scripts/benchmark.py);
regenerate the tables with `--render`.

**Read the ratios as overhead, not scale-out.** Every engine here runs on the same
8-core, 16 GB Windows machine, inside Docker Desktop's 8 GB VM, reading the same MinIO.
icefleet pays for the exchange (every shuffled row written to MinIO and read back),
task dispatch and result files, and on one machine it gains no hardware in return. What
the numbers do show: which shapes carry the least overhead, how the fleet behaves under
pod limits, and what the shuffle partition count is worth.

## Setup

| engine | where | resources |
|---|---|---|
| `single` | icetl `Session` in the dev container | all 8 CPUs, DuckDB's default memory (no container limit) |
| `local[3]` | icefleet, 3 worker processes in the dev container | 8 CPUs shared; each worker DuckDB with a third of the threads and memory; 2 slots each |
| `fleet` | icefleet, Argo fleet-per-workflow on the kind cluster: 2 daemon workers + a coordinator pod | worker pods 2 Gi / 2 CPU (DuckDB 1 GiB, set by the worker from its limit), 2 slots; coordinator 2 Gi / 2 CPU |

Exchange in MinIO for both icefleet engines; `icefleet.shuffle.partitions` 64 (the
default when measured) unless the column says otherwise. Each case: 1 warm-up, then the median of 3 timed runs; every run's
row count must equal the warm-up's. 2026-11-26.

## Results

At the default of the time, 64 shuffle partitions (fleet runs: `icefleet-fleet-m2w2v`).

| case | single | local[3] | fleet |
|---|---:|---:|---:|
| count | 0.05 s | 0.05 s (1.0x) | 0.09 s (1.8x) |
| filtered | 0.18 s | 0.52 s (2.9x) | 1.07 s (5.9x) |
| stream | 1.31 s | 3.03 s (2.3x) | 17.17 s (13.1x) |
| aggregate | 0.46 s | 0.81 s (1.8x) | 1.45 s (3.2x) |
| join | 0.86 s | 11.58 s (13.5x) | 31.37 s (36.5x) |
| window | 3.05 s | 16.18 s (5.3x) | 46.47 s (15.2x) |

Median of 3 timed runs after 1 warm-up; in brackets, the ratio to `single` (above 1 =
slower). Every engine returned the same row count for every case. Cases:

- `count` (41,169,720 rows): `count(*)`, answered from manifests, opens no file (icetl `count`)
- `filtered` (2,964,617 rows): 2 columns of January 2024 as Arrow, file pruning (icetl `filtered`)
- `stream` (41,169,671 rows): 2 columns of every 2024+ trip, streamed in batches (icetl `stream`)
- `aggregate` (4 rows): revenue / trips / mean distance per VendorID over ~41M rows (icetl `aggregate`)
- `join` (263 rows): every trip joined to its zone's average fare, aggregated per zone; a shuffle join (icetl `join`)
- `window` (811 rows): the top 3 fares per pickup zone over ~41M rows, `rank()` + `QUALIFY`

## Shuffle partitions: 64 or 16

The same cases with `icefleet.shuffle.partitions=16` (fleet run `icefleet-fleet-bh9t6`):

| case | single | local[3] | local[3], 16 | fleet | fleet, 16 |
|---|---:|---:|---:|---:|---:|
| count | 0.05 s | 0.05 s | 0.05 s | 0.09 s | 0.06 s |
| filtered | 0.18 s | 0.52 s | 0.53 s | 1.07 s | 1.09 s |
| stream | 1.31 s | 3.03 s | 4.25 s | 17.17 s | 15.12 s |
| aggregate | 0.46 s | 0.81 s | 1.05 s | 1.45 s | 1.53 s |
| join | 0.86 s | 11.58 s | **8.01 s** | 31.37 s | **20.17 s** |
| window | 3.05 s | 16.18 s | 15.94 s | 46.47 s | **39.91 s** |

Only the join and the window shuffle wide (all ~41M rows); `filtered` and `stream` do not
shuffle at all, and `aggregate` shuffles a few partial rows, so their columns move with
run-to-run noise (stream: 3.03 s and 4.25 s on the same plan). Where rows are shuffled,
16 partitions win on both engines: the join by 31% on `local[3]` and 36% on the fleet,
the window by 14% on the fleet. Each partition is one task, one file per upstream task
and one S3 upload buffer per writing task; at 16 the fleet's 4 slots run 4 waves of
tasks instead of 16, and a shuffle task holds 16 uploads open instead of 64 (~80 MB
instead of ~320 MB of buffers, tracker Known gaps). **The default became 16 on
2026-11-26** (PLAN §6 Phase 6, "As built"); the offline suite sets its own (4).

## What it says

- **Scans, metadata and narrow aggregates carry little overhead**: `count` is
  answered from manifests on every engine; `aggregate` costs 1.8x on `local[3]`.
- **Wide shuffles are the cost** (the join and the window): every shuffled row is
  written to MinIO and read back. On one machine that is pure overhead; the streaming
  exchange that removes it is Phase 8 (decision 2).
- **The fleet is slower than `local[3]` on the same machine**, for reasons of setup, not
  design: its 2 workers have 2 CPUs and a 1 GiB DuckDB budget each (spilling where
  `local[3]` does not), against `local[3]`'s 8 shared CPUs; its pods reach MinIO through
  Docker Desktop's host gateway (`host.docker.internal`) rather than a bridge network; and
  `stream` brings ~41M rows back through result files to a 2-CPU coordinator pod.
- **Correctness is the other half**: the same queries are checked row for row against icetl
  by `scripts/fleet_check.py` (on the fleet: all equal, every exchange prefix removed, no
  pod or Service left).
