# Task Tracker

Current state of work. **Read this first when resuming.**
Update at the end of every session. Design lives in
[PLAN_distributed_engine.md](PLAN_distributed_engine.md); status lives here.

## At a glance

| | |
|---|---|
| Last updated | 2026-11-16 |
| Current phase | **Phase 2 — Exchange (shuffle) and two-phase aggregation: built, gate green, awaiting owner confirmation** |
| Last updated (git) | 2026-11-16: Phase 1 committed (`fb937a7`) and pushed to the new **public** repo https://github.com/marun224/Distributed_Query_Engine (`origin`, `main` tracks `origin/main`). **Phase 2 committed on `main`** at the owner's request ("pls commit phase 2", 2026-11-16; the commit after `fb937a7`, see `git log`). **Not pushed** |
| Next action | **Owner to confirm Phase 2** and say whether to push it; then the Phase 3 kickoff questions |
| Next phase | Phase 3 — Joins: broadcast and shuffle hash |
| Blocked on | Owner confirmation only |
| Gate | **`.\scripts\dev.ps1 uv run tox` green on 2026-11-16** (Phase 2): lint · type · py312 **320 passed** · dist, 82 s. Proof of life `scripts/smoke_cluster.py` on the dev catalog: **OK** for both queries (projection, and the two-stage aggregate). The Windows `.venv` gate is not authoritative (decision 28) |

## Phases

| Phase | Scope (plan §6) | Status | Done on | Confirmed by owner |
|---|---|---|---|---|
| 0 | Scaffold: venv, uv, pyproject, tox gate, docs, smoke tests | **done** | 2026-11-15 | **yes, 2026-11-15** |
| 1 | Worker (HTTP), `local[N]`, splits, scan-only stages, fallback, proof of life on the dev catalog | **done** | 2026-11-15 | **yes, 2026-11-16** |
| 2 | Exchange (shuffle) + two-phase aggregation + explain DAG | **built — awaiting confirmation** | 2026-11-16 | pending |
| 3 | Joins: broadcast and shuffle hash | not started | — | — |
| 4 | Windows, distributed ORDER BY (range partitioning), OFFSET, set operations, grouping sets | not started | — | — |
| 5 | Distributed writes, one commit; 5b row-level operations | not started | — | — |
| 6 | Container image, Kubernetes manifests, Argo fleet-per-workflow, integration tier, benchmarks (decision 27) | not started | — | — |
| 6b | Argo as a scheduler backend: pod per task (`ArgoScheduler`, `icefleet-worker run-task`) | not started | — | — |
| 7 | Task retry (thread-pool backend), adaptive partitions, skew, renamed-column splits, `explain(analyze)` | not started | — | — |
| 8 | Streaming exchange (Arrow IPC over HTTP), result streaming, Python UDFs, optional coordinator service | not started | — | — |

## Phase 0 checklist

| # | Task | Status | Notes |
|---|---|---|---|
| 0.1 | Design plan written and decisions locked | **done** | `docs/PLAN_distributed_engine.md`, 28 decisions in §2 (v1.1 adds 26 HTTP control plane, 27 Kubernetes + Argo target; v1.2 adds 28 Linux dev container) |
| 0.2 | venv created, uv installed inside it | **done** | Python 3.12.10, uv 0.13.8; no global uv on this machine |
| 0.3 | `pyproject.toml`, `tox.ini`, `.gitignore`, `.python-version`, `.env.example`, `.env`, `README.md` | **done** | catalog on `localhost:8182` (the owner's note said 8181; only 8182 answers), MinIO `localhost:9100` |
| 0.4 | `CLAUDE.md`, workflow file, this tracker, `commands.md` | **done** | |
| 0.5 | Source skeleton: `icefleet.__version__`, `icefleet-worker --help` | **done** | argparse only; the server is Phase 1 |
| 0.6 | Smoke tests | **done** | version, icetl pin, Arrow IPC round trip, Flight availability (skips here), worker CLI |
| 0.7 | `uv sync --all-extras` resolves and installs | **done** | icetl 0.1.0, duckdb 1.5.6, pyarrow 26.0.1, pyiceberg 0.11.1, sqlglot 30.31.0, ipykernel 7.4.0 |
| 0.8 | `uv run tox` green (lint, type, py312, dist) | **done** | 58.8 s; `python -m <tool>` form everywhere (see decision 26) |
| 0.10 | Linux dev container (decision 28, [PLAN_linux_dev_environment.md](PLAN_linux_dev_environment.md)) | **done** | owner approved "yes to all"; `deploy/dev/`, `scripts/dev.ps1`, `scripts/wheelhouse.ps1`, `.gitattributes`, `tox.ini` `work_dir`; docs updated. Verified: clean `up` + `setup` in both modes, tox green, catalog read through icetl, Windows `.venv`/`.tox` untouched (plan §11) |
| 0.9 | Owner confirms Phase 0 | **done** | 2026-11-15, "go with your recommendation": Phase 0, decision 26 and decision 28 confirmed |

## Phase 1 checklist

| # | Task | Status | Notes |
|---|---|---|---|
| 1.0 | Kickoff questions (6) answered | **done** | owner: "yes to all" (2026-11-15); answers recorded in plan §6 Phase 1 "As built" |
| 1.1 | Phase 0 committed on `main` | **done** | `28554e2`; branch renamed `master` → `main`; repo-local git identity set (none was configured) |
| 1.2 | Worker: `serve` (stdlib HTTP `/ping` `/tasks` `/cancel`, slot semaphore), runner on icetl's `DuckDBEngine`, `result` sink | **done** | `worker/{cli,server,runner,sinks}.py`; `run-task` exits 2 ("Phase 6b"); `--exchange` dropped, `--port-file` / `--log-level` added |
| 1.3 | Wire format `TaskSpec` / `TaskResult` | **done** | `plan/tasks.py`; base64 `schema_ipc` (Phase 2 will use it) |
| 1.4 | Cluster: `local[N]` launcher, `static://`, `k8s://` (DNS, stubbed resolver test), handshake refusing a DuckDB/icefleet/protocol mismatch | **done** | `cluster/{launcher,discovery,client}.py`, `icefleet/_versions.py` |
| 1.5 | Scheduler protocol + `ThreadPoolScheduler` | **done** | first failure cancels the rest (`/cancel`), error names worker and task |
| 1.6 | Stage planner v1: `Project(Scan)` → one stage; everything else `Uncuttable` with the phase that will distribute it | **done** | `plan/{rel,stages}.py`; stage SQL = icetl's compiled SQL unchanged |
| 1.7 | Split planner | **done** | `plan/splits.py`; LPT into `max(ceil(bytes/target), workers×slots)` splits; hypothesis property test |
| 1.8 | Exchange: `LocalDirExchange`, `ObjectStoreExchange` (s3fs list/delete) | **done** | prefix deleted in a `finally`; `icefleet.exchange.keep` keeps it |
| 1.9 | `ClusterSession`: builder, `icefleet.master`, fallback (`single-node` / `error`), `explain()` section, `toArrowBatches` streaming | **done** | `session.py`; `config.py` with conf > env > .env precedence |
| 1.10 | Tests: oracle suite on `local[2]` and `local[3]`; explain ≥ 2 splits; GROUP BY falls back and says so; handshake refused; worker death fails cleanly; prefix gone; worker HTTP surface; `test_seams.py`; parquet type round-trip pinned | **done** | 178 tests, ~110 s offline |
| 1.11 | `scripts/smoke_cluster.py` on `nyc.yellow_tripdata` | **done** | 13 of 62 files after pruning, 4 splits, 6,485 rows identical to icetl, prefix removed; 9.4 s cold (incl. worker start), 1.0 s warm, icetl 2.7 s (one run each, not a benchmark) |
| 1.12 | Plan updated | **done** | v1.3: §4.6 correction (DuckDB writes field ids), §4.10 seams added, §6 Phase 1 "As built" |
| 1.13 | Owner confirms Phase 1 | **done** | 2026-11-16, "yes pls" (after `fb937a7` was pushed) |

## Phase 2 checklist

| # | Task | Status | Notes |
|---|---|---|---|
| 2.0 | Kickoff questions (6) answered | **done** | owner: "yes to all" (2026-11-16): (1) shapes in scope; (2) ORDER BY / LIMIT applied by the coordinator; (3) partition count, no tasks for empty partitions; (4) stage SQL built from the optimized tree, a new icetl seam; (5) casting parquet-unsafe partials such as `sum(INTEGER)` → `HUGEINT`; (6) proof of life by extending `smoke_cluster.py`. Recorded in plan §6 Phase 2 "As built" |
| 2.1 | Spikes: icetl's optimized aggregate trees; `COPY ... PARTITION_BY` + `RETURN_STATS`; `hash()` across types | **done** | `FILE_SIZE_BYTES` + `PARTITION_BY` not combinable; zero rows write nothing; `x/0` is `inf`; **`hash()` differs by integer width for negative values** (plan §4.1 correction) |
| 2.2 | `plan/rel.py`: chain recogniser `Project* → Aggregate? → Project* → Scan`, ORDER BY / LIMIT lifted, hidden order columns | **done** | aggregates found by sqlglot class *or* DuckDB's `duckdb_functions()` |
| 2.3 | **Phase 1 bug found and fixed:** `SELECT list(x) FROM t` would have been distributed as row-at-a-time (sqlglot parses icetl's `list` as scalar `exp.List`), one list per split | **done** | `test_rel.py::test_an_aggregate_sqlglot_does_not_know_is_still_an_aggregate` |
| 2.4 | `plan/aggregates.py`: decomposition table, partial/final rewrite, raw path, global (1 partition), DISTINCT as keys, HAVING, typed casts back, `partition_expression` with key alignment | **done** | `explain()` names the aggregate that forced the raw path |
| 2.5 | `plan/types.py`: lossless stand-ins (`HUGEINT` → `DECIMAL(38,0)`, `TIMESTAMP_S/_MS` → `TIMESTAMP`, `T[n]` → `T[]`), for shuffled columns and results | **done** | Phase 1 results of those types now distribute too |
| 2.6 | `shuffle` sink (`PARTITION_BY`, manifest per partition), `TaskSpec`/`OutputFile` fields, scheduler runs stages in order on upstream manifests | **done** | `worker/sinks.py`, `plan/tasks.py`, `scheduler/threadpool.py` |
| 2.7 | `plan/stages.py`: `ScanStage` → `ShuffleStage`; a task per partition with rows; typed empty input when none | **done** | stage SQL rendered via icetl `substitute_sources` + `build_source` (new seam, plan §4.10) |
| 2.8 | Coordinator: stand-in casts back, ORDER BY / LIMIT, hidden columns dropped; `explain()` stage DAG + `coordinator` line; `icefleet.shuffle.partitions` (64) | **done** | `QueryRun.stages`, `.stage_tasks` |
| 2.9 | Tests: aggregation suite on `local[2]`/`local[3]` (partial, raw, ordered, NULL keys, empty groups, streaming, empty-partition skipping); rewrite checked in plain DuckDB through the real sink; INTEGER/BIGINT co-partition + hypothesis property; stand-in round trips; shuffle sink; exchange layout §4.4; float tolerance in the oracle | **done** | 320 tests (+142), offline suite ~60 s |
| 2.10 | `scripts/smoke_cluster.py` headline aggregate on `nyc.yellow_tripdata` | **done** | 39 of 62 files, 4 splits, 64-way shuffle via MinIO (4 partitions with rows), 4 rows equal to icetl, prefix removed; 1.2 s cold, 1.0 s warm, icetl 0.6 s (one run, not a benchmark) |
| 2.11 | Docs: plan v1.4 (§4.1, §4.4 corrections, §4.10, §5, §6 "As built"), `docs/divergence.md` (new), README | **done** | |
| 2.12 | Owner confirms Phase 2 | **pending** | |

## Open decisions

- ~~Which dev Kubernetes cluster?~~ **Settled 2026-11-15: Docker Desktop's built-in
  Kubernetes** (owner agreed). Reasons: no new unsigned binaries for Smart App Control to
  block, images built with `docker build` are visible to pods without a registry, and the
  dev MinIO / REST catalog are reachable as `host.docker.internal`. Argo Workflows installs
  with `kubectl apply` of its release manifest; the `argo` CLI is optional because the plan
  submits through Argo's REST API. Still to do *before Phase 6*: enable Kubernetes in Docker
  Desktop settings, give Docker Desktop ~10 GB, install Argo. `kind` remains the drop-in
  for multi-node or CI runs; the manifests are plain Kubernetes either way.
- ~~Decision 26 needs the owner's eyes.~~ **Confirmed by the owner 2026-11-15** with Phase 0
  (HTTP/JSON control plane; Arrow IPC over HTTP for the Phase 8 data plane).
- ~~**git.**~~ **Settled 2026-11-16:** Phase 0 committed (`28554e2`, 2026-11-15) and Phase 1
  committed (`fb937a7`, 2026-11-16), both at the owner's request. Remote `origin` =
  https://github.com/marun224/Distributed_Query_Engine, created **public** by `gh repo create`
  at the owner's choice (name and visibility theirs). Before pushing, the files were checked
  for credentials: `.env` is gitignored and none of its secret values appear in any tracked
  file. Commits and pushes still happen only when the owner asks.
- ~~icetl upstream hooks.~~ **Settled 2026-11-15: after Phase 5**, once every override is
  known (plan §4.10 now lists six seams). The pin stays `icetl==0.1.0` until then.

## Deferred

| Item | Deferred to | Why |
|---|---|---|
| Merge-on-read reads | after icetl Phase 12 | icetl decision 11 |
| Helm chart | after `deploy/k8s` and `deploy/argo` settle | decision 27: plain manifests first |
| VMs without Kubernetes | unscheduled | decision 27; `static://` covers it anyway |
| Auth / TLS between processes | unscheduled | decision 20 |
| Web UI | unscheduled | decision 21 |
| Row-group-level splits | unscheduled | decision 6 |
| Non-identity partition transforms on write | Phase 5 follow-up | needs Iceberg transforms in SQL |
| Python UDFs on the cluster | Phase 8 | decision 9 |
| Streaming exchange (Arrow IPC over HTTP; Flight where it loads) | Phase 8 | decisions 2, 26 |

## Known gaps

No known wrong answers. Limits by design and environment issues:

- **Smart App Control blocks pyarrow on Windows**: `arrow_flight.dll` since Phase 0, and
  since a later re-check on 2026-11-15 also `_parquet` / `_dataset` (same files, verdict
  changed). `icetl.sql` cannot import there, so the Windows gate is red. **Resolved by
  decision 28:** development and the gate run in the Linux dev container, where everything
  imports, Flight included. The Flight smoke test passes there and skips only on Windows.
- **Docker Desktop: container downloads are intermittently unreliable** (2026-11-15). When it
  happens, small requests work but large downloads stall: 0.1–0.3 MB in 20 s, against
  ~14.5 MB/s on the host. Seen on bridge and host networks; containers have no proxy env, and
  MTU 1400 did not help (host path MTU ~1480–1491). A Docker Desktop restart cleared it
  briefly; within the hour one of three downloads stalled again. Root cause not found.
  **Handled:** `.\scripts\dev.ps1 setup` falls back to `scripts\wheelhouse.ps1` (exact
  locked Linux wheels downloaded on Windows, installed offline), and the dev image takes `uv`
  from an image pull, not PyPI. Phase 6's image build will meet the same problem; it needs
  the same wheelhouse approach or a fix.
- **Script execution policy:** Windows PowerShell's machine policy is undefined (scripts
  blocked by default in a plain terminal). README gives
  `powershell -ExecutionPolicy Bypass -File .\scripts\dev.ps1 ...`; changing the policy is
  the owner's call.

- **Phase 2 limits, by design (plan §6 Phase 2 "As built"):** one table per query; joins
  (Phase 3), windows, OFFSET, ORDER BY / LIMIT inside a subquery, grouping sets and an
  aggregate over an aggregate (Phase 4) run single-node. ORDER BY of a *large* result is
  sorted on the coordinator (bounded by `icefleet.result.maxBytes`); the range-partitioned
  sort is Phase 4. Still single-node from Phase 1: ENUM / BIT / VARINT / UNION result or
  shuffled types, scans pruned to no files, renamed columns across file groups, duplicate
  output names, UDF calls.
- **Shuffle files are not rolled** (DuckDB cannot combine `FILE_SIZE_BYTES` with
  `PARTITION_BY`): one file per task and partition, which is fine at 64 partitions and
  should be measured in Phase 6.
- **Partition count is fixed per query** (`icefleet.shuffle.partitions`, 64): a
  high-cardinality key in a small query still writes up to splits × 64 small files.
  Adaptive coalescing is Phase 7; the default is Phase 6's to measure.
- **Workers download DuckDB's `httpfs` on first object-store use** (`INSTALL httpfs`). In the
  dev container that is an internet download, subject to the flaky-download problem above; it
  worked on 2026-11-15. Phase 6 bakes it into the image (plan §3.5).
- **No task timeout by default** (`icefleet.task.timeoutSeconds` unset): a worker that hangs
  without dropping its connection hangs the query. A dead process is detected at once.
  Liveness checks are Phase 7.
- **Split sizing lists the manifests a second time** (one extra metadata pass on the
  coordinator; icetl does not expose per-file sizes). Fine at 62 files; an icetl hook is on
  the upstream list (plan §4.10).

## Environment facts (dev machine, 2026-11-15)

| | |
|---|---|
| OS | Windows 11 Pro, 8 logical cores, 15.8 GB RAM (per icetl's benchmark notes) |
| **Smart App Control** | **enforce mode** (`VerifiedAndReputablePolicyState = 1`). Blocks unsigned binaries without reputation: pyarrow `arrow_flight.dll`, `.venv\Scripts\mypy.exe`, and **since the later re-check also `_parquet` / `_dataset` .pyd** (verdicts can change over time). Allowed: `pytest.exe`, `ruff.exe` (signed), DuckDB, pyarrow core. Always run tools as `python -m <tool>`. |
| Python | **dev container: 3.12.16** (`python:3.12-slim` pinned by digest), venv `/opt/venv`, uv 0.13.8. Windows (editor only): 3.12.10 via `py -3.12`; no global `uv`, it lives in `.venv\Scripts\uv.exe` |
| Dev container | `icefleet-dev` (compose project `icefleet-dev`), volumes `icefleet-dev-venv`, `icefleet-dev-cache`; joins `iceberg-nyc_iceberg_net` |
| Docker | Docker Desktop 29.8.0, Compose v5.5.1 — running. **Its built-in Kubernetes is the dev cluster for Phase 6** (not yet enabled; Argo not yet installed) |
| REST catalog | `http://localhost:8182` from Windows, `http://iceberg-rest:8181` in the dev container (namespaces `nyc`, `temporal_poc`; table `nyc.yellow_tripdata`). Compose stack `iceberg-nyc` has **no restart policy**: after a Docker Desktop restart, `docker start` postgres, minio, iceberg-rest (and pgadmin) |
| MinIO | `http://localhost:9100` from Windows, `http://minio:9000` in the dev container; path-style, `us-east-1`; credentials in `.env` |
| Other clusters | a `kind` cluster `temporal-poc` runs in Docker, publishing 2746 (Argo Server's port). It may be a quicker Phase 6 target than Docker Desktop's Kubernetes; the owner decides at Phase 6 |
| icetl checkout | `E:\workspace\Transformation_Engine_08172026` (uncommitted docs reorganisation in progress there) |
