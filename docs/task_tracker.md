# Task Tracker

Current state of work. **Read this first when resuming.**
Update at the end of every session. Design lives in
[PLAN_distributed_engine.md](PLAN_distributed_engine.md); status lives here.

## At a glance

| | |
|---|---|
| Last updated | 2026-11-18 |
| Current phase | **Phase 4 — Windows, ordering, set operations, grouping sets: built, gate green, awaiting owner confirmation** (Phase 3 confirmed 2026-11-18) |
| Last updated (git) | 2026-11-16: Phase 1 committed (`fb937a7`) and pushed to the new **public** repo https://github.com/marun224/Distributed_Query_Engine (`origin`, `main` tracks `origin/main`). **Phase 2 committed on `main`** at the owner's request ("pls commit phase 2", 2026-11-16; `88fc616`). **Pushed:** checked 2026-11-18 with `git ls-remote`, GitHub's `main` is `88fc616`. **Phase 3 committed on `main`** at the owner's request ("pls commit pahse 3 changes", 2026-11-18; the commit after `88fc616`, see `git log`). **Not pushed.** **Phase 4 committed on `main`** at the owner's request ("pls commit phase 4 chnages to git", 2026-11-18; the commit after `0330d62`). **Not pushed** |
| Next action | **Owner to confirm Phase 4** (and say whether to push); then the Phase 5 kickoff questions |
| Next phase | Phase 5 — Distributed writes, one commit |
| Blocked on | Owner confirmation only |
| Gate | **`.\scripts\dev.ps1 uv run tox` green on 2026-11-18** (Phase 4): lint · type · py312 **755 passed** · dist, 219 s (the tests alone 3 min 20 s, over plan §9's 3-minute target -- Known gaps). Proof of life `scripts/smoke_cluster.py` on the dev catalog: **OK** for all eight queries (projection, aggregate, join broadcast / shuffled, window, rollup, union, range sort). The Windows `.venv` gate is not authoritative (decision 28) |

## Phases

| Phase | Scope (plan §6) | Status | Done on | Confirmed by owner |
|---|---|---|---|---|
| 0 | Scaffold: venv, uv, pyproject, tox gate, docs, smoke tests | **done** | 2026-11-15 | **yes, 2026-11-15** |
| 1 | Worker (HTTP), `local[N]`, splits, scan-only stages, fallback, proof of life on the dev catalog | **done** | 2026-11-15 | **yes, 2026-11-16** |
| 2 | Exchange (shuffle) + two-phase aggregation + explain DAG | **done** | 2026-11-16 | **yes, 2026-11-18** |
| 3 | Joins: broadcast and shuffle hash | **done** | 2026-11-18 | **yes, 2026-11-18** |
| 4 | Windows, distributed ORDER BY (range partitioning), OFFSET, set operations, grouping sets | **built — awaiting confirmation** | 2026-11-18 | pending |
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
| 2.12 | Owner confirms Phase 2 | **done** | 2026-11-18, "pls start pahse 3" (after the done-when checklist was shown); Phase 2 was already on GitHub (`88fc616`) |

## Phase 3 checklist

| # | Task | Status | Notes |
|---|---|---|---|
| 3.0 | Kickoff questions (7) answered | **done** | owner: "yes to all" (2026-11-18): (1) every equi-join kind, cross / non-equi by broadcast only, `IN (subquery)` as a semi join; (2) strategy chosen when planning, from upper bounds; (3) broadcast side written once, read whole; (4) co-partition reuse, joins and aggregates; (5) key type alignment; (6) independent stages in one wave; (7) proof of life broadcast + shuffled. Recorded in plan §6 Phase 3 "As built" |
| 3.1 | Spike: icetl's optimized trees for 18 join shapes (fixtures, read-only) | **done** | a join stays one flat `SELECT` with `joins` (an aggregate over a join is the same SELECT with `GROUP BY`); semi/anti arrive as DuckDB `SEMI` / `ANTI JOIN`; the DataFrame `on="id"` form projects `COALESCE(l.id, r.id)`; **a self-join has one `ScanPlan` and both references share one `$<view>_paths_0` parameter**, so each table *reference* must become its own stage input; `x IN (SELECT ...)` is left as a subquery (not unnested); `ScanPlan.bytes_scanned` is parquet bytes of the selected columns. The dev catalog now holds only `nyc.yellow_tripdata` (`yellow_tripdata_wide`, plan §3.4, is gone) |
| 3.2 | `plan/partitioning.py`: `Hash`, `partition_expression` (moved from `aggregates.py`, re-exported), `hash_type`, `join_hash_type` | **done** | plan §4.1 "As built, Phase 3" |
| 3.3 | `plan/rel.py`: a tree with `Join`; `IN (subquery)` -> `SEMI JOIN`; an aggregating join split into `Aggregate` over `Join`; join kinds; refusals (ASOF, USING left over, `*`, correlated / `NOT IN` / `EXISTS`) | **done** | Phase 2's chain tests unchanged and green |
| 3.4 | `plan/joins.py`: equality keys (incl. `IS NOT DISTINCT FROM`), `BROADCASTABLE`, NULL-supplied relations, filter push-down rules | **done** | plan §4.3 correction |
| 3.5 | `plan/stages.py`: pipeline planner; one `Stage` type with `StageInput`s (`partition` / `all`); broadcast; shuffle with co-partition reuse; aggregate in place; per-reference scans | **done** | `ScanStage` / `ShuffleStage` gone; `describe.py` renders any stage, `reads stage k (broadcast)` |
| 3.6 | Scheduler waves (`ThreadPoolScheduler.run`) | **done** | `py_test/unit/test_scheduler.py` |
| 3.7 | `icefleet.broadcast.thresholdBytes` (64 MiB, 0 = never) | **done** | `config.py` |
| 3.8 | **Phase 1 race found and fixed:** the worker released its slot after sending the answer; a slot handed its next task at once could be refused "all slots are busy" (seen once in the gate) | **done** | `worker/server.py` releases first; regression test fails against the old server |
| 3.9 | Tests: join suite `py_test/cluster/test_joins.py` (27 joins x broadcast / shuffled x `local[2]` / `local[3]`, plus broadcast-only, ordered, strategy and reuse checks); unit tests for keys, kinds, `IN` rewrite (NULL semantics checked in DuckDB), hash types (+ hypothesis), scheduler waves; Phase 1/2 tests that used a join as "single-node" moved to a window | **done** | 508 tests (+188) |
| 3.10 | `scripts/smoke_cluster.py`: `join-broadcast`, `join-shuffle` on `nyc.yellow_tripdata` | **done** | 6,485 rows equal to icetl both ways, 3 stages each, prefix removed; broadcast needs the threshold at 1 GiB (see Known gaps) |
| 3.11 | Docs: plan v1.5 (§4.1, §4.3, §4.4, §5, §6 "As built"), `divergence.md`, README | **done** | |
| 3.12 | Owner confirms Phase 3 | **done** | 2026-11-18, "pls commit pahse 3 changes. and start pahse 4 after that" |

## Phase 4 checklist

| # | Task | Status | Notes |
|---|---|---|---|
| 4.0 | Kickoff questions (8) answered | **done** | owner: "yes to all" (2026-11-18): (1) windows + QUALIFY, set operations, grouping sets / ROLLUP / CUBE with `grouping()`, OFFSET, ORDER BY / LIMIT in subqueries, aggregate over aggregate, DISTINCT ON; (2) range-partitioned sort when the size bound exceeds `icefleet.sort.coordinatorBytes` (64 MiB), coordinator sort below; (3) sort input written once with a per-task key sample (`icefleet.sort.sampleRows`, 1000), boundaries on the coordinator between stages; (4) windows: one shuffle by shared keys, else a chain; no PARTITION BY = one partition with a warning; reuse; (5) UNION ALL with no extra stage, distinct set ops hashed on all columns cast to the result type; (6) grouping sets in one shuffle with a hidden grouping id; raw path repeats rows per set; (7) top-n subqueries via one partition, ORDER BY lifted through projections / dropped under aggregates, joins, set ops; OFFSET at the coordinator; (8) icetl's window cases on Iceberg fixtures; proof of life: window + QUALIFY, ROLLUP, UNION ALL, range sort |
| 4.1 | Spike: icetl's optimized trees for 28 Phase 4 shapes (fixtures, read-only) | **done** | windows sit in the SELECT's projection (or `QUALIFY`), a window over an aggregate in the same SELECT as its `GROUP BY`; set operations are `exp.Union` / `Intersect` / `Except` with `distinct`, a DataFrame `union` is positional `UNION ALL`, a union under an aggregate is a subquery; `rollup` / `cube` / SQL `GROUPING SETS` stay in `GROUP BY` with `GROUPING()`; `dropDuplicates(subset)` is `DISTINCT ON`; `orderBy(...).select(...)` leaves the `ORDER BY` in a subquery under a projection, `orderBy(...).groupBy(...)` under an aggregate, `orderBy(...).limit(n).groupBy(...)` a top-n subquery; icetl's DataFrame has no `offset()`. icetl's window tests (`tests/fixture/test_window.py`) build their frames with `createDataFrame`, which icefleet runs single-node |
| 4.2 | `plan/rel.py`: `Window`, `DistinctOn`, `TopN`, `SetOp`; window over aggregate / join and DISTINCT over window split in two; a subquery ORDER BY moved up or dropped; OFFSET lifted; aggregate over aggregate allowed; `aggregate_calls` skips window functions | **done** | |
| 4.3 | `plan/aggregates.py`: grouping sets / ROLLUP / CUBE (icetl's `rollup()` / `cube()` form and the parsed one) in one shuffle with `__g`; `grouping()` from its bits; raw path repeats rows per set | **done** | the same set twice is refused (each would be its own group) |
| 4.4 | `plan/stages.py`: windows (shared keys / chain / one partition, reuse), DISTINCT ON, top-n, UNION ALL branches, distinct set operations, range-sort stages; `StagePlan.final_stages`, `.offset`, `.ranged` | **done** | |
| 4.5 | `plan/sort.py` + sinks + scheduler: key sample in the result sink, `task` input mode, boundaries filled in between stages (`Stage.resolved`, `ThreadPoolScheduler(fetch=...)`) | **done** | `icefleet.sort.coordinatorBytes` (64 MiB), `icefleet.sort.sampleRows` (1000) |
| 4.6 | **DISTINCT ON keeps its ORDER BY** -- found while writing the tests: lifting it to the coordinator would have kept an arbitrary row per key instead of the first | **done** | `test_windows.py::test_distinct_on_keeps_the_first_row_of_its_order` |
| 4.7 | Tests: `test_windows.py` (21 window shapes + icetl's 21 window cases on new fixtures `ties` / `gaps` / `groups`), `test_setops.py`, `test_ordering.py` (range sort incl. ties, OFFSET, top-n subqueries, grouping sets, aggregate over aggregate); unit: Phase 4 shapes, grouping sets in plain DuckDB through the real sink, the range partitioner (hypothesis + 8 types x directions x NULL placement), the sample sink | **done** | 755 tests (+247) |
| 4.8 | `smoke_cluster.py`: window, rollup, union, sorted on `nyc.yellow_tripdata` | **done** | all equal to icetl, the sort checked key by key; the four earlier queries rerun green |
| 4.9 | Docs: plan v1.6 (§4.2 grouping-set row, §4.4, §5, §6 "As built"), `divergence.md`, README | **done** | |
| 4.10 | Owner confirms Phase 4 | **pending** | |

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

- **Still single-node, by design (plan §6, Phases 1-4 "As built"):** a non-equi / cross
  join with no side under the broadcast threshold, a full join without an equality, join
  keys of types that do not hash alike (`VARCHAR = INTEGER`, `DATE = TIMESTAMP`), ASOF /
  POSITIONAL joins, `NOT IN`, `EXISTS` and correlated subqueries; `UNION BY NAME`, an
  ORDER BY / LIMIT on a set operation inside a subquery, a subquery ORDER BY on something
  it does not select (under projections only), DISTINCT ON with an aggregate, a window
  or a join, a window over grouping sets, the same grouping set twice, GROUP BY ALL;
  ENUM / BIT / VARINT / UNION result or shuffled types, scans pruned to no files, renamed
  columns across file groups (Phase 7), duplicate output names, rows the session holds
  itself (`createDataFrame`), UDF calls (Phase 8).
- **A window without PARTITION BY runs in one task** (decision 18), and `explain()` says
  so; so do a top-n subquery's last step and a global aggregate's final.
- **Many small tasks on small data:** a 64-partition shuffle of a few thousand rows makes
  ~63 tiny final tasks (the window proof of life: 3.8 s warm against icetl's 0.9 s).
  Adaptive coalescing is Phase 7; the default count is Phase 6's to measure.
- **A range sort can be chosen where it does not pay:** a join's size is unknown, so an
  ORDER BY without a LIMIT over a join always range-sorts, two more stages even for a
  small result. Measuring the input at run time is Phase 7's.
- **The offline suite takes 3 min 20 s** with the Phase 4 suites, over plan §9's 3-minute
  target; fewer cluster parametrisations for the newest suites is the cheap fix.
- **Join sizes are upper bounds, and loose for a self-join** (kickoff answer 2): icetl plans one
  `ScanPlan` per table for all its references, pruned by the union of their filters, so a
  small aggregate of the same table is bounded by the whole scan and is not broadcast at
  the default 64 MiB (the proof of life: 265 zones bounded at 330 MB; it raises the
  threshold to 1 GiB). An aggregate of a *large* input is never broadcast either, however
  few its groups. The fix is measuring the materialised side at run time (Phase 7).
- **A shuffle join skews on a hot key** (and all NULL keys of an outer join meet in one
  partition): every row of a key lands in one task. Skew handling is Phase 7.
- **icetl's optimizer sometimes does not run** on a DataFrame that refers to a column icetl
  renamed for a join (`amount_1` after `many.join(plain, "id")`): the "optimized" tree is
  then the raw one, still `JOIN ... USING`, and icefleet runs it single-node (correctly; it
  says "a JOIN ... USING the optimizer did not rewrite"). Seen 2026-11-18; icetl's to look at.
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
| REST catalog | `http://localhost:8182` from Windows, `http://iceberg-rest:8181` in the dev container (namespaces `nyc`, `temporal_poc`; table `nyc.yellow_tripdata` -- the only one in `nyc` as of 2026-11-18; `yellow_tripdata_wide` is gone). Compose stack `iceberg-nyc` has **no restart policy**: after a Docker Desktop restart, `docker start` postgres, minio, iceberg-rest (and pgadmin) |
| MinIO | `http://localhost:9100` from Windows, `http://minio:9000` in the dev container; path-style, `us-east-1`; credentials in `.env` |
| Other clusters | a `kind` cluster `temporal-poc` runs in Docker, publishing 2746 (Argo Server's port). It may be a quicker Phase 6 target than Docker Desktop's Kubernetes; the owner decides at Phase 6 |
| icetl checkout | `E:\workspace\Transformation_Engine_08172026` (uncommitted docs reorganisation in progress there) |
