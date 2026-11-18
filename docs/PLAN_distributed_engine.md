# icefleet — a distributed SQL and DataFrame engine over Apache Iceberg, executed by a fleet of DuckDB workers

**Status:** Draft v1.5 — 2026-11-18, decisions locked (§2); v1.1 adds decision 26 (HTTP control plane) and decision 27 (Kubernetes + Argo Workflows as the deployment target); v1.2 adds decision 28 (development in a Linux dev container); v1.3 records Phase 1 as built (§6), corrects §4.6 (DuckDB writes field ids) and extends the seam table (§4.10); v1.4 records Phase 2 as built (§6), corrects §4.1 (DuckDB hashes integers by width) and §4.4 (shuffle files are not rolled), extends §4.10 and §5; v1.5 records Phase 3 as built (§6), corrects §4.3 (join strategy chosen when planning, from upper bounds) and §4.4 (broadcast), updates §5
**Owner:** Arun
**Base:** [`icetl`](https://github.com/marun224/data_transformation_engine) 0.1.0 — single-node, sqlglot IR, PyIceberg planning, DuckDB execution. Local checkout: `E:\workspace\Transformation_Engine_08172026`.
**State of the work:** [task_tracker.md](task_tracker.md). This file is the design; it does not say what is done.

---

## 1. What we are building

`icetl` runs one query on one machine: DuckDB reads the pruned file list PyIceberg planned,
and spills to disk when memory runs out. Past roughly 40–50 million rows that stops being
enough — a wide aggregation, a window over the whole table or a join of two large tables
needs more RAM than one box has, and spilling turns a slow query into a very slow one.

`icefleet` keeps every one of icetl's three pillars and adds one idea: **a query is cut
into stages at the points where rows must change hands, and each stage is ordinary DuckDB
SQL that many workers run at once, each over its own slice of the input.**

```python
from icefleet import ClusterSession
from icetl.sql import functions as F

session = (
    ClusterSession.builder.appName("etl")
    # in a cluster: "k8s://icefleet-workers.etl.svc:8815" (a headless Service, decision 27)
    # on a laptop:  "local[4]"
    .config("icefleet.master", "static://worker1:8815,worker2:8815,worker3:8815")
    .getOrCreate()
)

df = (
    session.table("nyc.yellow_tripdata")  # 41M rows, 62 files
    .filter(F.col("tpep_pickup_datetime") >= "2024-01-01")
    .groupBy("VendorID")
    .agg(F.sum("total_amount").alias("revenue"))
)
df.explain()  # stage 0: 62 files in 14 splits, partial sum  ->  shuffle(VendorID, 64)
# stage 1: 64 partitions, final sum            ->  result
df.write.mode("overwrite").saveAsTable("nyc.revenue_by_vendor")  # workers write, one commit
```

The user-facing API **is icetl's API**. `ClusterSession` is a subclass of `icetl.sql.Session`;
`DataFrame`, `Column`, `functions`, `Window`, `session.sql()` are inherited unchanged. The
only visible difference is the `icefleet.*` configuration and the stage DAG in `explain()`.

### The pillars, one level up

| Library | Role in icetl | Role added by icefleet |
|---|---|---|
| **sqlglot** | the single IR for both surfaces | still the IR — every *stage* is a sqlglot `Select` whose `FROM` is a placeholder substituted at task time, exactly as icetl substitutes table references today |
| **PyIceberg** | plans scans, commits writes | plans scans once on the coordinator; the file list is cut into **splits**; workers write data files and the coordinator commits **one snapshot** |
| **DuckDB** | executes everything | executes every stage on every worker, **and performs the shuffle itself** — `COPY ... PARTITION_BY` writes hash-partitioned parquet, `read_parquet` reads it back |
| **pyarrow** | result format | already a dependency; its **Arrow IPC** format is how rows cross a process boundary when they must (the Phase 8 data plane, over plain HTTP). Flight is an optional transport only where its DLL loads — decision 26 |

### Design principles

| # | Principle |
|---|---|
| **P1** | **icetl's semantics, unchanged.** Spark 3.5 remains the written reference; icetl's conformance pass runs on the coordinator *before* the tree is cut, so every stage inherits it. A distributed answer must equal the single-node answer — icetl is the test oracle. |
| **P2** | **Correct by co-location, fast by decomposition.** Any operator is correct once all the rows that must meet are on one worker (hash-partition by its keys). Pre-aggregation, broadcast and partial sorts are optimisations applied only where provably safe. |
| **P3** | **Degrade, never fail wrong.** A plan the stage planner cannot cut runs single-node on the coordinator through icetl itself, and `explain()` says so. Slower is a cost; a wrong row is a bug. |
| **P4** | **DuckDB moves the bytes.** Workers never serialise rows in Python. Scan, shuffle write, shuffle read and data-file write are all DuckDB or pyarrow streaming paths. |
| **P5** | **Stages are restartable.** A stage's output is written under its own prefix and only read after the whole stage completed, so a failed task can be rerun without touching anything else (Phase 7 uses this; Phase 1 designs for it). |
| **P6** | **Measured, not assumed.** Defaults (split size, partition count, broadcast threshold) are set by the Phase 6 benchmark, not by taste. |
| **P7** | **Resumable by a stranger.** Every phase ends with the tracker updated so the next session — days later, a new context — knows what is done, what is next and what is blocked. |

---

## 2. Decisions locked (2026-11-15)

Every question asked before planning, and the answer. Changing one is a new dated row, not
an edit.

| # | Decision | Consequence |
|---|---|---|
| 1 | **Own coordinator + workers**; no Ray, no Dask | The coordinator is *embedded in the client process* (Spark "client mode"). Workers are long-lived processes. A standalone coordinator service is Phase 8, optional. |
| 2 | **Shuffle through the object store first, a streaming exchange later** | DuckDB `COPY ... PARTITION_BY` to MinIO (or a local directory in `local[N]`); the streaming exchange (Arrow IPC over HTTP, decision 26) is Phase 8. |
| 3 | **New package depending on `icetl==0.1.0`** | This repo. icetl is pinned *exactly* because icefleet overrides private `Session` seams (§4.10); each upstream hook that replaces a private seam bumps the pin. |
| 4 | ~~**Docker Compose on one machine, then 2–5 VMs**~~; plus in-process `local[N]` — **superseded by decision 27** (same day) | What survives: `local[N]` spawns N worker subprocesses on 127.0.0.1 and is what the offline test suite uses; Docker Compose remains as the local smoke test of the container image. What changed: the deployment target is Kubernetes with Argo Workflows, not VMs. |
| 5 | **Shuffle files in a dedicated bucket/prefix, cleaned per query** | `icefleet.exchange.location`; a query's prefix is deleted on completion unless `icefleet.exchange.keep=true`. |
| 6 | **Design scale: 50M–500M rows per query, tables to a few TB** | No row-group-level splits, no cost-based join ordering in the first release. |
| 7 | **Workload order: aggregation → joins → windows → writes → row-level** | Phases 2, 3, 4, 5, 5b. |
| 8 | **Distributed writes in the first release** | Workers write Iceberg data files; the coordinator commits one snapshot with `add_files`. |
| 9 | **Python UDFs later** | Phase 8, shipped with cloudpickle. Until then a plan with a UDF falls back to single-node. |
| 10 | **Merge-on-read stays deferred** | Refused at scan time by icetl's own guard. |
| 11 | **Catalogs: whatever icetl's registry resolves** | REST first; Glue, Hive, SQL inherit. Only the coordinator talks to the catalog. |
| 12 | **Fallback to single-node on an uncuttable plan** | `icefleet.fallback = single-node \| error`; default `single-node`. `explain()` prints the reason. |
| 13 | **Fault tolerance v1: whole-query failure** | The caller reruns. Stage/task retry is Phase 7, which P5 makes possible. |
| 14 | **Spark 3.5 stays the semantic reference** | Via icetl's `apply_compat_semantics`; divergences recorded in this repo's `docs/divergence.md` only where *distribution* adds one (float summation order, nondeterministic aggregates). |
| 15 | **Same icetl API** | `ClusterSession(Session)`; nothing else is public. |
| 16 | **Results stream back through the coordinator as Arrow batches**, with a size guard | v1 routes the final stage through the exchange and the coordinator's DuckDB reads it; Phase 8 streams Arrow IPC over HTTP. Guard: `icefleet.result.maxBytes`, default 2 GiB. |
| 17 | **Global ORDER BY: range partition by sampling** | Phase 4. `ORDER BY ... LIMIT n` is local top-n then global top-n. |
| 18 | **Window with no PARTITION BY: one partition** | Documented as the slow case; a warning in `explain()`. |
| 19 | **One worker process per node, one DuckDB, N task slots** | `--slots` (default 2); DuckDB parallelises inside a task. `local[N]` divides RAM and threads between the N processes. |
| 20 | **Trusted network, no auth** | Credentials reach workers through their own `.env`/environment at start, never over the wire. TLS/token auth deferred. |
| 21 | **`explain()` prints the stage DAG; per-stage metrics logged; no web UI** | `explain(mode="analyze")` after a run shows measured rows/bytes/seconds per stage (Phase 7). |
| 22 | **Two test tiers, like icetl** | Offline: `local[N]` + PyIceberg `SqlCatalog` fixtures. Integration (`-m integration`): the owner's REST catalog + MinIO (§3.4), later the dev Kubernetes cluster with Argo (§3.5), and the 41M-row tables. |
| 23 | **Name: `icefleet`** | Free on PyPI (checked 2026-11-15). Import `icefleet`, worker binary `icefleet-worker`. |
| 24 | **Python 3.12 only** | Matches icetl's pin. |
| 25 | **uv, hatchling, ruff, mypy, tox, pytest** | Same gate as icetl; see [workflow_instructions_file.md](workflow_instructions_file.md). |
| 26 | **Control plane: HTTP/JSON on the Python standard library, not Arrow Flight** *(added 2026-11-15, after Phase 0 found the dev machine blocks Flight)* | Windows Smart App Control runs in enforce mode on the dev machine and refuses pyarrow's unsigned `arrow_flight.dll`; it has no per-file allow-list, only a permanent off switch, which is the owner's security setting and not this project's to flip. So workers serve a stdlib `ThreadingHTTPServer` (`GET /ping`, `POST /tasks`, `POST /cancel`) and the coordinator calls them with `urllib`: nothing new to install, debuggable with curl, loads everywhere. Phase 8's fast data plane becomes **Arrow IPC streams over HTTP**; Flight stays a drop-in alternative on machines where it imports. `python -m <tool>` is used throughout the gate for the same reason (the unsigned `mypy.exe` launcher is blocked, the module form is not). |
| 27 | **Kubernetes with Argo Workflows is the real deployment target** *(added 2026-11-15, at the owner's request; supersedes decision 4's VMs)* | One image and one task format serve three ways of running on Kubernetes (§3.5): **A** a standing worker fleet behind a headless Service, found by `k8s://` discovery; **B** a fleet per workflow — worker pods as Argo *daemon steps*, the coordinator as the next step — which is the **first target** because it needs nothing beyond that discovery; **C** a pod per task — an `ArgoScheduler` that turns one query into one Workflow — which is **Phase 6b**, a second scheduler backend beside the thread pool. The scheduler therefore becomes a protocol with two implementations rather than one class. Docker Compose stays only as the local smoke test of the image. Dev cluster: **Docker Desktop's built-in Kubernetes** (owner agreed 2026-11-15: no new unsigned binaries for Smart App Control to block, local images visible to pods without a registry, dev MinIO and catalog reachable as `host.docker.internal`), with Argo Workflows installed from its release manifests; `kind` is the drop-in for multi-node or CI since the manifests are plain Kubernetes. Helm is deferred; plain manifests first. |
| 28 | **Development runs in a Linux dev container** *(added 2026-11-15, after Smart App Control began blocking pyarrow's Parquet and Dataset modules on the dev machine, which stops `icetl.sql` importing; owner approved [PLAN_linux_dev_environment.md](PLAN_linux_dev_environment.md))* | `deploy/dev/` defines it and `scripts\dev.ps1` drives it from Windows. The repo is mounted at `/work`; the venv (`/opt/venv`), uv cache and tox envs are container-local volumes. The gate is authoritative only there; the Windows `.venv` is editor-only. The container joins the dev catalog's Docker network (`iceberg-rest:8181`, `minio:9000`). Because container downloads are intermittently unreliable on this machine, `setup` falls back to a wheelhouse of the exact locked Linux wheels downloaded on Windows (`scripts\wheelhouse.ps1`). The Phase 6 runtime image (`deploy/Dockerfile`) shares the base image but is a separate file. Decision 26 stands: the design must not depend on where the coordinator runs. |

---

## 3. Architecture

```
 CLIENT PROCESS  (= coordinator)                                WORKER (one per node, long-lived)
┌────────────────────────────────────────────────┐           ┌─────────────────────────────────────┐
│ icetl surface: Session.sql / DataFrame / F.*   │           │ icefleet-worker  (HTTP/JSON server) │
│                   │ one sqlglot tree           │           │   actions: ping · run_task · cancel │
│ icetl: conformance → bind → optimize → pushdown│           │   slots: N concurrent tasks         │
│                   │ optimized tree + scan reqs │           │                                     │
│ icetl: plan_scan() per table  (PyIceberg)      │           │   icetl DuckDBEngine                │
│                   │ pruned file lists          │  HTTP     │     ├ httpfs + S3 secret            │
│ icefleet.plan: rel tree → required partitioning│ ───────▶  │     ├ read_parquet(split | shuffle) │
│               → exchanges → STAGE DAG          │ TaskSpec  │     ├ COPY ... PARTITION_BY (shuffle)│
│ icefleet.plan.splits: files → splits (bin-pack)│ ◀───────  │     └ parquet writer w/ field ids   │
│ icefleet.scheduler: stage at a time, slots     │ TaskResult│                                     │
│ icefleet.exchange: locate / list / clean       │           └──────────────┬──────────────────────┘
│ result: coordinator DuckDB reads final stage   │                          │ parquet
│ write:  table.add_files(paths) — ONE commit    │                          ▼
└───────────────┬────────────────────────────────┘           ┌─────────────────────────────────────┐
                │ catalog (REST/Glue/Hive/SQL)                │ OBJECT STORE (MinIO/S3)             │
                ▼                                             │  warehouse/…/data/*.parquet         │
        ┌───────────────┐                                     │  icefleet-shuffle/<query>/<stage>/  │
        │ Iceberg cat.  │                                     │     task-<i>/__part=<p>/*.parquet   │
        └───────────────┘                                     └─────────────────────────────────────┘
```

### 3.1 Components

| Component | Lives in | Does |
|---|---|---|
| **ClusterSession** | `icefleet/session.py` | `icetl.sql.Session` subclass. Overrides execution (`_execute`, `_stream`) and, from Phase 5, the writer. Everything else inherited. |
| **Stage planner** | `icefleet/plan/` | Turns icetl's *optimized* sqlglot tree into a DAG of stages separated by exchanges. Each stage is a sqlglot `Select` over placeholders. |
| **Split planner** | `icefleet/plan/splits.py` | Bin-packs the files of one scan into splits by selected-column bytes (from manifests). |
| **Scheduler** | `icefleet/scheduler/` | Runs stages in topological order, one stage fully before its dependents (blocking shuffle). A protocol with two backends: `ThreadPoolScheduler` dispatches tasks to the slots of running workers over HTTP; `ArgoScheduler` (Phase 6b) turns the stage DAG into one Argo Workflow with a pod per task. Both consume the same `StagePlan` and produce the same manifests. |
| **Exchange** | `icefleet/exchange/` | Where stage output lives and how it is addressed: `LocalDirExchange` (`local[N]`, tests), `ObjectStoreExchange` (S3/MinIO), later `StreamExchange` (Arrow IPC over HTTP). |
| **Worker** | `icefleet/worker/` | stdlib HTTP server + task runner over icetl's `DuckDBEngine`. Three sinks: shuffle, result, Iceberg data files. `icefleet-worker serve` is the long-lived form; `icefleet-worker run-task` (Phase 6b) runs one task and exits, which is what an Argo pod does. |
| **Cluster** | `icefleet/cluster/` | Worker discovery (`static://`, `local[N]` launcher, `k8s://` headless-Service DNS), handshake (version check), health. |
| **Config** | `icefleet/config.py` | `icefleet.*` keys on top of icetl's conf layers. |

### 3.2 Life of a query

1. `df.toArrow()` → icetl builds the sqlglot tree (both surfaces, one tree).
2. icetl: `apply_compat_semantics` → `bind` → `optimize_plan` → `extract_scan_requests` → `plan_scan()` per table. **Unchanged, reused.**
3. icefleet: `plan_stages(optimized_tree, scans)` → `StagePlan` or `Uncuttable(reason)`. Uncuttable → icetl single-node (`super()._execute`).
4. icefleet: scan stages get splits; each stage gets a `partitions` count (splits, or `icefleet.shuffle.partitions`).
5. Scheduler: for each stage in order, build one `TaskSpec` per partition, dispatch to slots, collect `TaskResult`s (output manifests). Children see their parents' manifests as explicit file lists — never a glob.
6. Final stage sink = `result`: the coordinator's own DuckDB `read_parquet`s the manifest and hands back Arrow (or batches). Sink = `iceberg_files`: the coordinator commits once.
7. Exchange prefix for the query deleted (unless `keep`).

### 3.3 Task specification (the wire format)

JSON over HTTP — `POST /tasks` blocks until the task ends and returns the `TaskResult`; `GET /ping`; `POST /cancel` — Python standard library on both ends (decision 26). Deliberately boring.

```
TaskSpec
  query_id, stage_id, task_index, attempt
  sql            DuckDB dialect; FROM clauses reference $placeholders, as icetl does today
  inputs         {placeholder: {"paths": [...], "schema_ipc": <bytes, used when paths is empty>}}
  sink           {"kind": "shuffle", "location": ..., "partitions": N, "keys": [sql exprs]}
               | {"kind": "result",  "location": ...}
               | {"kind": "iceberg_files", "location": ..., "schema_ipc": ..., "partition_cols": [...], "target_file_bytes": ...}
TaskResult
  rows_in, rows_out, bytes_out, seconds, duckdb_peak_memory
  manifest       shuffle: {partition: [{"path", "bytes", "rows"}]}
                 result:  [{"path", "bytes", "rows"}]
                 iceberg_files: [{"path", "bytes", "rows", "partition": {...}}]
```

Workers are stateless between tasks. S3 credentials and the DuckDB memory/thread/temp settings come from the worker's own environment at start (decision 20).

### 3.4 The development catalog

The owner runs an Iceberg REST catalog and MinIO locally (shared with icetl's integration
suite). It is the proof-of-life target from Phase 1 and the integration tier from Phase 6.

| Item | Value |
|---|---|
| REST catalog URI | `http://localhost:8182` |
| Warehouse | `s3://warehouse/` |
| S3 endpoint (MinIO) | `http://localhost:9100`, path-style, region `us-east-1` |
| Credentials | in `.env` (gitignored); `.env.example` names the keys |
| Table | `nyc.yellow_tripdata` — 41M rows, 62 files; `nyc.yellow_tripdata_wide` — 219 columns, 357 files |
| Shuffle location | `s3://icefleet-shuffle/` (bucket created by Phase 1's proof-of-life script if missing) |
| Protected namespaces | `nyc`, `amazon` — never written to; the integration tier writes only in `icefleet_it` |

### 3.5 Deployment: Kubernetes and Argo Workflows (decision 27)

**One image, every role.** `deploy/Dockerfile` builds a single image whose entry point is
`icefleet-worker`. `serve` is a long-lived worker; `run-task <spec-uri>` executes one task
and exits (Phase 6b); and the same image running the owner's ETL script is the coordinator.
DuckDB's `httpfs` extension is installed at build time, so no pod ever reaches for the
extension repository, and the Python environment is the locked `uv.lock`, so every pod runs
the DuckDB version the handshake expects.

**Three ways to run, one task format:**

| | **A. Standing fleet** | **B. Fleet per workflow** *(first target)* | **C. Pod per task** *(Phase 6b)* |
|---|---|---|---|
| Workers | a `StatefulSet` of `icefleet-worker serve` pods behind a headless `Service` | N `icefleet-worker serve` pods as Argo **daemon steps** (`daemon: true`), alive for the rest of the workflow | one short-lived pod per task, `icefleet-worker run-task` |
| Coordinator | any process that can reach the Service: a notebook via port-forward, or an Argo step | the workflow's next step, running the ETL script | the workflow's first step; it *generates* the rest of the Workflow and waits |
| Discovery | `k8s://<service>.<ns>.svc:8815` | the same — the daemon pods carry the label the headless Service selects | none; Argo schedules the pods |
| Scheduler backend | `ThreadPoolScheduler` | `ThreadPoolScheduler` | `ArgoScheduler` |
| Starts / stops | `kubectl apply`; idle between jobs | Argo starts them with the workflow and kills the daemons when it ends | Argo, per task; `ttlStrategy` removes finished pods |
| Best for | interactive work, many small jobs | scheduled pipelines, zero idle cost | the largest stages; elastic to the cluster's size |

**Discovery without a Kubernetes client.** `k8s://` resolves the headless Service name with
`socket.getaddrinfo`; Kubernetes DNS returns one A record per *ready* pod, so the coordinator
learns the fleet with a lookup and no RBAC. It re-resolves between stages, so a pod that
appeared or vanished is noticed at the next stage boundary. `static://` stays for everything
that is not Kubernetes; `local[N]` for a laptop.

**Memory is the new failure mode.** On one machine DuckDB's `memory_limit` plus spill meant
a query either finished or raised. In a pod, exceeding the container's memory limit is an
OOMKill with no message. So: the pod limit is DuckDB's `memory_limit` plus headroom (20% and
at least 1 GiB, enforced by the manifests' templates and checked at worker start, which
refuses a `memory_limit` within 15% of the cgroup limit); spill goes to an `emptyDir`
volume; `--slots × --threads` stays within the CPU request. `explain(mode="analyze")`
(Phase 7) reports DuckDB's peak memory per task, which is how the limit is tuned.

**Credentials** reach workers as environment variables from a `Secret` (decision 20
unchanged). Only the coordinator step gets the catalog credentials. From inside Docker
Desktop's Kubernetes the dev MinIO and REST catalog are `host.docker.internal:9100` and
`:8182`, not `localhost`, so the in-cluster Secret differs from `.env` in the host name only.

**Results and commits** do not change: the coordinator step reads the final stage from the
object store and returns Arrow to the script, or commits one snapshot to Iceberg. For a
notebook, model A with a port-forward is the interactive path.

**Model C in detail.** The `ArgoScheduler` writes every `TaskSpec` to
`<exchange>/<query_id>/specs/stage-<k>/task-<i>.json` (file lists are too large for workflow
parameters), then submits one `Workflow` whose DAG has a node per stage, each a template with
`withItems` over the task indices and a `parallelism` cap; a pod runs
`icefleet-worker run-task <spec-uri>` and writes its `TaskResult` beside the spec. Submission
goes through the Argo Server REST API with a bearer token from the environment (stdlib
HTTP, consistent with decision 26); the coordinator polls the Workflow status and reads the
results when a stage node completes. Argo's `retryStrategy` on the task template is Phase 7's
task retry for free, and its archive is the per-task log. The cost is pod start latency —
image pull, scheduling, DuckDB start — of seconds per task, which is fine for a 64-partition
stage over 41M rows and wrong for a two-second query; the small-query heuristic in §8 and the
single-node fallback (P3) carry the small cases, and `icefleet.master` picks the backend per
session, so a standing fleet (model A) and the Argo backend can coexist on one cluster.

---

## 4. The hard parts

### 4.1 Cutting a sqlglot tree into stages

sqlglot's tree is a *SQL* tree, not relational algebra (icetl PLAN.md §2.1 said so and kept
scan metadata in a side table for that reason). Distribution needs structure, so icefleet
adds a **thin relational layer that wraps sqlglot nodes and never re-implements
expressions**:

```
RelNode  = Scan(source, scan_plan)                     # an Iceberg table reference
         | Project(child, select_exprs, where)         # anything DuckDB does row-at-a-time: filter, map, functions
         | Aggregate(child, keys, aggs, having)
         | Join(left, right, kind, condition)
         | Window(child, partition_keys, order, funcs)
         | Sort(child, order, limit)                   # ORDER BY, LIMIT, ORDER BY ... LIMIT
         | Distinct(child)
         | SetOp(children, kind, distinct)
         | Exchange(child, Hash(keys, n) | Single | Broadcast | Range(order, n))   # inserted by the planner
```

`from_sqlglot(select)` recognises these shapes in the *optimized* tree (after icetl's
`merge_subqueries`, five nested `SELECT * FROM (...)` are already one `Select`). Anything it
does not recognise — a correlated subquery, a lateral, a UDF call before Phase 8, a
`QUALIFY`, a recursive CTE — yields `Uncuttable(reason)` and the plan runs single-node (P3).

**Required partitioning, bottom-up:**

| Node | Requires its input partitioned by | Provides |
|---|---|---|
| `Scan` | — | *splits* (arbitrary) |
| `Project` | whatever the child has | same as child |
| `Aggregate(keys)` | `Hash(keys)`; `Single` if no keys | `Hash(keys)` |
| `Join(equi keys)` | both sides `Hash(keys)` with the same `n` and the same hash expression, **or** small side `Broadcast` | `Hash(keys)` of the big/probe side |
| `Window(partition_keys)` | `Hash(partition_keys)`; `Single` if none (decision 18) | same |
| `Sort(order)` without `LIMIT` | `Range(order)` by sampling (decision 17) | ordered partitions, read in order |
| `Sort(order, LIMIT n)` | local top-n per partition, then `Single` | one partition |
| `Limit n` without order | first partitions until n rows | — |
| `Distinct` | `Hash(all columns)` | same |
| `SetOp UNION ALL` | nothing — concatenate manifests | union of children's partitions |
| `SetOp` distinct / `INTERSECT` / `EXCEPT` | `Hash(all columns)` on every child | same |

Where the required partitioning differs from the provided one, an `Exchange` is inserted.
**Cutting** = every `Exchange` is a stage boundary: the subtree below it becomes a stage
with a `shuffle` sink; the node above reads a placeholder. `to_sqlglot(stage)` regenerates
one `Select` per stage, built from the very sqlglot nodes the tree was parsed from —
expressions are never re-rendered from a second representation.

**Partition identity.** Two stages are co-partitioned only when their `Hash` carries the
same `n` and the same *typed* key expressions. The planner casts join and group keys to a
common type on both sides before hashing (an `INTEGER` and a `BIGINT` holding `1` are not
guaranteed to hash alike), and every worker must run the same DuckDB version (the `ping`
handshake refuses a mismatch) because `hash()` stability across versions is not promised.

> **Correction, 2026-11-16 (Phase 2, measured on DuckDB 1.5.6):** the cast is not optional
> even for integers. `hash(1::INTEGER) = hash(1::BIGINT)`, but `hash((-1)::INTEGER) ≠
> hash((-1)::BIGINT)`. `partition_expression` therefore hashes every integer type except
> `UHUGEINT` as `HUGEINT`, and `FLOAT` as `DOUBLE`, whatever the query compares;
> `py_test/unit/test_partitioning.py` pins both the fact and the fix. Within one aggregate
> every key has a single type, so Phase 2 needs it only for consistency; Phase 3's joins
> need it for correctness. `DECIMAL` keys of different scales are still Phase 3's to align.
>
> **As built, Phase 3 (2026-11-18, kickoff answer 5):** `plan/partitioning.py`
> `join_hash_type(l, r)` picks the type both sides of a join key are hashed as: one type
> on both sides is hashed as an aggregate key would be (so an aggregate's output and a
> join side can be co-partitioned); integers of different widths as `HUGEINT`; any
> `FLOAT`/`DOUBLE` among numbers as `DOUBLE`; integers and `DECIMAL`s as
> `DECIMAL(38, the larger scale)` when every value fits; anything else (`VARCHAR =
> INTEGER`, `DATE = TIMESTAMP`) does not hash alike, so that join broadcasts or runs
> single-node. A hypothesis test checks that values DuckDB calls equal land in one
> partition for every mixed pair.

### 4.2 Two-phase aggregation

`Aggregate(keys, aggs)` over a scan is cut into **partial** (on the splits) → `Hash(keys)` →
**final**. The partial stage runs only when *every* aggregate in the select is in the
decomposition table; otherwise raw rows are shuffled by `keys` and the final stage runs the
original aggregate unchanged (P2 — always correct, sometimes slower).

| Aggregate | Partial (per split) | Final (per partition) | Note |
|---|---|---|---|
| `count(*)`, `count(x)` | `count(...)` | `sum(p)` | `coalesce(sum, 0)` on an empty group |
| `sum(x)` | `sum(x)` | `sum(p)` | summation order changes the last digits of a double; icetl FINDINGS 3.7 already tolerates this |
| `min` / `max` | same | same | |
| `avg(x)` | `sum(x)`, `count(x)` | `sum(s) / sum(c)` | typed as DuckDB's `avg` would be; checked against the oracle |
| `bool_and/or`, `bit_and/or/xor` | same | same | |
| `count(distinct x)`, `approx_count_distinct` | — raw path — | original | two-level distinct is a Phase 7 optimisation |
| `first`, `last`, `any_value`, `collect_list/set`, `string_agg`, `mode`, `median`, `percentile*`, `stddev*`, `var*`, `corr`, `regr_*` | — raw path — | original | nondeterministic or order-dependent; raw rows co-located by key is the only honest answer |
| grouping sets / rollup / cube | — Phase 4 — | | one `Hash` per grouping set, `UNION ALL` of the finals |

`HAVING` runs in the final stage. A global aggregate (no keys) is partial → `Single` →
final. icetl's manifest-only `count(*)` shortcut still fires before any of this.

### 4.3 Joins

| Shape | Strategy |
|---|---|
| equi-join, one side ≤ `icefleet.broadcast.thresholdBytes` (default 64 MiB; size from `ScanPlan.bytes_scanned` for a scan, from the measured shuffle manifest for a derived side) | **broadcast**: the small side is written once, unpartitioned; every task of the other side reads all of it |
| equi-join, both large | **shuffle hash join**: both sides `Hash(keys, n)` with aligned key types; each partition joins locally |
| outer joins | broadcast only the side that may be broadcast for that kind: inner → either; left/semi/anti → right side; right → left side; full → never |
| non-equi or cross | broadcast if a side fits, else `Uncuttable` in the first release |
| multi-way | left-deep in the order the optimizer left; a side already `Hash`-partitioned on the needed keys with the same `n` is not reshuffled |

No cost-based join reordering (decision 6). sqlglot's `optimize_joins` already runs in icetl.

> **Correction, 2026-11-18 (Phase 3, kickoff answer 2):** the strategy is chosen *when
> planning*, so `explain()` is exact before anything runs. A side's size is an upper
> bound: a scan is `ScanPlan.bytes_scanned` (parquet bytes of the selected columns), an
> aggregate at most its input's size (a global aggregate 0), and a join's output is never
> broadcast. "From the measured shuffle manifest for a derived side" above is Phase 7's.
> The bound is loose in one common case: icetl plans **one** `ScanPlan` for every
> reference to a table, pruned by the union of their filters, so both sides of a
> self-join -- including an aggregate of the same table -- are bounded by that whole scan
> (the proof of life: a 265-row zone aggregate bounded at 330 MB). Of an inner (or cross)
> join both sides may be broadcast; the smaller is, the right on a tie. Equality keys
> include `IS NOT DISTINCT FROM`; every other conjunct of the ON stays in it and runs per
> partition. A conjunct of the WHERE that reads one relation runs below the join too when
> no outer join can NULL that relation, and so does an ON conjunct reading only the
> joined relation of an inner, left, semi or anti join (a copy: the original stays). A
> side already hash-partitioned by its keys with the same count and hash types is not
> reshuffled, and an aggregate over rows already partitioned by a subset of its keys runs
> in place, whole, with no partial and no shuffle (kickoff answer 4).

### 4.4 The exchange

```
<location>/<query_id>/stage-<k>/task-<i>/__part=<p>/data_0.parquet      shuffle
<location>/<query_id>/stage-<k>/task-<i>/data_0.parquet                 result · broadcast
```

- **Write:** `COPY (SELECT <stage columns>, hash(<typed keys>) % <n> AS __part FROM <stage>) TO '<prefix>' (FORMAT parquet, PARTITION_BY (__part), COMPRESSION zstd, RETURN_FILES true)` — one statement, DuckDB does the hashing, partitioning and I/O. `RETURN_FILES` gives the manifest; if it proves unavailable the worker lists the prefix.
- **Read:** `read_parquet($in, union_by_name => false, hive_partitioning => false)` with `$in` = the explicit file list for partition `p` across every upstream task. Empty partition → the registered empty Arrow table icetl already uses for an empty scan.
- **Why not a glob:** a glob reads what is there, including a failed attempt's leftovers. A manifest reads what completed. P5.
- **Clean-up:** the query prefix is removed by the coordinator (`s3fs` / `shutil`) in a `finally`; a crashed coordinator leaves garbage, and `icefleet-admin gc <location> --older-than 1d` (Phase 7) removes it.
- **Single-machine `local[N]`:** `LocalDirExchange` is the same code with a directory; it is what the offline suite exercises, so the object-store implementation adds only the path scheme and the listing/deletion client.

> **As built, Phase 2 (2026-11-16):** `RETURN_STATS` (not `RETURN_FILES`) gives the
> manifest, and the partition is read off each file's `__part=<p>/` directory.
> `FILE_SIZE_BYTES` cannot be combined with `PARTITION_BY` in DuckDB 1.5.6, so shuffle
> files are not rolled (one file per task and partition). The partition column is not
> written into the files; reading one with DuckDB's default hive auto-detection would
> invent it from the path, which is why every read says `hive_partitioning => false`.
> A task with no rows writes nothing and reports no files. A final-stage partition no
> row reached gets **no task**; when no partition has rows at all, partition 0 runs over
> a typed empty relation (`SELECT CAST(NULL AS t) AS c, ... WHERE FALSE`) rendered into
> its SQL, so the result keeps its schema and a global aggregate its one row. The
> reserved `InputSpec.schema_ipc` is not needed for that.
>
> **As built, Phase 3 (2026-11-18):** a broadcast is a stage with the `result` sink
> (`.../stage-<k>/task-<i>/data_<n>.parquet`, one typed file even when empty), and every
> task of the stage reading it binds *all* its files (`StageInput.mode = "all"`); a
> shuffle input binds the task's partition (`"partition"`). A stage reads each input under
> its own placeholder `__icefleet_input_<k>__` and parameter `$icefleet_input_<k>`. A
> partition-driven stage gets a task per partition any of its partitioned inputs has
> rows in; an input with no rows there is read as its typed empty relation.

### 4.5 Splits

Input: one scan's files from icetl's `plan_scan()` (pruned by predicate and partition, with per-file sizes, record counts and per-column byte sizes from the manifests).

- Size of a file for packing = bytes of the *selected* columns (`column_sizes` summed over the selected field ids), falling back to the file size — the same number icetl's `explain()` prints as `bytes_scanned`.
- First-fit decreasing into bins of `icefleet.split.targetBytes` (default 128 MiB, to be measured).
- Lower bound on split count: `workers × slots`, so a small table still parallelises. Upper bound: `icefleet.split.max` (default 4096).
- A file is never split in the first release (no row-group ranges, decision 6).
- Renamed / late-added columns: icetl groups such files and aliases them per group. In the first release a scan whose `ScanPlan` carries aliasing (`renamed_columns` or a group whose projection has `stored != output`) is **uncuttable** → single-node. Phase 7 packs within groups.

### 4.6 Distributed write, one commit

The final stage's sink is `iceberg_files`:

1. The coordinator resolves the table (or creates it, for `saveAsTable`, from the plan's analysed schema exactly as icetl does), reads its location, schema, spec and properties.
2. Each task runs its stage SQL and **streams** Arrow batches from DuckDB into a parquet writer that writes `PARQUET:field_id` metadata (`pyiceberg.io.pyarrow.schema_to_pyarrow(table.schema())` supplies the Arrow schema). Files go under `<location>/data/<partition path>/<query>-<task>-<seq>.parquet`, rolled at `write.target-file-size-bytes`.
3. Identity partitioning: the stage SQL orders by the partition columns; the writer switches files when the partition value changes. Non-identity transforms (`bucket`, `truncate`, `year/month/day/hour`) are **uncuttable** in the first release → icetl's single-node writer.
4. Workers return the file paths. The coordinator opens one transaction: `delete(filter)` for overwrite / dynamic partition overwrite, `add_files(paths)` for the rows, `commit_transaction()`. icetl's `commit_with_retry` wraps it. `add_files` reads each footer for statistics and partition values, which is why field ids are written: without them icetl's own rename detection would read the files as having no columns.
5. `insertInto`, SQL `INSERT`, `saveAsTable` and `df.write` all land here; `DELETE`/`UPDATE`/`MERGE` (Phase 5b) reuse icetl's row-level planner for the *rows* and this path for the files.

Why field ids matter enough to write them ourselves: DuckDB's `COPY` does not write them, and a file without them is correct only through a name mapping that not every reader honours.

> **Correction, 2026-11-15 (owner agreed at the Phase 1 kickoff):** DuckDB 1.5.6's `COPY ... (FORMAT parquet, FIELD_IDS {col: id, ...})` *does* write `PARQUET:field_id` (measured: `parquet_schema()` reads them back). So Phase 5 should write data files with DuckDB, which keeps P4 and keeps pyarrow's Parquet writer off the workers. Steps 2–3 above become `COPY ... FIELD_IDS` with `FILE_SIZE_BYTES` rolling and `RETURN_STATS` for the manifest; nested field ids use DuckDB's `__duckdb_field_id` form. To be confirmed against `add_files` and icetl's rename detection in Phase 5.

### 4.7 Semantics under distribution

- Spark 3.5 remains the reference, inherited through icetl's conformance pass, which runs on the whole tree *before* cutting. A stage never sees an unconformed expression.
- Floating-point sums and averages differ in the last digits by summation order. Recorded in `docs/divergence.md`; tests compare with `rel=1e-9` on doubles, exactly on everything else.
- `first`/`last`/`any_value`/`collect_list` are nondeterministic on the reference too; the raw path keeps them *as* nondeterministic as DuckDB single-node, no more.
- `ORDER BY` output is produced by reading range partitions in order; `LIMIT` without `ORDER BY` returns *some* n rows, as the reference does.
- NULL join keys never match, so their partition does not matter; NULL group keys hash consistently (`hash(NULL)` is a fixed value in DuckDB) and meet in one partition.

### 4.8 Fallback

`Uncuttable(reason)` → `ClusterSession` calls `super()._execute(...)`, i.e. icetl single-node on the coordinator's own DuckDB, with icetl's own spill. `explain()` prints `mode: single-node — <reason>`. `icefleet.fallback=error` turns it into `UnsupportedFeatureError` for pipelines that must not silently run on one machine.

### 4.9 `explain()`

```
== Distributed Plan ==
stage 0  scan nyc.yellow_tripdata   14 splits · 62 of 62 files · 1.9 GB of 11.2 GB · partial: sum(total_amount), count(*)
         -> shuffle hash(VendorID) x 64
stage 1  final: sum(p0), sum(p1) · group by VendorID · 64 partitions
         -> result
cluster  3 workers x 2 slots · exchange s3://icefleet-shuffle/q-7f3a
```
`explain(mode="analyze")` (Phase 7) adds measured rows, bytes and seconds per stage and flags skew (max task > 4× median).

### 4.10 What is reused from icetl, and the seams

Reused as-is: the entire surface (`Session`, `DataFrame`, `Column`, `functions`, `Window`,
`GroupedData`, readers, `conformance`), `plan.optimizer`, `plan.pushdown`, `plan.annotations`,
`plan.schema`, `exec.scan_planner.plan_scan`, `exec.engine.DuckDBEngine` (on every worker),
`catalog.*`, `conf`, `errors`, `sql.writer.commit_with_retry` / `iceberg_ready`.

Overridden (private, which is why the pin is exact):

| icetl seam | icefleet override | Upstream plan |
|---|---|---|
| `Session._execute`, `Session._stream` | route through the stage planner, fall back via `super()` | icetl 0.2.0: an `Executor` protocol so a subclass overrides one documented method |
| `Session._compile` | called unchanged, then its `optimized` tree and `scans` are consumed | expose `CompiledPlan` as public |
| `DataFrame.write` → `DataFrameWriter._save` | a `ClusterDataFrameWriter` that compiles to an `iceberg_files` sink (Phase 5) | icetl 0.2.0: `Session._writer_class` hook |
| `exec.scan_planner._selected_field_ids` and friends | copied, not imported, for split sizing (small, pure) | make public |
| `DataFrame._explain_text` *(added 2026-11-15, Phase 1)* | wrapped at import: frames of a `ClusterSession` get the `== Distributed Plan ==` section first; every other frame unchanged. A subclass cannot do it, because icetl derives every new frame as a plain `DataFrame` | icetl 0.2.0: an explain hook on the `Executor` protocol |
| `Session.Builder.getOrCreate` / `create`, `Session._active` *(Phase 1)* | `ClusterSession.Builder` subclasses them so the builder makes a `ClusterSession` (icetl's hard-codes `Session(...)`) | `Session._builder_class` or `cls(...)` in the builder |
| `Session._analyzer.connection`, `Session._udf`, `Session._engine` *(Phase 1, read only)* | `DESCRIBE` of the output types (the parquet-safety check, §4.4); UDF names (a UDF call is uncuttable); the coordinator's DuckDB reads the result | part of the `Executor` context |
| per-file sizes inside `plan_scan` *(Phase 1)* | not reachable, so `plan/splits.py` lists the scan's files again from the manifests (one extra metadata pass on the coordinator) | `ScanPlan` keeps per-file sizes |
| `exec.source_sql.build_source`, `DuckDBEngine.register` *(Phase 2)* | stage SQL is no longer icetl's compiled SQL: each stage's own tree is rendered through `substitute_sources` + `build_source` with `CompiledPlan.scans`, exactly as `_compile` renders the whole query | `CompiledPlan` exposes a `render(tree)` |
| `Session._analyzer.connection` *(Phase 2, wider use)* | `DESCRIBE` types every stage (keys, partials, each aggregate as it is single-node); `duckdb_functions()` lists DuckDB's aggregates, because sqlglot leaves some (`list`) as scalar functions | part of the `Executor` context |

Each upstream hook is a small, additive PR to the icetl repo and a version bump of the pin. Until then: `icetl==0.1.0`, and `py_test/test_seams.py` asserts every private attribute icefleet touches still exists with the expected signature, so a pin bump fails loudly rather than mysteriously.

---

## 5. Package layout

```
py_src/icefleet/
  __init__.py            ClusterSession, __version__
  session.py             ClusterSession(icetl.sql.Session)
  config.py              icefleet.* settings on top of icetl's conf
  errors.py              Uncuttable, ClusterError, WorkerError
  _versions.py           the handshake's versions and protocol number
  plan/
    rel.py               RelNode types (Scan, Project, Aggregate, Join) and Query; from_sqlglot,
                         IN (subquery) -> SEMI JOIN, an aggregating join split in two
    partitioning.py      Hash, partition_expression, the type a key is hashed as (Phase 3;
                         Range is Phase 4's)
    stages.py            Stage, StageInput, StagePlan, the pipeline planner, plan_stages()
    aggregates.py        decomposition table (§4.2), the partial/final rewrite
    types.py             parquet-unsafe types and their lossless stand-ins (Phase 2)
    joins.py             equality keys, which side may be broadcast, NULL-supplied relations (§4.3)
    splits.py            bin-packing (§4.5)
    tasks.py             TaskSpec / TaskResult (de)serialisation
    describe.py          the explain() renderer
  scheduler/
    base.py              Scheduler protocol: run(StagePlan) -> manifests; shared stage ordering
    threadpool.py        ThreadPoolScheduler: tasks to the slots of running workers over HTTP,
                         every stage whose inputs are done in one wave (Phase 3)
    argo.py              ArgoScheduler (Phase 6b): one Workflow per query, a pod per task
  exchange/
    base.py              Exchange protocol: locate, read_source(), list, delete
    local.py             LocalDirExchange
    object_store.py      ObjectStoreExchange (S3/MinIO)
  cluster/
    client.py            HTTP client (urllib), handshake, slot accounting
    launcher.py          local[N] subprocess launcher
    discovery.py         static://, local[N] and k8s:// (headless-Service DNS) resolution
  worker/
    __main__.py, cli.py  icefleet-worker: `serve` (long-lived) · `run-task <spec-uri>` (one task, Phase 6b)
    server.py            stdlib HTTP server: /ping, /tasks, /cancel
    runner.py            executes a TaskSpec on icetl's DuckDBEngine
    sinks.py             shuffle / result / iceberg_files writers
  write/
    commit.py            add_files + delete in one transaction, commit_with_retry
    parquet.py           streaming parquet writer with Iceberg field ids
py_test/
  conftest.py            local[N] cluster fixture, SqlCatalog warehouse, oracle session
  fixtures/generator.py  fixture tables (adapted from icetl's)
  unit/                  rel, partitioning, splits, aggregates, tasks, config
  cluster/               end-to-end on local[N] against the oracle
  integration/           REST catalog + MinIO, later the dev Kubernetes cluster (-m integration)
docs/                    this plan · task_tracker.md · commands.md · workflow · divergence.md · findings.md
scripts/                 dev.ps1 · wheelhouse.ps1 (decision 28) · smoke_cluster.py (Phase 1) · benchmark.py (Phase 6)
deploy/
  dev/                   the dev container (decision 28): Dockerfile · compose.yml · devrun.sh · devsetup.sh
  Dockerfile             one image: icefleet-worker serve | run-task | the coordinator script
  docker-compose.yml     local smoke test of the image (3 workers + a driver with Jupyter)
  k8s/                   namespace · Secret template · headless Service · worker StatefulSet (model A)
  argo/                  WorkflowTemplates: fleet-per-workflow (model B, Phase 6) · pod-per-task (model C, Phase 6b)
```

---

## 6. Phases

Every phase: finishable in a few sittings, ends with the gate green (`uv run tox`),
the tracker updated, and **a question to the owner** — the next phase does not start until
the owner confirms. Each phase lists what "done" means so that confirmation is a check, not
a feeling.

### Phase 0 — Scaffold *(2026-11-15)*

**Goal.** A project that installs, lints, type-checks and runs an empty test suite inside its own venv, with the resumable-work files in place.

- `pyproject.toml`: `icefleet` 0.0.1, Python 3.12, deps `icetl==0.1.0`, `pyarrow`, `duckdb`, `pyiceberg`, `sqlglot` (pinned to icetl's ranges), `cloudpickle`, `s3fs`, `python-dotenv`, `rich`; extras `notebook` (`ipykernel`, `jupyterlab`), `dev` (`pytest`, `pytest-cov`, `hypothesis`, `ruff`, `mypy`); dev group `tox`, `tox-uv`, `uv`.
- Layout per the workflow file: `py_src/icefleet`, `py_test`, `docs`; tooling pointed at them.
- `tox.ini`: `lint`, `type`, `py312`, `dist`.
- `icefleet.__version__`; `icefleet-worker --help` entry point (argparse only).
- Smoke tests: `import icefleet`; `import pyarrow.flight`; `icetl.__version__ == "0.1.0"`; the worker CLI prints help.
- Docs: this plan, `task_tracker.md`, `commands.md`, `workflow_instructions_file.md`, `CLAUDE.md` at the root pointing at them, `README.md`, `.env.example`, `.env` for the dev catalog (§3.4), `.gitignore`.
- venv bootstrap recorded in `commands.md` (uv is not on this machine's PATH; it is installed *inside* the venv).

**Done when:** `uv run tox` is green from a fresh clone following only `commands.md`; the tracker says Phase 0 done and Phase 1 next.

### Phase 1 — Worker, control plane, scan-only queries, `local[N]`

**Goal.** The thinnest end-to-end slice: a filter/projection over an Iceberg table runs on two worker processes and returns the same rows icetl returns alone — on the local fixtures offline, and on `nyc.yellow_tripdata` through the dev catalog.

- `worker/server.py`: stdlib `ThreadingHTTPServer` with `GET /ping` (versions: icefleet, duckdb, pyarrow, icetl), `POST /tasks` (blocks until the task ends, returns `TaskResult`), `POST /cancel`; a semaphore of `slots`.
- `worker/runner.py`: binds `TaskSpec.inputs` as parameters, runs `sql` on icetl's `DuckDBEngine` (its httpfs + secret handling reused), drives the sink.
- `worker/sinks.py`: `result` sink only (parquet to the exchange, `RETURN_FILES` manifest).
- `worker/cli.py`: `--host --port --slots --memory-limit --threads --temp-dir --exchange`.
- `cluster/launcher.py`: `local[N]` spawns N workers on free 127.0.0.1 ports, divides memory/threads, waits for `ping`, terminates on `session.stop()`.
- `cluster/client.py`: handshake refuses mismatched DuckDB versions; slot accounting.
- `cluster/discovery.py`: `static://`, `local[N]`, and `k8s://<service>:<port>` as a DNS lookup returning every ready pod (tested offline with a stubbed resolver; proven in-cluster in Phase 6).
- `scheduler/base.py` + `scheduler/threadpool.py`: the protocol and its first backend, so Phase 6b adds a class rather than a refactor.
- `plan/rel.py` + `plan/stages.py` v1: recognise `Project(Scan)` (with `WHERE`, any expressions, optional `LIMIT`); everything else `Uncuttable`.
- `plan/splits.py`: bin-packing with tests (every file in exactly one split; bytes balanced within 2× for the fixture).
- `exchange/local.py`; `exchange/object_store.py` (write via DuckDB; list/delete via `s3fs`).
- `ThreadPoolScheduler`: single stage, N tasks, collect manifests, read result through the coordinator's DuckDB; `toArrowBatches` streams it.
- `session.py`: `ClusterSession`, `icefleet.master`, fallback with `explain()` note.
- `scripts/smoke_cluster.py`: proof of life against §3.4 — `local[2]`, `ObjectStoreExchange` on MinIO, a filtered projection of `nyc.yellow_tripdata` compared with icetl's answer; creates the shuffle bucket if missing.
- Tests (offline, `local[2]`): oracle equality on the fixture tables (sorted compare); `explain()` shows ≥ 2 splits; a `GROUP BY` falls back and says so; version-mismatch handshake is refused; worker death during a query fails the query cleanly; exchange prefix is gone afterwards.
- `py_test/test_seams.py`.

**Done when:** the oracle suite passes on `local[2]` and `local[3]`; `smoke_cluster.py` matches icetl on the real table; `uv run tox` green; tracker updated.

**As built (2026-11-15).** Where the build differs from the list above or adds to it:

- **Kickoff answers (owner, "yes to all"):** worker subcommands `serve` / `run-task` now (`run-task` exits 2, "Phase 6b"); bytes in the task JSON as base64 (`InputSpec.schema_ipc`, unused until Phase 2); §4.6 corrected (DuckDB writes field ids); the icetl hooks PR after Phase 5; the `icefleet-shuffle` bucket may be created; Phase 0 committed on `main` first.
- **CLI:** `--exchange` dropped (the sink location travels in each `TaskSpec`); `--port-file` (how `local[N]` learns a port-0 worker's port) and `--log-level` added.
- **Stage SQL is icetl's compiled SQL, unchanged.** The scan's file list is already a `$<view>_paths_0` parameter there; each task binds it to its split. Nothing is re-rendered.
- **Result sink:** `COPY (...) TO '<task dir>' (FORMAT parquet, COMPRESSION zstd, FILE_SIZE_BYTES n, RETURN_STATS true)`. `RETURN_STATS` (not `RETURN_FILES`) gives rows and bytes per file, so the manifest needs no listing, even on S3. A zero-row task still writes one typed file. DuckDB creates the task directory but not its parents, so the worker creates those for a local exchange.
- **Parquet-unsafe result types run single-node** (new rule, §4.4): `HUGEINT`, `UHUGEINT`, `TIMESTAMP_S/_MS`, fixed-size arrays, `ENUM`, `BIT`, `VARINT`, `UNION` change Arrow type through parquet (measured on DuckDB 1.5.6, pinned by `py_test/unit/test_result_types.py`). Phase 2 must cast partials of these types (e.g. `sum()` of an integer is `HUGEINT`) before they enter a shuffle.
- **Also single-node:** a scan pruned to no files (nothing to distribute), more than one file group (renamed or late-added columns; Phase 7), duplicate output names, a Python UDF call. A *single* group that needs aliasing is cut, because every split runs identical SQL.
- **Config keys** beyond §2: `icefleet.split.targetBytes`, `.split.max`, `.worker.slots`, `.local.threads`, `.local.memoryLimit`, `.result.maxBytes`, `.result.fileBytes`, `.task.timeoutSeconds`, `.worker.startupTimeoutSeconds` (all in `icefleet/config.py` with their environment names).
- **Proof of life:** `scripts/smoke_cluster.py` on `nyc.yellow_tripdata`: 13 of 62 files after pruning, 4 splits on `local[2]`, 6,485 rows identical to icetl, exchange prefix removed.

### Phase 2 — Exchange and aggregation

**Goal.** The headline query — a grouped aggregate over 41M rows — runs as two stages.

- `shuffle` sink: `COPY ... PARTITION_BY` with typed-key hashing; manifest per partition.
- Stage DAG with dependencies; the scheduler runs parents to completion first; a child's placeholder resolves to the explicit file list for its partition.
- `plan/aggregates.py` decomposition table (§4.2); partial/final rewrite; raw path; global aggregate; `HAVING`; `DISTINCT` as `Hash(all)`.
- Key type alignment (§4.1) with a test that an `INTEGER` key and a `BIGINT` key co-partition.
- `explain()` stage DAG renderer.
- `icefleet.shuffle.partitions` (default 64), `icefleet.exchange.keep`.
- Tests: every aggregate in the table against the oracle, grouped and global, with and without `HAVING`; `count(distinct)` raw path; empty groups; NULL keys; a property test (hypothesis) that the partitioner sends every row to exactly one partition and equal keys to the same one.

**Done when:** the aggregation suite passes on `local[3]`; the headline query runs two-stage on `nyc.yellow_tripdata` and matches icetl; the partition prefix layout matches §4.4; tracker updated.

**As built (2026-11-16).** Where the build differs from the list above or adds to it:

- **Kickoff answers (owner, "yes to all"):** (1) a GROUP BY / global aggregate / DISTINCT
  over one table, with WHERE, HAVING and expressions over aggregates; (2) `ORDER BY` /
  `LIMIT` on top applied by the coordinator, for aggregates and Phase 1 scans alike;
  (3) `icefleet.shuffle.partitions` default 64, no task for a partition without rows,
  tests at 4; (4) stage SQL built from the optimized tree's own nodes, rendered through
  icetl's `build_source` (a new seam, §4.10); (5) parquet-unsafe partials and results
  carried as lossless stand-ins and cast back, every final aggregate cast to its
  single-node type; (6) proof of life by extending `scripts/smoke_cluster.py`.
- **Recognised shape:** a *chain* of single-table SELECTs, `Project* → Aggregate? →
  Project* → Scan` (`plan/rel.py`). That is how icetl's optimizer leaves a flat aggregate,
  a `.filter()` after `.agg()` (a Project over an aggregate subquery) and a
  `.withColumn()` before `.groupBy()` (an aggregate over a Project subquery).
- **ORDER BY / LIMIT** are lifted off the top SELECT. A key that is not a result column
  becomes a hidden result column the coordinator orders by and drops; with a LIMIT, each
  final task keeps its own top-n first. ORDER BY / LIMIT inside a subquery, OFFSET, and
  ORDER BY outside a `SELECT DISTINCT` stay single-node (Phase 4).
- **Aggregates are found by DuckDB's catalog too.** sqlglot parses `list(x)` from icetl's
  trees as a scalar `exp.List`; Phase 1 would have distributed `SELECT list(x) FROM t` as
  row-at-a-time work and returned one list per split. Fixed: a call is an aggregate when
  it is a sqlglot `AggFunc` *or* its name is in `duckdb_functions()`'s aggregates.
- **Partial path** for `count`, `sum`, `avg` (as `sum` + `count`, only when the
  single-node type is `DOUBLE`), `min`, `max`, `bool_and/or`, `bit_and/or/xor`; anything
  else -- a `DISTINCT` or `FILTER` aggregate, `list`, `string_agg`, `median`, ... -- takes
  the **raw path** (§4.2), and `explain()` names the aggregate that forced it. Every
  final aggregate is `CAST(<combinator> AS <its single-node type>)`, found with one
  `DESCRIBE` of a probe SELECT; `avg` divides by `NULLIF(count, 0)` because DuckDB's
  `x / 0` is `inf`, not NULL.
- **Stand-ins** (`plan/types.py`): `HUGEINT`/`UHUGEINT` → `DECIMAL(38,0)`,
  `TIMESTAMP_S/_MS` → `TIMESTAMP`, `T[n]` → `T[]`. A Phase 1 result of one of these types
  now distributes too. ENUM, BIT, VARINT, UNION still run single-node.
- **Key alignment** (§4.1 correction): integer keys hashed as `HUGEINT`, `FLOAT` as
  `DOUBLE`. Grouping sets / ROLLUP / CUBE are Phase 4.
- **Explain** prints each stage's work (`partial: ... · group by ...` or `raw rows, no
  partial aggregate (<why>)`), the shuffle (`-> shuffle hash(k) x n`), the final, and a
  `coordinator` line for ORDER BY / LIMIT.
- **Proof of life:** `smoke_cluster.py` on `nyc.yellow_tripdata`: revenue, trips and mean
  distance per VendorID over 39 of 62 files (2024 onwards), 4 splits on `local[2]`, a
  64-way shuffle through MinIO of which 4 partitions had rows, 4 rows equal to icetl
  (doubles to 1e-9), prefix removed. 1.2 s cold, 1.0 s warm, icetl 0.6 s (one run each,
  not a benchmark). `docs/divergence.md` records the floating-point tolerance.

### Phase 3 — Joins

- `plan/joins.py`: broadcast vs shuffle choice from sizes; the outer-join broadcast rule; co-partition reuse in multi-way joins.
- `Broadcast` exchange: unpartitioned write, read by every task.
- Tests against the oracle: inner/left/right/full/semi/anti, small×large (broadcast), large×large (shuffle), three-way, join keys of differing integer widths, string keys, NULL keys, a join after an aggregate (derived side sized from the measured manifest), non-equi join → fallback.

**Done when:** the join suite passes; `explain()` names the strategy per join; tracker updated.

**As built (2026-11-18).** Where the build differs from the list above or adds to it:

- **Kickoff answers (owner, "yes to all"):** (1) equi-joins of every kind over two or more
  tables with Phase 2's shapes above and below, residual ON conditions per partition,
  cross / non-equi only by broadcast, and an uncorrelated `x IN (SELECT ...)` WHERE
  conjunct as a `SEMI JOIN` (`NOT IN`, `EXISTS`, correlated stay single-node); (2) the
  strategy chosen when planning, from upper bounds (§4.3 correction); (3) the broadcast
  side written once, read whole by every task (§4.4); (4) co-partition reuse for joins
  and for an aggregate on the join key; (5) key type alignment (§4.1); (6) independent
  stages run in one wave; (7) proof of life: a join of the table to an aggregate of
  itself, broadcast and shuffled.
- **The planner is a tree now, not Phase 2's chain.** `plan/rel.py` adds `Join` (a SELECT
  over two or more relations, one child per relation) and splits an aggregating join
  SELECT into an `Aggregate` over a `Join` subquery `__icefleet_j`. `plan/stages.py`
  plans bottom up with *open pipelines* -- SQL a task could run, what drives its tasks
  (a scan's splits, a shuffle's partitions, or a single task), what it reads, what it is
  partitioned by and how big it can be -- closed into a stage at a shuffle, a broadcast
  or the result. One `Stage` type replaces `ScanStage` / `ShuffleStage`. Phase 2's
  plans come out stage for stage as before.
- **Every table reference is its own pipeline.** icetl gives a self-join one `ScanPlan`
  and one `$<view>_paths_0` parameter for both references; each reference is planned and
  rendered in its own stage, so no stage binds two file lists to one parameter.
- **A join's columns travel under unique names.** Each side projects exactly the columns
  the join reads or returns, named after the column where that is unambiguous
  (`<alias>_<name>` otherwise); ON, WHERE and the projection are rewritten onto them.
- **The scheduler runs waves** (kickoff answer 6): every stage whose inputs are done, all
  its tasks sharing the slots. The result is sorted by (stage, task).
- **Found and fixed, a Phase 1 race:** the worker released a task's slot only *after*
  sending its answer, so a coordinator that handed the slot its next task at once could
  be refused "all slots are busy". Phase 3's waves of short tasks hit it once in the
  gate. The slot is now released before the answer goes out
  (`test_worker_server.py::test_the_slot_is_free_when_the_answer_arrives`, which fails
  against the old server).
- **Config:** `icefleet.broadcast.thresholdBytes` (64 MiB; `0` never broadcasts).
- **Proof of life:** `smoke_cluster.py` on `nyc.yellow_tripdata`: long trips (13 of 62
  files) joined to the average fare per pickup zone in January 2024, both scans 14 of 62
  files. Broadcast (threshold raised to 1 GiB, §4.3 correction): 3 stages -- partial,
  final -> broadcast, the trip scan joining it -- 6,485 rows equal to icetl. Shuffled
  (threshold 0): 3 stages -- partial, the trips shuffled by zone, and the join *inside*
  the zone aggregate's final stage, already partitioned by zone -- 6,485 rows equal to
  icetl. Prefix removed both times. 2.2 s and 4.4 s cold, icetl 0.7 s (one run each, not
  a benchmark).

### Phase 4 — Windows, ordering, set operations, grouping sets

- `Window` → `Hash(partition_keys)`; no keys → `Single` with a warning.
- `Sort` with `LIMIT` → local top-n + `Single`; without → `Range` by sampling (sample stage of `icefleet.sort.sampleRows`, boundaries computed on the coordinator, partitions read in order).
- `LIMIT` without `ORDER BY`; `UNION ALL` by manifest concatenation; `UNION`/`INTERSECT`/`EXCEPT` via `Hash(all)`.
- Grouping sets / rollup / cube: one aggregate per set, `UNION ALL`.
- Tests against the oracle, including icetl's `test_window.py` cases run through the cluster.

### Phase 5 — Distributed writes

- `write/parquet.py` streaming writer with field ids and partition switching; `write/commit.py` single transaction; `worker/sinks.py` `iceberg_files`.
- `ClusterDataFrameWriter`: `saveAsTable` (create on the coordinator, fill distributed), `insertInto`, `mode("append"|"overwrite")`, `partitionOverwriteMode=dynamic`, SQL `INSERT INTO/OVERWRITE`.
- Non-identity partition specs and `mergeSchema` → icetl's writer (fallback).
- Tests: written tables read back identical through *icetl* (the independent reader); one snapshot per write; partition paths correct; a concurrent-commit retry; icetl's rename detection still works on files icefleet wrote.

**Phase 5b — row-level operations.** `DELETE`/`UPDATE`/`MERGE` through icetl's row-level planner with the rows produced distributed and committed via `write/commit.py`.

### Phase 6 — The image, Kubernetes, Argo fleet-per-workflow, integration tier, benchmarks

**Goal.** The engine runs on the dev Kubernetes cluster as an Argo workflow that brings its own workers and leaves nothing behind.

- `deploy/Dockerfile`: `uv`-built from the lock, `httpfs` pre-installed, non-root, entry point `icefleet-worker`. `deploy/docker-compose.yml` as the local smoke test of that image (3 workers + a driver with Jupyter against §3.4) — the test of the image, not a deployment.
- `deploy/k8s/`: namespace, `Secret` template generated from `.env` (host names rewritten for in-cluster access), headless `Service`, worker `StatefulSet` with the memory/CPU rules of §3.5 (model A, for interactive use).
- `deploy/argo/fleet.yaml`: a `WorkflowTemplate` with N daemon worker steps and a coordinator step that runs a script passed as a parameter or mounted from a `ConfigMap`; the daemons carry the headless Service's label so the coordinator uses `k8s://` unchanged (model B).
- Worker start-up check: refuse a `memory_limit` within 15% of the cgroup limit (§3.5).
- `py_test/integration/`: the icetl integration pattern — namespace guard, protected-table witness, seeded slices in `icefleet_it`, the 41M-row suite (`slow`) — runnable from the host against `local[N]`, and from inside the cluster as an Argo workflow.
- `scripts/benchmark.py`: the icetl suite plus join and window cases, **single-node vs `local[N]` vs the Argo fleet**; results into `docs/BENCHMARKS.md`.
- Defaults set from measurement: split size, partitions, broadcast threshold, slots, compression, pod memory headroom.
- Dev cluster prerequisite before this phase starts: Kubernetes enabled in Docker Desktop (decision 27), Docker Desktop given ~10 GB so the existing MinIO/catalog containers and the worker pods fit, Argo Workflows installed with `kubectl apply` of its release manifest. Docker Desktop's signed `kubectl` is enough; the `argo` CLI is optional (REST submission).

**Done when:** `argo submit` of the fleet template runs the 41M-row aggregate, window and join end to end on the dev cluster with answers equal to icetl's, the daemon pods are gone afterwards, the exchange prefix is gone, and the benchmark file records the ratio to single-node.

### Phase 6b — Argo as a scheduler backend (pod per task)

**Goal.** The same query runs with no standing workers at all: Argo schedules one pod per task.

- `icefleet-worker run-task <spec-uri>`: read a `TaskSpec` from the exchange, execute it, write the `TaskResult` beside it, exit 0/1. Reuses `worker/runner.py` unchanged.
- `scheduler/argo.py`: `ArgoScheduler` — spec files to the exchange, Workflow generation (a DAG node per stage, `withItems` over task indices, `parallelism`, `retryStrategy`, `ttlStrategy`, resources from `icefleet.argo.*` config), submission through the Argo Server REST API with a bearer token from the environment, status polling, result collection per stage.
- `icefleet.master = argo://<namespace>?server=<url>`; `icefleet.argo.image`, `.parallelism`, `.memory`, `.cpu`, `.retries`.
- Unit tests: golden-file Workflow YAML for a two-stage plan; a stubbed Argo server for the submit/poll loop. Integration: the headline query on the dev cluster.

**Done when:** the headline aggregate runs pod-per-task on the dev cluster and matches icetl; a task pod killed mid-run is retried by Argo and the answer is unchanged; finished pods are reaped by `ttlStrategy`; `explain()` names the backend.

### Phase 7 — Robustness and adaptivity

- Task retry on the thread-pool backend (rerun on another worker; output dir per attempt; manifest from the winning attempt) — the Argo backend already has it from Phase 6b; worker loss detection, query cancellation, `icefleet-admin gc`.
- Adaptive partition coalescing: after a shuffle, merge small partitions into one task using the manifest sizes; skew flagging in `explain(mode="analyze")`.
- Renamed / late-added column scans become cuttable (splits within icetl's groups).
- Two-level `count(distinct)`.
- Per-stage metrics; `explain(mode="analyze")`.

### Phase 8 — Fast paths and extras

- `StreamExchange`: workers serve their stage output as Arrow IPC streams over HTTP (`GET /stages/<k>/<p>`); downstream pulls directly; spill to local disk; the object store remains the fallback and the broadcast path. Flight is a drop-in alternative transport where its DLL loads.
- Result streaming as one Arrow IPC stream over HTTP (no result files).
- Python UDFs: cloudpickle the function, register on every worker per query; icetl's UDF registration reused.
- Optional standalone coordinator service for shared clusters.

---

## 7. Deferred by decision

| Item | Why | Where it would go |
|---|---|---|
| Merge-on-read deletes | icetl decision 11; refused at scan | after icetl Phase 12 |
| Helm chart | decision 27: plain manifests first | once `deploy/k8s/` and `deploy/argo/` have stopped changing |
| Argo Events / cron triggering of pipelines | out of the engine's scope | the owner's workflow repo; a `CronWorkflow` wrapping the fleet template is a one-file example |
| VMs without Kubernetes | decision 27 | `static://` already covers it if ever needed; nothing to build |
| Auth / TLS between processes | decision 20 | when the cluster leaves a trusted network |
| Web UI | decision 21 | never, unless `explain(analyze)` proves insufficient |
| Row-group-level splits | decision 6 | if a single huge file ever dominates a benchmark |
| `bucket`/`truncate`/time-transform partition writes | needs Iceberg's transforms in SQL | Phase 5 follow-up |
| Cost-based join ordering | decision 6 | — |
| Decimal promotion, SQL-surface function resolution | icetl Phases 14, 15 — inherited | upstream |
| Multi-tenant coordinator | decision 1 | Phase 8, optional |

---

## 8. Risks

| Risk | Exposure | Mitigation |
|---|---|---|
| icetl private seams change under a pin bump | Phases 1, 5 | exact pin; `test_seams.py`; upstream hooks in icetl 0.2.0 |
| `hash()` differs across DuckDB versions or key types | silent wrong joins/aggregates | version handshake; typed-key alignment; property test |
| `COPY ... RETURN_FILES` unavailable or shaped differently | Phase 2 | verify in Phase 1 spike; listing fallback |
| `add_files` partition inference rejects a file | Phase 5 | one partition value per file by construction; integration test |
| Object-store latency makes small queries slower than single-node | Phase 6 | measure; a "small query" heuristic (bytes below N → single-node) is cheap |
| `local[N]` oversubscribes RAM | tests | memory divided across workers at launch |
| stdlib HTTP control plane: a thread per connection, no backpressure | many workers × slots | fine at a handful of nodes; measure before Phase 8; the Argo backend has no such plane at all |
| Pod OOMKill: DuckDB over the container limit dies with no message | every Kubernetes run | headroom rule and start-up check (§3.5); spill on `emptyDir`; peak memory in `explain(analyze)` |
| Pod start latency per task on the Argo backend | Phase 6b, small queries | small-query heuristic; standing fleet for interactive work; `parallelism` sized to the cluster |
| `httpfs` extension fetched at runtime inside a pod | every Kubernetes run | baked into the image at build; the image smoke test imports it offline |
| Argo Server access (token, RBAC) for the coordinator to submit Workflows | Phase 6b | a `ServiceAccount` with `workflows: create/get/list` only, in the manifests; model B needs none of it |
| Windows firewall prompts for 127.0.0.1 listeners | dev | bind loopback only in `local[N]` |
| Floating-point disagreement with the oracle | tests | tolerance only on floating types, exact elsewhere |

---

## 9. Testing strategy

- **The oracle is icetl.** Every cluster test runs the same plan on a plain `Session` over the same warehouse and compares sorted Arrow tables, exact except for floating-point columns (`rel=1e-9`).
- **Offline tier** (`uv run pytest`): `local[N]` workers, PyIceberg `SqlCatalog` on sqlite, local warehouse, `LocalDirExchange`. No network. Target under 3 minutes.
- **Integration tier** (`-m integration`): the dev catalog (§3.4), `ObjectStoreExchange` on MinIO, the real 41M-row tables, and from Phase 6 the dev Kubernetes cluster with Argo (§3.5), both the fleet-per-workflow and the pod-per-task paths; `slow` for full scans. Namespace guard and snapshot witness copied from icetl.
- **Golden manifests**: the Workflow YAML the `ArgoScheduler` generates for a fixed plan is checked into `py_test/golden/` and diffed, so a change in the generated workflow is a reviewed change.
- **Properties** (hypothesis): partitioner totality and consistency; split packing covers every file once; `to_sqlglot(from_sqlglot(x))` round-trips for recognised shapes.
- **Seams**: `test_seams.py` pins icetl's private surface.
- **Shapes**: `explain()` output is asserted for stage count, strategy and fallback reason, so a regression that quietly stops distributing is caught.

---

## 10. Conventions

Work runs as [workflow_instructions_file.md](workflow_instructions_file.md) says: plan →
phase → gate green → tracker updated → **owner confirms** → next phase. Every command goes
to [commands.md](commands.md). Everything runs inside `.venv` through `uv`. Nothing is
committed unless asked.
