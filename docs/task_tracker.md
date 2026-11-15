# Task Tracker

Current state of work. **Read this first when resuming.**
Update at the end of every session. Design lives in
[PLAN_distributed_engine.md](PLAN_distributed_engine.md); status lives here.

## At a glance

| | |
|---|---|
| Last updated | 2026-11-15 |
| Current phase | **Phase 0 — Scaffold: done, confirmed by the owner 2026-11-15** (with decisions 26 and 28) |
| Next action | Phase 1 kickoff: the owner answers the up-front questions (asked 2026-11-15) → build Phase 1 |
| Next phase | Phase 1 — Worker, control plane, scan-only queries, `local[N]` |
| Blocked on | Owner's answers to the Phase 1 kickoff questions |
| Gate | **`.\scripts\dev.ps1 uv run tox` green on 2026-11-15** in the dev container, both setup modes: lint · type · py312 8 passed 0 skipped · dist (online mode 11 s warm). The Windows `.venv` gate is red (Smart App Control) and **no longer authoritative** (decision 28) |

## Phases

| Phase | Scope (plan §6) | Status | Done on | Confirmed by owner |
|---|---|---|---|---|
| 0 | Scaffold: venv, uv, pyproject, tox gate, docs, smoke tests | **done** | 2026-11-15 | **yes, 2026-11-15** |
| 1 | Worker (HTTP), `local[N]`, splits, scan-only stages, fallback, proof of life on the dev catalog | not started | — | — |
| 2 | Exchange (shuffle) + two-phase aggregation + explain DAG | not started | — | — |
| 3 | Joins: broadcast and shuffle hash | not started | — | — |
| 4 | Windows, ORDER BY / LIMIT, set operations, grouping sets | not started | — | — |
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
- **git.** Repository initialised on branch `master`, every Phase 0 file staged, **no commit
  yet** (`.env` correctly unstaged). The first commit happens only when the owner asks
  (workflow rule). Phase 0's "green from a fresh clone" check cannot run until it exists.
- **icetl upstream hooks.** Plan §4.10 lists the private seams icefleet will override. Whether
  to open the `Executor` hook PR in icetl during Phase 1 or after Phase 5 is the owner's call;
  until then the pin stays `icetl==0.1.0`.

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

None in the code — nothing runs yet. In the environment:

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
