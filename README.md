# icefleet

A distributed SQL and DataFrame engine over Apache Iceberg, executed by a fleet of DuckDB
workers. Built on [`icetl`](https://github.com/marun224/data_transformation_engine): the same
sqlglot IR, PyIceberg planning and DuckDB execution, with queries cut into stages that many
workers run at once.

```python
from icefleet import ClusterSession
from icetl.sql import functions as F

session = ClusterSession.builder.config("icefleet.master", "local[4]").getOrCreate()
df = session.table("nyc.yellow_tripdata").filter(F.col("trip_distance") > 50)
df.explain()  # == Distributed Plan ==: the stage, its splits, or why it runs single-node
df.toArrow()  # the same rows icetl returns on one machine
```

**Status: Phase 1 built.** Scan-only queries (filters, projections, expressions, `LIMIT`)
run on a fleet of DuckDB workers (`local[N]`, `static://`, `k8s://`); everything else runs
single-node through icetl and `explain()` says why. Aggregation is Phase 2. The design is in
[docs/PLAN_distributed_engine.md](docs/PLAN_distributed_engine.md); what is done is in
[docs/task_tracker.md](docs/task_tracker.md); how work runs is in
[docs/workflow_instructions_file.md](docs/workflow_instructions_file.md).

## Setup

Development runs in a Linux dev container (Python 3.12, `uv`), driven from Windows with
Docker Desktop. The source stays on the host and is mounted in; the venv lives in the
container. See [docs/PLAN_linux_dev_environment.md](docs/PLAN_linux_dev_environment.md).

```powershell
cp .env.example .env              # then fill in the catalog and MinIO details
.\scripts\dev.ps1 up              # build + start; the dev catalog stack must be running
.\scripts\dev.ps1 setup           # uv sync in the container (wheelhouse fallback built in)
```

If Windows refuses to run the script (the default execution policy of Windows PowerShell 5.1),
call it as `powershell -ExecutionPolicy Bypass -File .\scripts\dev.ps1 up`, or allow local
scripts once with `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

A Windows `.venv` is optional and only for the editor:
`py -3.12 -m venv .venv; .venv\Scripts\python -m pip install --upgrade pip uv; .venv\Scripts\uv.exe sync --all-extras`.

## Gate

```powershell
.\scripts\dev.ps1 uv run tox                    # lint, mypy, tests against the built wheel, dist/
.\scripts\dev.ps1 uv run tox -e py312           # just the tests
.\scripts\dev.ps1 uv run pytest -m integration  # opt-in: needs the dev REST catalog + MinIO
```
