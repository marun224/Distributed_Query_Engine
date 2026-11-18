# icefleet

A distributed SQL and DataFrame engine over Apache Iceberg, executed by a fleet of DuckDB
workers. Built on [`icetl`](https://github.com/marun224/data_transformation_engine): the same
sqlglot IR, PyIceberg planning and DuckDB execution, with queries cut into stages that many
workers run at once.

```python
from icefleet import ClusterSession
from icetl.sql import functions as F

session = ClusterSession.builder.config("icefleet.master", "local[4]").getOrCreate()
df = (
    session.table("nyc.yellow_tripdata")
    .groupBy("VendorID")
    .agg(F.sum("total_amount").alias("revenue"))
)
df.explain()  # == Distributed Plan ==: partial sums -> shuffle by VendorID -> final sums
df.toArrow()  # the same rows icetl returns on one machine
```

**Status: Phase 4 built.** Filters, projections, aggregation (`GROUP BY`, global
aggregates, `HAVING`, `DISTINCT`) and joins run on a fleet of DuckDB workers (`local[N]`,
`static://`, `k8s://`): partial aggregates on the splits, a hash shuffle through the object
store, final aggregates per partition; inner, left, right, full, semi and anti joins (and
`IN (SELECT ...)`) either broadcast their small side
(`icefleet.broadcast.thresholdBytes`, 64 MiB) or shuffle both sides by the keys, skipping
a side already partitioned that way. Windows (and `QUALIFY`) are hashed by their
`PARTITION BY`; `UNION ALL`, `UNION`, `INTERSECT` and `EXCEPT` run on the workers;
grouping sets, `ROLLUP` and `CUBE` take one shuffle; a large `ORDER BY` is sorted by
range partitions on the workers. What cannot be distributed runs single-node through
icetl, and `explain()` says why. The design is in
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
