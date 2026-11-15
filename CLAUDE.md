# icefleet — how to resume

Read these, in this order, before doing anything:

1. [docs/task_tracker.md](docs/task_tracker.md) — **what is done, what is next, what is blocked.** The only source of state.
2. [docs/PLAN_distributed_engine.md](docs/PLAN_distributed_engine.md) — the design and the phases. Read the *next* phase's section fully.
3. [docs/workflow_instructions_file.md](docs/workflow_instructions_file.md) — the rules below in full.
4. [docs/commands.md](docs/commands.md) — every command that has been run; append yours.

## Rules that are easy to break

- **Phases are gated by the owner.** Finish a phase, run the gate, update the tracker, then
  *ask the owner to confirm*. Do not start the next phase until they say so.
- **Everything runs inside the Linux dev container through `uv`** (PLAN decision 28,
  [docs/PLAN_linux_dev_environment.md](docs/PLAN_linux_dev_environment.md)), driven from
  Windows by `.\scripts\dev.ps1 <command>`. The gate is authoritative only there. The repo
  is mounted at `/work`; the venv is `/opt/venv` in the container. The Windows `.venv` is
  kept **only** for VS Code's code analysis, so never run the gate in it (Smart App Control
  blocks pyarrow's Parquet modules there). Never `pip install` ad hoc; every dependency goes in
  `pyproject.toml`, then `.\scripts\dev.ps1 setup`.
- **Container downloads are flaky on this machine** (tracker, Known gaps). `setup` falls back
  to `.\scripts\wheelhouse.ps1` by itself; if it says so, that is expected, not an error.
- PowerShell 5.1 strips double quotes from arguments to native programs: a container command
  that needs quoting goes in a script file, not inline through `dev.ps1`.
- **Log every command** you execute in `docs/commands.md`, under a dated session heading.
- **Track every task** in `docs/task_tracker.md`: done, in progress, deferred (with why).
- **Never `git add` / `commit` / `push`** unless told to in that message.
- Layout is `py_src/icefleet`, `py_test`, `docs`, `scripts`, `deploy`. Tooling paths in
  `pyproject.toml` point there; keep them pointed there.
- **Deployment target is Kubernetes with Argo Workflows** (PLAN decision 27, §3.5): one
  image, a fleet-per-workflow template first (Phase 6), a pod-per-task scheduler backend
  second (Phase 6b). The scheduler is a protocol with two backends from Phase 1 on, so
  design nothing that assumes long-lived workers only. Docker Compose is just the image's
  local smoke test.
- The base project `icetl` is the **test oracle**: a distributed answer must equal the
  single-node one. Its local checkout is `E:\workspace\Transformation_Engine_08172026`.
- The dev REST catalog is `http://localhost:8182`, MinIO `http://localhost:9100` from
  Windows (PLAN §3.4); inside the dev container they are `http://iceberg-rest:8181` and
  `http://minio:9000` (set by `deploy/dev/compose.yml`). Namespaces `nyc` and `amazon` are
  **never written to**.
- **Windows Smart App Control is in enforce mode here.** It blocks unsigned binaries it has
  no reputation for: pyarrow's Flight, Parquet and Dataset modules and `mypy.exe` among
  them, and its verdicts change over time. That is why development moved into Linux (decision 28).
  Keep `python -m <tool>` everywhere (tox does), and the control plane stays stdlib HTTP
  (PLAN decision 26). Do not try to change the policy; it is the owner's security setting.

## Commands

```powershell
.\scripts\dev.ps1 up                          # once per boot: build + start the container
.\scripts\dev.ps1 setup                       # install / refresh (uv sync; wheelhouse fallback)
.\scripts\dev.ps1 uv run pytest               # offline suite
.\scripts\dev.ps1 uv run tox                  # the full gate: lint, type, tests on the wheel, dist
.\scripts\dev.ps1 uv run pytest -m integration   # opt-in, needs the dev catalog + MinIO
.\scripts\dev.ps1 shell                       # bash in the container
```

The dev catalog stack (`iceberg-nyc`) must be running before `up`; its containers have no
restart policy, so after a Docker Desktop restart start them with `docker start`.
