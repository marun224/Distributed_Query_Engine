# Project Workflow

How work runs in this repo — for Claude, and for anyone picking the project up later.
Project-agnostic in its body; project-specific notes are at the end.

## Ground rules

| Rule | Detail |
|---|---|
| Plan before building | Nothing is implemented until a plan file exists and is agreed. |
| Ask before consequential actions | Writing outside the project, installing globally, publishing, or anything touching git needs approval first. Ordinary edits and test runs inside the project do not. |
| Never commit unasked | No `add`, `commit`, `push`, or tag without a direct instruction. |
| Log every command | Append each executed command to [commands.md](commands.md). |
| Test in the sandbox | Trial runs and scratch output go to the session scratch directory, never into the repo. |
| PowerShell | Commands are written for PowerShell on Windows. |

## Setup

Python 3.12, `uv` as package manager, everything inside the virtual environment.

```powershell
uv venv --python 3.12
.venv\Scripts\activate
uv pip install uv
uv sync --all-extras
```

- Every dependency is declared in `pyproject.toml`. Never install ad hoc.
- `ipykernel` is a declared dependency.
- Nothing is installed into the system Python.

## Layout

| Path | Holds |
|---|---|
| `py_src/` | All source code. |
| `py_test/` | All test cases. |
| `docs/` | Plan, task tracker, this file, command log. |

Point the tooling at these paths, or the gate silently checks nothing: package path in
`pyproject.toml`, `testpaths` for pytest, `files` for mypy, `src` for ruff.

## Validation gate

`tox.ini` defines the gate. It must be green before a phase is called done.

```powershell
uv run tox            # everything
uv run tox -e lint    # ruff check + ruff format --check
uv run tox -e type    # mypy
uv run tox -e py312   # pytest
```

## Planning

Work is written to a plan file before implementation starts.

- Name it for the work, not generically — `PLAN_<topic>.md`.
- Split into **phases**, each finishable in a single sitting.
- Each phase states: goal, files touched, how it is verified, what "done" means.
- Phases are executed on different days. Assume the context is lost between them.

## Resuming

Status lives in [task_tracker.md](task_tracker.md), not in the plan. The plan file holds
the **design**; the tracker holds the **state**.

Without reading any code, the tracker must answer:

1. Which phases are **done**, and on what date.
2. Which phase is **next**.
3. What is in progress, and any known gaps, blockers or open decisions.

Update the tracker at the end of every session — before stopping, not afterwards. It is
the first file to read when picking the project back up.

## Daily loop

1. Read [task_tracker.md](task_tracker.md). Confirm the next phase.
2. Read that phase in the plan file. Implement it.
3. Run the gate until green.
4. Update the tracker: mark the phase done, record anything discovered or newly blocked.
5. Check commands are logged. Stop. Do not commit unless asked.

---

## Project notes — icefleet

- **Phase gate is the owner's.** After step 4 of the daily loop, ask the owner to confirm
  the phase. The next phase starts only after that confirmation, even if context remains.
- **Ask, then plan, then build.** Questions that change the design are asked up front, as a
  single numbered list in plain text (not one at a time), with a recommended answer each.
- **Work runs in a Linux dev container** (PLAN decision 28,
  [PLAN_linux_dev_environment.md](PLAN_linux_dev_environment.md)), because Windows Smart App
  Control blocks pyarrow modules icetl needs. The setup block above is replaced by:

  ```powershell
  .\scripts\dev.ps1 up        # build + start (needs the iceberg-nyc catalog stack running)
  .\scripts\dev.ps1 setup     # uv sync in the container; falls back to the wheelhouse
  .\scripts\dev.ps1 uv run tox
  ```

  Every gate command above runs the same way, prefixed with `.\scripts\dev.ps1`. The venv is
  `/opt/venv` inside the container, not `.venv`. A Windows `.venv` (created as below) is kept
  only for the editor's code analysis; there is no global `uv` on the machine:

  ```powershell
  py -3.12 -m venv .venv
  .venv\Scripts\python -m pip install --upgrade pip uv
  .venv\Scripts\uv.exe sync --all-extras
  ```

  `uv` is also declared in the `dev` dependency group, pinned, so an exact sync never
  removes the binary that is running it. `scripts\wheelhouse.ps1` uses this `.venv`'s uv and
  pip to download Linux wheels when the container cannot.
- **The oracle.** `icetl` (base project, local checkout `E:\workspace\Transformation_Engine_08172026`)
  gives the reference answer for every distributed test.
- **Dev catalog.** REST `http://localhost:8182`, MinIO `http://localhost:9100`; see the
  plan's §3.4. `nyc` and `amazon` are protected; tests write only to `icefleet_it`.
- **Extra layout.** `scripts/` (dev container driver now; proof-of-life, benchmark later)
  and `deploy/` (`deploy/dev/` now; the runtime image, Compose, Kubernetes, Argo from Phase 6)
  join `py_src/`, `py_test/`, `docs/`.
