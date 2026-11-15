# Linux dev environment — proposal

**Status:** **Approved and built 2026-11-15** (owner: "yes to all" on §9). It is decision 28 in
the main plan. What was built and how it differs from this proposal is in §11.
**Why:** Windows Smart App Control now blocks pyarrow's Parquet and Dataset modules, so
`icetl.sql` cannot import on Windows and the gate is red there
([task_tracker.md](task_tracker.md), Known gaps). The owner chose to develop in Linux.
**Evidence:** in a throwaway `python:3.12-slim` container the locked environment passes the
full gate offline: lint, type, 8 tests passed with none skipped, dist, in 18 s
([commands.md](commands.md), 2026-11-15).

---

## 1. The proposal in one paragraph

One **dev container**, defined in the repo, does every build, test and tool run. The source stays
where it is on `E:\`, mounted into the container, so editing still happens on Windows in VS
Code. The Linux venv lives *inside* the container on a named volume and never touches the
Windows `.venv`. The container joins the `iceberg-nyc` Docker network, so the dev catalog and
MinIO are reachable by name. A small PowerShell wrapper, `scripts\dev.ps1`, runs commands
in the container from Windows, so the daily commands keep their shape:
`.\scripts\dev.ps1 uv run tox`. This becomes **decision 28** in the plan.

## 2. Why a dev container rather than WSL2

| | Dev container *(recommended)* | WSL2 Ubuntu |
|---|---|---|
| Install on Windows | nothing — Docker Desktop is already running | a distro from the Store (`wsl --install -d Ubuntu-24.04`), a system change |
| Defined in the repo | yes, `deploy/dev/` | no, it is machine state |
| Same base as the Phase 6 image | yes, `python:3.12-slim` for both | no |
| Reaches catalog / MinIO | proven: `iceberg-rest:8181`, `minio:9000` → 200 | untested; Docker's ports from a WSL distro need checking |
| Internet downloads | **broken at the moment** (§5), worked around | unknown, possibly unaffected (different network path) |
| Claude Code's commands | through `scripts\dev.ps1` | through `wsl -d Ubuntu -- …` |

WSL2's only possible advantage is the download problem in §5, which isn't proven. The
container wins on everything that is.

## 3. What gets built

| File | Holds |
|---|---|
| `deploy/dev/Dockerfile` | `FROM python:3.12-slim` (pinned by digest); `git`, `curl`; `uv==0.13.8` installed into the image's Python; workdir `/work`; no source copied in (it is mounted). |
| `deploy/dev/compose.yml` | one service `dev`, `sleep infinity`, kept running between commands. Mounts: repo → `/work`; named volumes `icefleet-dev-venv` → `/opt/venv`, `icefleet-dev-cache` → `/cache`; `.wheelhouse/` → `/wheelhouse` (read-only). Joins the external network `iceberg-nyc_iceberg_net`. Environment in §4. |
| `scripts/dev.ps1` | `up` (build + start), `down`, `setup` (create or refresh the venv, §5), `shell`, and anything else is run in the container: `.\scripts\dev.ps1 uv run pytest -q` → `docker compose -f deploy/dev/compose.yml exec dev uv run pytest -q`. |
| `scripts/wheelhouse.ps1` | the §5 fallback: on Windows, `uv export` → `uv pip compile --python-platform x86_64-manylinux_2_28` → `pip download` into `.wheelhouse/`. This is exactly what step 1 ran. |
| `.gitattributes` | `* text=auto`, `*.sh text eol=lf`, so scripts mounted into Linux keep LF endings (git already warns about CRLF on `commands.md`). |
| `.gitignore` | add `.wheelhouse/`. |
| `tox.ini` | `[tox] work_dir = {env:TOX_WORK_DIR:{tox_root}/.tox}`, so Linux tox envs go to `/cache/tox` and never share the Windows `.tox/`. Nothing else changes. |

## 4. Environment inside the container

| Variable | Value | Why |
|---|---|---|
| `UV_PROJECT_ENVIRONMENT` | `/opt/venv` | the Linux venv, on its own volume; the Windows `.venv` on the mount is never touched |
| `UV_CACHE_DIR` | `/cache/uv` | persists across container rebuilds |
| `UV_LINK_MODE` | `copy` | the cache and venv are on different volumes |
| `TOX_WORK_DIR` | `/cache/tox` | see `tox.ini` above |
| `MYPY_CACHE_DIR`, `RUFF_CACHE_DIR` | `/cache/mypy`, `/cache/ruff` | Linux and Windows caches kept apart |
| `ICETL_CATALOG_URI` | `http://iceberg-rest:8181` | the catalog by its name on the `iceberg-nyc` network |
| `ICETL_S3_ENDPOINT` | `http://minio:9000` | MinIO likewise |

Credentials still come from the repo's `.env`, which icetl reads from `/work`. The two host
names above override it because process env outranks `.env` (`.env.example`, precedence).
So `.env` stays as it is, and the same file serves Windows tools that point at `localhost`.

## 5. Installing dependencies while container downloads are broken

Containers on this machine cannot currently download large files. Small requests work, but
transfers stall at 0.1–0.3 MB in 20 s, while the host gets ~14.5 MB/s. It isn't a proxy and it
isn't the MTU; the cause is somewhere inside Docker Desktop's networking. So `scripts\dev.ps1 setup`
works in two modes:

1. **Online (normal):** `uv sync --all-extras --frozen` in the container. This is the
   project's existing rule, unchanged.
2. **Wheelhouse (fallback, what step 1 proved):** `scripts\wheelhouse.ps1` on Windows, then in
   the container `uv pip install --offline --no-index --find-links /wheelhouse` of the exported
   lock plus `--no-deps -e .`. `UV_NO_SYNC=1` is set for this mode so `uv run` never tries to
   fetch. It is the same locked versions with no drift (checked in step 1), installed by a
   different command.

`setup` tries online first with a short timeout and falls back on its own, printing which
mode it used. Fixing Docker's networking makes the fallback unused, not wrong.

## 6. Rule changes (CLAUDE.md, workflow file, README)

| Today | Becomes |
|---|---|
| Everything runs inside `.venv` through `uv` | Everything runs **inside the dev container** through `uv` (`.\scripts\dev.ps1 …`). The Windows `.venv` stays only so VS Code's Python analysis works; it is not the gate. |
| Run every tool as `python -m <tool>` (Smart App Control) | kept: harmless on Linux, and still needed for anything run on Windows |
| Commands block: `uv sync`, `uv run tox`, … | the same commands behind `.\scripts\dev.ps1`, plus `up`/`setup` once per boot |
| Log every command | unchanged; container commands are logged as run (`.\scripts\dev.ps1 uv run tox`) |
| Phase 0 "done when": green from a fresh clone | green **in the dev container** from a fresh clone, following only `commands.md` |

Decision 26 (HTTP control plane, no Flight) **stays**: on Linux Flight would load, but the
design should not depend on where the coordinator runs, and HTTP is already decided.

## 7. Decision 28 (to add to the plan's §2, dated, not an edit)

> **28. Development runs in a Linux dev container** *(2026-11-15, after Smart App Control
> began blocking pyarrow's Parquet and Dataset modules on the dev machine)* —
> `deploy/dev/` defines it, `scripts\dev.ps1` drives it from Windows, the repo is mounted and
> the venv is container-local. The gate is authoritative only there. The container joins the
> dev catalog's Docker network. The Phase 6 runtime image (`deploy/Dockerfile`) shares its
> base but is a separate file.

## 8. How it is verified

1. `.\scripts\dev.ps1 up; .\scripts\dev.ps1 setup` from a clean state (no volumes).
2. `.\scripts\dev.ps1 uv run tox`: lint, type, py312 (8 passed, 0 skipped), dist, all green.
3. `.\scripts\dev.ps1 uv run python -c "<list namespaces through icetl's catalog>"` shows `nyc`.
   This is read-only and writes to no namespace.
4. On Windows, the `.venv` and `.tox/` are untouched (mtimes unchanged).
5. Tracker and `commands.md` updated; then Phase 0 goes back to the owner for confirmation.

## 9. Questions for the owner (recommended answer first)

1. **Dev container, not WSL2?** Recommended: **yes** (§2).
2. **May I restart Docker Desktop once to see if the download stall clears?** This briefly
   stops your `iceberg-nyc` containers and the `temporal-poc` kind cluster; they come back if
   their restart policy says so. Recommended: **yes, at a time that suits you**. The design
   works either way (§5).
3. **Keep the Windows `.venv` for the editor?** Recommended: **yes**. Deleting it saves disk
   but loses VS Code's code completion.
4. **Add a `.devcontainer/devcontainer.json`** so VS Code can open the project *inside* the
   container? That needs Microsoft's Dev Containers extension, which isn't installed.
   Recommended: **not now**, and revisit once this setup has been stable for a phase.
5. **Reach the catalog by Docker network names** (`iceberg-rest`, `minio`) rather than
   `host.docker.internal`? Recommended: **network names**. They are proven, and it's the same
   pattern Phase 6 uses in-cluster. The cost: the container depends on the network name
   `iceberg-nyc_iceberg_net`, which is a setting in `compose.yml`.

## 10. Out of scope

- The Phase 6 runtime image, Kubernetes manifests, Argo: unchanged, still Phase 6.
- Fixing Docker Desktop's networking: diagnosed in the tracker, the owner's machine.
- Any Phase 1 work.

---

**Side note (no action now):** `docker ps` shows a `kind` cluster, `temporal-poc`, publishing
port 2746, which is Argo Server's port. The tracker plans to use Docker Desktop's built-in
Kubernetes for Phase 6. If that cluster already runs Argo, it may be a quicker Phase 6
target. That is the owner's call, when Phase 6 comes.

---

## 11. Outcome (2026-11-15)

**Answers (§9):** all five as recommended. These were: dev container; restart Docker Desktop once;
keep the Windows `.venv`; no `.devcontainer` for now; reach the catalog by network names.

**Built:** `deploy/dev/{Dockerfile,compose.yml,devrun.sh,devsetup.sh}`,
`scripts/{dev.ps1,wheelhouse.ps1}`, `.gitattributes`, `.wheelhouse/` in `.gitignore`,
`work_dir` in `tox.ini`; CLAUDE.md, the workflow file, the README and the main plan (decision
28, §5 layout) updated.

**Where the build differs from the proposal:**

| Proposal | Built | Why |
|---|---|---|
| `uv==0.13.8` installed into the image's Python | copied from `ghcr.io/astral-sh/uv:0.13.8` | an image pull, which kept working while in-container downloads stalled; the image build never touches PyPI |
| `git`, `curl` in the image | left out | nothing needs them yet; `apt-get` would be another in-container download |
| wheelhouse-mode env set by the wrapper | `/opt/venv/.dev-env`, sourced by `devrun` before every command; online `setup` deletes it | no extra `docker exec` per command, and tox gets the offline settings too |
| setup logic inline in `dev.ps1` | `devsetup.sh` baked into the image | PowerShell 5.1 mangles double quotes in native-command arguments |
| wheelhouse: locked wheels + hatchling | + `editables` | the editable build of icefleet needs it; the first fallback run failed without it |
| pytest cache on the mount | `PYTEST_ADDOPTS=-o cache_dir=/cache/pytest` | keeps Linux and Windows caches apart, like mypy's and ruff's |

**The Docker Desktop restart:** it cleared the stall *at first*: 12 MB/s from a container.
Within the hour the stall was back **intermittently**: two of three 17 MB downloads at
~12 MB/s, one timed out after 0.1 MB. So the first `setup` (auto) fell back to the
wheelhouse by itself, as designed. The restart is not a fix; the fallback is needed.

**Verification (§8):**

1. From a clean state (no volumes): `up` 14 s; `setup` auto → online failed → wheelhouse →
   the missing `editables`, fixed → `setup wheelhouse` 46 s; then `setup online` 1 s
   (nothing to fetch). Both modes proven.
2. `uv run tox` green in both modes: wheelhouse 89 s (cold mypy cache), online 11 s.
   pytest: 8 passed, 0 skipped. tox envs are in `/cache/tox`.
3. Catalog through icetl, read-only, from the container: `ICETL_CATALOG_URI=http://iceberg-rest:8181`
   reached it; namespaces `nyc`, `temporal_poc`; table `nyc.yellow_tripdata`; a 5-row read
   returned 5 rows from MinIO.
4. Windows `.venv`, `.venv\Lib\site-packages`, `.tox`, `.tox\py312` mtimes unchanged;
   `.tox` still 21752 files.

**Left as is:** Windows PowerShell's machine execution policy is undefined, so scripts are
blocked by default in a plain terminal. The README gives the `-ExecutionPolicy Bypass -File` form;
changing the policy is the owner's call.
