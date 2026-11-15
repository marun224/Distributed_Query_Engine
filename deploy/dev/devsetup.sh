#!/bin/sh
# Create or refresh the Linux venv at /opt/venv. Called by `scripts\dev.ps1 setup`.
#
#   devsetup online      uv sync --all-extras -- the project's normal rule
#   devsetup wheelhouse  the same locked versions, installed offline from /wheelhouse, which
#                        scripts\wheelhouse.ps1 fills on Windows. For when containers cannot
#                        download (docs/PLAN_linux_dev_environment.md §5).
set -eu
mode="${1:-online}"
cd /work

case "$mode" in
  online)
    rm -f /opt/venv/.dev-env
    UV_HTTP_TIMEOUT="${UV_HTTP_TIMEOUT:-60}" uv sync --all-extras
    ;;
  wheelhouse)
    if [ ! -f /wheelhouse/requirements-linux.txt ]; then
      echo "devsetup: /wheelhouse is empty -- run scripts\\wheelhouse.ps1 on Windows first" >&2
      exit 2
    fi
    [ -f /opt/venv/pyvenv.cfg ] || uv venv --allow-existing /opt/venv
    uv pip sync --offline --no-index --find-links /wheelhouse --python /opt/venv/bin/python \
      /wheelhouse/requirements-linux.txt
    uv pip install --offline --no-index --find-links /wheelhouse --python /opt/venv/bin/python \
      --no-deps -e /work
    # Read by devrun before every command: stay offline, use the wheelhouse, never re-sync.
    cat > /opt/venv/.dev-env <<'EOF'
export UV_NO_SYNC=true UV_OFFLINE=true UV_NO_INDEX=true UV_FIND_LINKS=/wheelhouse
export PIP_NO_INDEX=1 PIP_FIND_LINKS=/wheelhouse
EOF
    ;;
  *)
    echo "devsetup: mode must be 'online' or 'wheelhouse', not '$mode'" >&2
    exit 2
    ;;
esac

python - "$mode" <<'EOF'
import sys, duckdb, icetl, pyarrow, pyarrow.parquet  # noqa: F401 -- parquet is what Windows blocked
print(f"devsetup: {sys.argv[1]} mode done -- icetl {icetl.__version__}, "
      f"duckdb {duckdb.__version__}, pyarrow {pyarrow.__version__}")
EOF
