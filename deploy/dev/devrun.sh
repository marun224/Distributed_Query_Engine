#!/bin/sh
# Runs one command in the dev container. If `scripts\dev.ps1 setup` installed from the
# wheelhouse (container downloads unavailable), it left /opt/venv/.dev-env behind: uv and
# pip are then told to stay offline and use /wheelhouse, and `uv run` not to re-sync.
# An online `setup` removes that file again.
set -e
if [ -f /opt/venv/.dev-env ]; then
    . /opt/venv/.dev-env
fi
exec "$@"
