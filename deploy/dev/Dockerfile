# icefleet dev container -- where every build, test and tool run happens (PLAN decision 28,
# docs/PLAN_linux_dev_environment.md). The source is NOT copied in: the repo is mounted at
# /work, the venv lives on a named volume at /opt/venv. The Phase 6 runtime image
# (deploy/Dockerfile) shares this base but is a separate file.
#
# Build and run it through scripts\dev.ps1, not by hand.

FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

# uv from Astral's image rather than pip: an image pull, not a PyPI download, so the image
# builds even when container downloads stall on this machine (tracker, Known gaps).
# Pinned to the version the project declares in its dev dependency group.
COPY --from=ghcr.io/astral-sh/uv:0.13.8 /uv /uvx /usr/local/bin/

# Every command from scripts\dev.ps1 goes through devrun, which adds the wheelhouse-mode
# settings when `setup` had to fall back to them.
COPY devrun.sh /usr/local/bin/devrun
COPY devsetup.sh /usr/local/bin/devsetup
RUN sed -i 's/\r$//' /usr/local/bin/devrun /usr/local/bin/devsetup \
 && chmod 0755 /usr/local/bin/devrun /usr/local/bin/devsetup

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_CACHE_DIR=/cache/uv \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/local/bin/python3.12 \
    TOX_WORK_DIR=/cache/tox \
    MYPY_CACHE_DIR=/cache/mypy \
    RUFF_CACHE_DIR=/cache/ruff \
    PYTEST_ADDOPTS="-o cache_dir=/cache/pytest" \
    PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /work
CMD ["sleep", "infinity"]
