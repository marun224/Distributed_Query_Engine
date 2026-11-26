# icefleet runtime image -- one image, every role (PLAN §3.5, decision 27):
#   icefleet-worker serve ...        a worker (the default command)
#   python /scripts/<job>.py         the coordinator: a script using ClusterSession
#
# Built OFFLINE (Phase 6 kickoff answer 6): container downloads are unreliable on the dev
# machine, so nothing here reaches the network. Python packages are the exact locked
# wheels from the wheelhouse (scripts\wheelhouse.ps1), and DuckDB's httpfs extension is
# a file downloaded on Windows. Build it with scripts\image.ps1, not by hand:
#
#   docker build -f deploy/Dockerfile \
#     --build-context wheels=.wheelhouse --build-context stage=.scratch/image \
#     -t icefleet:dev deploy/image
#
#   wheels  the wheelhouse: every locked Linux wheel
#   stage   icefleet's own wheel, requirements-runtime.txt (uv export, no dev / extras)
#           and httpfs.duckdb_extension.gz for the locked DuckDB version
#
# The base is the dev container's, pinned by digest (decision 28), so the image runs the
# same Python the gate ran.

ARG BASE=python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

FROM ${BASE} AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.8 /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1
RUN --mount=type=bind,from=wheels,target=/wheels \
    --mount=type=bind,from=stage,target=/stage \
    uv venv /opt/icefleet --python /usr/local/bin/python3.12 \
 && uv pip install --python /opt/icefleet/bin/python --offline --no-index \
      --find-links /wheels -r /stage/requirements-runtime.txt \
 && uv pip install --python /opt/icefleet/bin/python --offline --no-index \
      --find-links /wheels --no-deps /stage/icefleet-*.whl

FROM ${BASE} AS runtime
ARG VERSION=unknown
ARG REVISION=unknown
LABEL org.opencontainers.image.title="icefleet" \
      org.opencontainers.image.description="Distributed SQL and DataFrame engine over Apache Iceberg on DuckDB workers" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="Apache-2.0"

# Non-root. uid 10001 is what the Kubernetes manifests set as runAsUser.
RUN groupadd --gid 10001 icefleet \
 && useradd --uid 10001 --gid 10001 --create-home --home-dir /home/icefleet \
      --shell /usr/sbin/nologin icefleet \
 && mkdir -p /spill \
 && chown 10001:10001 /spill

COPY --from=build /opt/icefleet /opt/icefleet
COPY install_httpfs.py check_image.py /opt/icefleet-image/

ENV PATH=/opt/icefleet/bin:$PATH \
    HOME=/home/icefleet \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ICETL_DUCKDB_TEMP_DIRECTORY=/spill

USER 10001:10001
WORKDIR /home/icefleet

# httpfs into the runtime user's DuckDB extension directory, from the staged file: no pod
# ever reaches for the extension repository (PLAN §3.5). INSTALL at run time then finds
# it in place and does nothing.
RUN --mount=type=bind,from=stage,target=/stage \
    python /opt/icefleet-image/install_httpfs.py /stage/httpfs.duckdb_extension.gz

EXPOSE 8815
ENTRYPOINT ["icefleet-worker"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8815"]

# --- icefleet-test: the runtime image plus pytest and the tests (scripts\image.ps1 -Test),
# so the integration tier runs as the coordinator step of an Argo fleet (kickoff answer 9:
# `.\scripts\k8s.ps1 run integration.py image=icefleet-test:dev`). Never the image workers
# or jobs run in production; the default build target stays `runtime`.
FROM build AS test-build
RUN --mount=type=bind,from=wheels,target=/wheels \
    uv pip install --python /opt/icefleet/bin/python --offline --no-index \
      --find-links /wheels pytest

FROM runtime AS test
COPY --from=test-build /opt/icefleet /opt/icefleet
COPY --from=stage --chown=10001:10001 tests /opt/icefleet-tests
ENV ICEFLEET_SCRIPTS=/opt/icefleet-tests/scripts
WORKDIR /opt/icefleet-tests
