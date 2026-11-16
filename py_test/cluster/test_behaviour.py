"""How the cluster behaves around the answer: explain, fallback, clean-up, failure."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from icetl.conf import IcetlConf, IcetlSettings
from icetl.errors import UnsupportedFeatureError
from icetl.paths import engine_path
from icetl.sql import functions as F
from pyiceberg.catalog.sql import SqlCatalog

from icefleet._versions import runtime_versions
from icefleet.cluster import WorkerClient, handshake
from icefleet.errors import ClusterConfigurationError, ClusterError, WorkerError
from icefleet.plan.tasks import InputSpec, SinkSpec, TaskSpec
from icefleet.session import ClusterSession
from py_test.conftest import with_spill


def _exchange_root(session: ClusterSession) -> Path:
    location = session.conf.get("icefleet.exchange.location")
    assert location is not None
    return Path(location)


def _leftovers(session: ClusterSession) -> list[str]:
    root = _exchange_root(session)
    return sorted(p.name for p in root.iterdir()) if root.exists() else []


# -- explain ---------------------------------------------------------------------


def test_explain_shows_the_stage_and_its_splits(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    cluster.table("fx.many").filter(F.col("qty") > 0).explain()
    out = capsys.readouterr().out
    assert out.startswith("== Distributed Plan ==\nmode: distributed")
    stage = next(line for line in out.splitlines() if line.startswith("stage 0"))
    splits = int(stage.split(" splits")[0].rsplit(" ", 1)[1])
    assert splits >= 2
    assert "8 of 8 file(s)" in stage
    assert "== Physical Plan (DuckDB SQL) ==" in out  # icetl's own explain follows, unchanged


def test_explain_says_why_a_plan_runs_single_node(
    cluster: ClusterSession, capsys: pytest.CaptureFixture[str]
) -> None:
    cluster.table("fx.many").groupBy("flag").count().explain()
    out = capsys.readouterr().out
    assert "mode: single-node -- GROUP BY / aggregation -- Phase 2" in out


def test_a_plain_icetl_session_explains_exactly_as_before(
    oracle: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    oracle.table("fx.many").explain()
    assert "Distributed Plan" not in capsys.readouterr().out


# -- fallback ----------------------------------------------------------------------


def test_fallback_error_refuses_an_uncuttable_plan(fresh_cluster: ClusterSession) -> None:
    fresh_cluster.conf.set("icefleet.fallback", "error")
    with pytest.raises(UnsupportedFeatureError, match="GROUP BY"):
        fresh_cluster.table("fx.many").groupBy("flag").count().toArrow()
    # A cuttable plan still runs.
    assert fresh_cluster.table("fx.plain").toArrow().num_rows == 5


def test_a_remote_master_without_an_exchange_location_is_refused(
    local_settings: IcetlSettings,
    catalog: SqlCatalog,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Neither the environment nor a .env may supply the location for this test.
    monkeypatch.delenv("ICEFLEET_EXCHANGE_LOCATION", raising=False)
    monkeypatch.chdir(tmp_path)
    server, endpoint = _fake_worker(runtime_versions())
    session = ClusterSession(
        settings=with_spill(local_settings, tmp_path),
        conf=IcetlConf({"icefleet.master": f"static://{endpoint}"}),
        catalog=catalog,
    )
    try:
        with pytest.raises(ClusterConfigurationError, match=r"exchange\.location"):
            session.table("fx.many").toArrow()
    finally:
        session.stop()
        server.shutdown()


# -- the exchange ----------------------------------------------------------------


def test_the_query_prefix_is_removed_afterwards(fresh_cluster: ClusterSession) -> None:
    fresh_cluster.table("fx.many").toArrow()
    run = fresh_cluster._last_run
    assert run is not None and run.mode == "distributed" and run.query_id
    assert _leftovers(fresh_cluster) == []


def test_exchange_keep_leaves_the_stage_output(fresh_cluster: ClusterSession) -> None:
    fresh_cluster.conf.set("icefleet.exchange.keep", "true")
    fresh_cluster.table("fx.many").toArrow()
    run = fresh_cluster._last_run
    assert run is not None and run.query_id
    stage = _exchange_root(fresh_cluster) / run.query_id / "stage-0"
    tasks = sorted(p.name for p in stage.iterdir())
    assert tasks == [f"task-{i}" for i in range(run.tasks)]
    assert all(any((stage / t).glob("data_*.parquet")) for t in tasks)


def test_result_size_guard_fails_cleanly(fresh_cluster: ClusterSession) -> None:
    fresh_cluster.conf.set("icefleet.result.maxBytes", "10")
    with pytest.raises(ClusterError, match=r"icefleet\.result\.maxBytes"):
        fresh_cluster.table("fx.many").toArrow()
    assert _leftovers(fresh_cluster) == []


# -- failure ---------------------------------------------------------------------


def test_a_dead_worker_fails_the_query_cleanly(fresh_cluster: ClusterSession) -> None:
    fresh_cluster.table("fx.many").toArrow()  # starts the fleet
    cluster = fresh_cluster._cluster
    assert cluster is not None
    victim = cluster.processes[0]
    victim.kill()
    victim.wait(timeout=10)

    with pytest.raises(WorkerError, match="lost contact"):
        fresh_cluster.table("fx.many").toArrow()
    assert _leftovers(fresh_cluster) == []
    # The session itself is still usable for what does not need the fleet.
    assert fresh_cluster.table("fx.many").groupBy("flag").count().toArrow().num_rows == 3


def test_a_task_error_names_the_worker_and_the_task(
    fresh_cluster: ClusterSession, catalog: SqlCatalog, tmp_path: Path
) -> None:
    worker = fresh_cluster._ensure_cluster().workers[0]
    files = [
        engine_path(t.file.file_path) for t in catalog.load_table("fx.plain").scan().plan_files()
    ]
    spec = TaskSpec(
        query_id="q-test",
        stage_id=0,
        task_index=7,
        sql="SELECT error('boom from the task') AS x FROM read_parquet($p)",
        inputs={"p": InputSpec(paths=tuple(files))},
        sink=SinkSpec(kind="result", location=str(tmp_path / "out" / "task-7")),
    )
    with pytest.raises(WorkerError, match="boom from the task") as caught:
        worker.client.run_task(spec)
    assert caught.value.worker == worker.endpoint
    assert caught.value.task == "q-test/stage-0/task-7"


# -- the handshake -----------------------------------------------------------------


def _fake_worker(versions: dict[str, str]) -> tuple[ThreadingHTTPServer, str]:
    """An HTTP server that answers /ping with `versions` and nothing else."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def do_GET(self) -> None:
            body = json.dumps({**versions, "slots": 2}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"127.0.0.1:{server.server_address[1]}"


@pytest.fixture
def stale_worker() -> Iterator[str]:
    server, endpoint = _fake_worker({**runtime_versions(), "duckdb": "0.9.2"})
    yield endpoint
    server.shutdown()


def test_handshake_refuses_a_different_duckdb(stale_worker: str) -> None:
    with pytest.raises(ClusterError, match=r"duckdb .* here, 0\.9\.2 there"):
        handshake(WorkerClient(stale_worker))


def test_a_session_on_a_mismatched_fleet_refuses_to_run(
    stale_worker: str, local_settings: IcetlSettings, catalog: SqlCatalog, tmp_path: Path
) -> None:
    session = ClusterSession(
        settings=with_spill(local_settings, tmp_path),
        conf=IcetlConf(
            {
                "icefleet.master": f"static://{stale_worker}",
                "icefleet.exchange.location": str(tmp_path / "exchange"),
            }
        ),
        catalog=catalog,
    )
    try:
        with pytest.raises(ClusterError, match="version mismatch"):
            session.table("fx.many").toArrow()
    finally:
        session.stop()


def test_handshake_accepts_a_matching_worker() -> None:
    server, endpoint = _fake_worker(runtime_versions())
    try:
        info = handshake(WorkerClient(endpoint))
        assert info.slots == 2 and info.endpoint == endpoint
    finally:
        server.shutdown()
