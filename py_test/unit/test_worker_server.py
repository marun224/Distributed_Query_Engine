"""The worker's HTTP surface, in-process: routes, refusals, a real task, cancellation."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest
from icetl.conf import EngineSettings, IcetlSettings
from icetl.exec.engine import DuckDBEngine

from icefleet.plan.tasks import InputSpec, SinkSpec, TaskResult, TaskSpec
from icefleet.worker.cli import EXIT_NOT_IMPLEMENTED, main
from icefleet.worker.runner import TaskRunner
from icefleet.worker.server import WorkerServer


@pytest.fixture
def worker(tmp_path: Path) -> Iterator[WorkerServer]:
    engine = DuckDBEngine(
        IcetlSettings(engine=EngineSettings(temp_directory=str(tmp_path / "spill")))
    )
    server = WorkerServer(("127.0.0.1", 0), TaskRunner(engine, "test-worker"), slots=1)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()
    server.server_close()
    engine.close()


def call(server: WorkerServer, method: str, path: str, body: Any = None) -> tuple[int, Any]:
    data = (
        None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    )
    request = urllib.request.Request(f"http://{server.endpoint}{path}", data=data, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


@pytest.fixture
def data_file(tmp_path: Path) -> str:
    path = (tmp_path / "in.parquet").as_posix()
    rows = "SELECT range AS id, 'r' || range AS name FROM range(100)"
    duckdb.connect().execute(f"COPY ({rows}) TO '{path}' (FORMAT parquet)")
    return path


def task(sql: str, paths: tuple[str, ...], location: Path, query_id: str = "q-1") -> TaskSpec:
    return TaskSpec(
        query_id=query_id,
        stage_id=0,
        task_index=0,
        sql=sql,
        inputs={"p": InputSpec(paths=paths)},
        sink=SinkSpec(kind="result", location=location.as_posix()),
    )


def test_ping_reports_versions_and_slots(worker: WorkerServer) -> None:
    status, body = call(worker, "GET", "/ping")
    assert status == 200
    assert body["slots"] == 1 and body["busy"] == 0
    assert {"protocol", "icefleet", "duckdb", "icetl", "pyarrow"} <= set(body)


def test_a_task_runs_and_reports_its_manifest(
    worker: WorkerServer, data_file: str, tmp_path: Path
) -> None:
    spec = task(
        "SELECT * FROM read_parquet($p) WHERE id % 2 = 0", (data_file,), tmp_path / "o" / "t0"
    )
    status, body = call(worker, "POST", "/tasks", spec.to_json())
    assert status == 200, body
    result = TaskResult.from_json(body)
    assert result.rows_out == 50 and result.worker == "test-worker"
    assert all(Path(f.path).exists() for f in result.manifest)


def test_a_task_with_no_rows_still_writes_a_typed_file(
    worker: WorkerServer, data_file: str, tmp_path: Path
) -> None:
    spec = task("SELECT * FROM read_parquet($p) WHERE false", (data_file,), tmp_path / "o" / "t0")
    status, body = call(worker, "POST", "/tasks", spec.to_json())
    assert status == 200
    result = TaskResult.from_json(body)
    assert result.rows_out == 0 and len(result.manifest) == 1
    columns = (
        duckdb.connect().execute(f"DESCRIBE SELECT * FROM '{result.manifest[0].path}'").fetchall()
    )
    assert [c[0] for c in columns] == ["id", "name"]


def test_failures_and_refusals(worker: WorkerServer, data_file: str, tmp_path: Path) -> None:
    assert call(worker, "GET", "/nope")[0] == 404
    assert call(worker, "POST", "/tasks", b"not json")[0] == 400
    assert call(worker, "POST", "/tasks", {"query_id": "q"})[0] == 400

    status, body = call(
        worker,
        "POST",
        "/tasks",
        task("SELECT nope FROM read_parquet($p)", (data_file,), tmp_path / "a").to_json(),
    )
    assert status == 500 and body["type"] == "TaskFailed" and "nope" in body["error"]

    assert worker.try_acquire()  # occupy the only slot
    try:
        spec = task("SELECT 1", (data_file,), tmp_path / "b")
        assert call(worker, "POST", "/tasks", spec.to_json())[0] == 503
    finally:
        worker.release()


def test_a_cancelled_query_runs_no_more_tasks(
    worker: WorkerServer, data_file: str, tmp_path: Path
) -> None:
    assert call(worker, "POST", "/cancel", {"query_id": "q-gone"}) == (200, {"cancelled": 0})
    spec = task("SELECT * FROM read_parquet($p)", (data_file,), tmp_path / "c", query_id="q-gone")
    status, body = call(worker, "POST", "/tasks", spec.to_json())
    assert status == 409 and body["type"] == "TaskCancelled"


def test_run_task_is_phase_6b(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run-task", "s3://bucket/spec.json"]) == EXIT_NOT_IMPLEMENTED
    assert "Phase 6b" in capsys.readouterr().err


def test_a_shuffle_task_writes_one_directory_per_partition(
    worker: WorkerServer, data_file: str, tmp_path: Path
) -> None:
    sql = "SELECT id, name, hash(id) % 4 AS __part FROM read_parquet($p)"
    spec = TaskSpec(
        query_id="q-s",
        stage_id=0,
        task_index=0,
        sql=sql,
        inputs={"p": InputSpec(paths=(data_file,))},
        sink=SinkSpec(
            kind="shuffle", location=(tmp_path / "out" / "task-0").as_posix(), partitions=4
        ),
    )
    status, body = call(worker, "POST", "/tasks", spec.to_json())
    assert status == 200, body
    result = TaskResult.from_json(body)
    assert sum(f.rows for f in result.manifest) == 100
    assert {f.partition for f in result.manifest} <= {0, 1, 2, 3}
    for output in result.manifest:
        assert f"__part={output.partition}/" in output.path.replace("\\", "/")
        # The partition column is not written into the file. (Read without hive
        # partitioning, as the final stage does: DuckDB would infer it from the path.)
        read = f"read_parquet('{output.path}', hive_partitioning => false)"
        columns = [
            c[0] for c in duckdb.connect().execute(f"DESCRIBE SELECT * FROM {read}").fetchall()
        ]
        assert columns == ["id", "name"]
        where = f"hash(id) % 4 = {output.partition}"
        expected = (
            duckdb.connect()
            .execute(f"SELECT count(*) FROM read_parquet('{data_file}') WHERE {where}")
            .fetchone()
        )
        assert expected is not None and output.rows == expected[0]


def test_a_shuffle_task_with_no_rows_reports_no_files(
    worker: WorkerServer, data_file: str, tmp_path: Path
) -> None:
    for partitions in (1, 4):
        sql = "SELECT id, hash(id) % 4 AS __part FROM read_parquet($p) WHERE id < 0"
        if partitions == 1:
            sql = "SELECT id FROM read_parquet($p) WHERE id < 0"
        spec = TaskSpec(
            query_id="q-e",
            stage_id=0,
            task_index=partitions,
            sql=sql,
            inputs={"p": InputSpec(paths=(data_file,))},
            sink=SinkSpec(
                kind="shuffle",
                location=(tmp_path / f"task-{partitions}").as_posix(),
                partitions=partitions,
            ),
        )
        status, body = call(worker, "POST", "/tasks", spec.to_json())
        assert status == 200, body
        assert TaskResult.from_json(body).manifest == ()


def test_a_single_partition_shuffle_is_partition_zero(
    worker: WorkerServer, data_file: str, tmp_path: Path
) -> None:
    spec = TaskSpec(
        query_id="q-1p",
        stage_id=0,
        task_index=0,
        sql="SELECT count(*) AS n FROM read_parquet($p)",
        inputs={"p": InputSpec(paths=(data_file,))},
        sink=SinkSpec(kind="shuffle", location=(tmp_path / "one").as_posix(), partitions=1),
    )
    status, body = call(worker, "POST", "/tasks", spec.to_json())
    assert status == 200, body
    manifest = TaskResult.from_json(body).manifest
    assert len(manifest) == 1 and manifest[0].partition == 0 and manifest[0].rows == 1
