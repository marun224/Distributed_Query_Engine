"""The worker sizes S3 upload parts once httpfs is loaded (Phase 6, the Compose smoke test)."""

from __future__ import annotations

from icefleet.worker.runner import S3_MAX_FILE_SIZE, TaskRunner


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)


class _Engine:
    def __init__(self) -> None:
        self.connection = _Connection()
        self.loaded: list[list[str]] = []

    def ensure_object_store(self, locations: list[str]) -> None:
        self.loaded.append(locations)


def _runner() -> tuple[TaskRunner, _Engine]:
    engine = _Engine()
    runner = TaskRunner(engine, "w")  # type: ignore[arg-type]
    return runner, engine


def test_object_store_locations_size_s3_parts_once() -> None:
    runner, engine = _runner()
    runner._object_store(["s3://bucket/in/a.parquet", "s3://bucket/out"])
    runner._object_store(["s3://bucket/in/b.parquet"])
    tuned = [s for s in engine.connection.statements if "s3_uploader_max_filesize" in s]
    assert tuned == [f"SET GLOBAL s3_uploader_max_filesize = '{S3_MAX_FILE_SIZE}'"]
    assert len(engine.loaded) == 2  # icetl's loader is still asked every time (idempotent)


def test_local_paths_leave_duckdb_alone() -> None:
    runner, engine = _runner()
    runner._object_store(["/tmp/exchange/a.parquet", "/tmp/exchange/out"])
    assert engine.connection.statements == []


def test_the_part_size_is_s3s_minimum() -> None:
    from icefleet.config import parse_bytes

    part = parse_bytes(S3_MAX_FILE_SIZE) / 10_000  # httpfs: max file size / 10,000 parts
    assert 5_000_000 <= part < 6_000_000
