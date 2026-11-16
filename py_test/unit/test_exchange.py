from __future__ import annotations

from pathlib import Path

from icefleet.exchange import open_exchange
from icefleet.exchange.local import LocalDirExchange
from icefleet.exchange.object_store import ObjectStoreExchange


def test_local_layout_and_idempotent_delete(tmp_path: Path) -> None:
    exchange = open_exchange(str(tmp_path / "ex"))
    assert isinstance(exchange, LocalDirExchange)
    task = exchange.task_location("q-1", 0, 3)
    assert task == f"{(tmp_path / 'ex').as_posix()}/q-1/stage-0/task-3"
    Path(task).mkdir(parents=True)
    (Path(task) / "data_0.parquet").write_bytes(b"x")
    assert exchange.exists(task)
    exchange.delete_query("q-1")
    assert not exchange.exists(exchange.query_prefix("q-1"))
    exchange.delete_query("q-1")  # again: no error


def test_file_uri_is_a_local_directory(tmp_path: Path) -> None:
    exchange = open_exchange(f"file://{(tmp_path / 'ex').as_posix()}")
    assert isinstance(exchange, LocalDirExchange)
    assert exchange.location == (tmp_path / "ex").as_posix()


def test_s3_locations_open_the_object_store() -> None:
    exchange = open_exchange("s3a://shuffle/icefleet/")
    assert isinstance(exchange, ObjectStoreExchange)
    assert exchange.location == "s3://shuffle/icefleet"
    assert exchange.bucket == "shuffle"
    assert exchange.task_location("q-9", 1, 0) == "s3://shuffle/icefleet/q-9/stage-1/task-0"
