from __future__ import annotations

import json

import pytest

from icefleet.plan.tasks import InputSpec, OutputFile, SinkSpec, TaskResult, TaskSpec


def spec() -> TaskSpec:
    return TaskSpec(
        query_id="q-1",
        stage_id=0,
        task_index=3,
        sql="SELECT * FROM read_parquet($p)",
        inputs={
            "p": InputSpec(paths=("/a.parquet", "s3://b/c.parquet"), schema_ipc=b"\x00\xffipc")
        },
        sink=SinkSpec(kind="result", location="/x/q-1/stage-0/task-3", target_file_bytes=1024),
        attempt=1,
    )


def test_task_spec_round_trips_through_json_text() -> None:
    original = spec()
    assert TaskSpec.from_json(json.loads(json.dumps(original.to_json()))) == original
    assert original.key == "q-1/stage-0/task-3"


def test_bytes_travel_as_base64() -> None:
    encoded = spec().to_json()["inputs"]["p"]["schema_ipc"]
    assert encoded == "AP9pcGM="


def test_task_result_round_trips_and_sums() -> None:
    result = TaskResult(
        query_id="q-1",
        stage_id=0,
        task_index=3,
        attempt=0,
        worker="w:1",
        seconds=0.5,
        manifest=(OutputFile("/a", 10, 2), OutputFile("/b", 5, 1)),
    )
    payload = json.loads(json.dumps(result.to_json()))
    assert payload["rows_out"] == 3 and payload["bytes_out"] == 15
    assert TaskResult.from_json(payload) == result


def test_a_shuffle_sink_and_partitioned_files_round_trip() -> None:
    sink = SinkSpec(kind="shuffle", location="/x/q-1/stage-0/task-3", partitions=64)
    assert SinkSpec.from_json(json.loads(json.dumps(sink.to_json()))) == sink
    assert SinkSpec.from_json({"kind": "result", "location": "/x"}).partitions == 1
    result = TaskResult(
        query_id="q-1",
        stage_id=0,
        task_index=3,
        attempt=0,
        worker="w:1",
        seconds=0.5,
        manifest=(OutputFile("/a/__part=7/data_0.parquet", 10, 2, partition=7),),
    )
    payload = json.loads(json.dumps(result.to_json()))
    assert payload["manifest"][0]["partition"] == 7
    assert TaskResult.from_json(payload) == result
    # A result file carries no partition at all.
    assert "partition" not in OutputFile("/a", 1, 1).to_json()
    with pytest.raises(ValueError):
        OutputFile.from_json({"path": "/a", "bytes": 1, "rows": 1, "partition": "7"})


@pytest.mark.parametrize(
    "change",
    [
        {"stage_id": "0"},
        {"sql": None},
        {"inputs": []},
        {"sink": {"kind": "parquet_files", "location": "/x"}},
        {"sink": {"kind": "iceberg_files", "location": "/x"}},  # no field ids
        {"task_index": True},
    ],
)
def test_malformed_specs_are_refused(change: dict[str, object]) -> None:
    payload = {**spec().to_json(), **change}
    with pytest.raises(ValueError):
        TaskSpec.from_json(payload)
