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


@pytest.mark.parametrize(
    "change",
    [
        {"stage_id": "0"},
        {"sql": None},
        {"inputs": []},
        {"sink": {"kind": "shuffle", "location": "/x"}},
        {"task_index": True},
    ],
)
def test_malformed_specs_are_refused(change: dict[str, object]) -> None:
    payload = {**spec().to_json(), **change}
    with pytest.raises(ValueError):
        TaskSpec.from_json(payload)
