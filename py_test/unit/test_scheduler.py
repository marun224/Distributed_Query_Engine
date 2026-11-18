"""The thread-pool scheduler's waves (Phase 3, kickoff answer 6), without workers."""

from __future__ import annotations

from typing import Any

from icefleet.exchange.local import LocalDirExchange
from icefleet.plan.stages import Stage, StageInput, StagePlan
from icefleet.plan.tasks import OutputFile, TaskResult, TaskSpec
from icefleet.scheduler import ThreadPoolScheduler


def _stage(stage_id: int, reads: tuple[int, ...] = (), sink: str = "shuffle") -> Stage:
    inputs = tuple(StageInput(stage_id=k, mode="partition", empty="(SELECT 1)") for k in reads)
    sql = "SELECT * FROM " + (", ".join(i.token for i in inputs) or "range(1)")
    return Stage(stage_id=stage_id, sql=sql, inputs=inputs, sink=sink, partitions=2)


class _Recorder(ThreadPoolScheduler):
    def __init__(self) -> None:
        self.waves: list[list[tuple[int, int]]] = []
        self._target_file_bytes = 1

    def _run_tasks(self, specs: list[TaskSpec], *, query_id: str) -> list[TaskResult]:
        self.waves.append(sorted((s.stage_id, s.task_index) for s in specs))
        return [
            TaskResult(
                query_id=query_id,
                stage_id=s.stage_id,
                task_index=s.task_index,
                attempt=0,
                worker="w",
                seconds=0.0,
                manifest=tuple(
                    OutputFile(path=f"/x/{s.stage_id}/{p}.parquet", bytes=1, rows=1, partition=p)
                    for p in (0, 1)
                ),
            )
            for s in specs
        ]


def test_stages_that_read_nothing_unfinished_run_together(tmp_path: Any) -> None:
    """Two scans, then the join that reads both, then a stage reading the join."""
    plan = StagePlan(
        stages=(_stage(0), _stage(1), _stage(2, (0, 1)), _stage(3, (2,), sink="result"))
    )
    scheduler = _Recorder()
    results = scheduler.run(plan, query_id="q-1", exchange=LocalDirExchange(str(tmp_path)))
    assert scheduler.waves == [
        [(0, 0), (1, 0)],
        [(2, 0), (2, 1)],
        [(3, 0), (3, 1)],
    ]
    assert [(r.stage_id, r.task_index) for r in results] == [
        (0, 0),
        (1, 0),
        (2, 0),
        (2, 1),
        (3, 0),
        (3, 1),
    ]


def test_a_task_reads_its_partition_of_every_partitioned_input(tmp_path: Any) -> None:
    upstream = [
        TaskResult(
            query_id="q",
            stage_id=k,
            task_index=0,
            attempt=0,
            worker="w",
            seconds=0.0,
            manifest=(OutputFile(path=f"/x/{k}/p1.parquet", bytes=1, rows=1, partition=1),),
        )
        for k in (0, 1)
    ]
    upstream.append(
        TaskResult(
            query_id="q",
            stage_id=0,
            task_index=1,
            attempt=0,
            worker="w",
            seconds=0.0,
            manifest=(OutputFile(path="/x/0/p3.parquet", bytes=1, rows=1, partition=3),),
        )
    )
    stage = _stage(2, (0, 1))
    specs = stage.tasks("q", LocalDirExchange(str(tmp_path)), upstream, target_file_bytes=1)
    assert [s.task_index for s in specs] == [1, 3]
    first, second = specs
    assert {k: v.paths for k, v in first.inputs.items()} == {
        "icefleet_input_0": ("/x/0/p1.parquet",),
        "icefleet_input_1": ("/x/1/p1.parquet",),
    }
    # Partition 3 has rows on one side only: the other side is its typed empty relation.
    assert set(second.inputs) == {"icefleet_input_0"}
    assert "(SELECT 1)" in second.sql and "$icefleet_input_0" in second.sql


def test_a_broadcast_input_is_read_whole_by_every_task(tmp_path: Any) -> None:
    upstream = [
        TaskResult(
            query_id="q",
            stage_id=0,
            task_index=i,
            attempt=0,
            worker="w",
            seconds=0.0,
            manifest=(OutputFile(path=f"/x/0/{i}.parquet", bytes=1, rows=1),),
        )
        for i in (1, 0)
    ]
    item = StageInput(stage_id=0, mode="all", empty="(SELECT 1)")
    stage = Stage(stage_id=1, sql=f"SELECT * FROM {item.token}", inputs=(item,))
    (spec,) = stage.tasks("q", LocalDirExchange(str(tmp_path)), upstream, target_file_bytes=1)
    assert spec.inputs["icefleet_input_0"].paths == ("/x/0/0.parquet", "/x/0/1.parquet")
    assert "read_parquet($icefleet_input_0" in spec.sql
