"""`plan_stages()`: a compiled icetl plan in, a `StagePlan` or `Uncuttable(reason)` out.

Phase 1 produces at most one stage -- a scan stage whose sink is `result` -- because
`Project(Scan)` needs no exchange. Its SQL is the SQL icetl compiled, unchanged: the
scan's file list is a `$placeholder` parameter there already, and each task binds that
parameter to its own split's files. Nothing about the query is re-rendered.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from icefleet.errors import Uncuttable
from icefleet.plan.rel import Project, from_sqlglot
from icefleet.plan.splits import Split, file_weights, plan_splits
from icefleet.plan.tasks import InputSpec, SinkSpec, TaskSpec

if TYPE_CHECKING:
    from icetl.exec.scan_planner import ScanPlan
    from icetl.plan.builder import ScanSource
    from icetl.sql.session import CompiledPlan

    from icefleet.config import FleetSettings
    from icefleet.exchange.base import Exchange

__all__ = ["ScanStage", "StagePlan", "parquet_unsafe_type", "plan_stages"]

#: DuckDB types that do not come back from parquet as the same Arrow type, measured on
#: duckdb 1.5.6 (commands.md, 2026-11-15): HUGEINT/UHUGEINT become DOUBLE, second- and
#: millisecond timestamps become microseconds, fixed-size arrays become lists, and ENUM,
#: BIT, VARINT and UNION change shape. A result carrying one runs single-node, because
#: the distributed answer would differ from icetl's in type. A struct field *named* like
#: one of these trips the check too -- a false positive that costs speed, never an answer.
_PARQUET_UNSAFE = re.compile(
    r"\b(HUGEINT|UHUGEINT|TIMESTAMP_S|TIMESTAMP_MS|BIT|BITSTRING|VARINT|BIGNUM)\b"
    r"|\bENUM\s*\(|\bUNION\s*\(|\[\d+\]",
    re.IGNORECASE,
)


def parquet_unsafe_type(duckdb_type: str) -> bool:
    """True when a column of this DuckDB type would change type through a result file."""
    return _PARQUET_UNSAFE.search(duckdb_type) is not None


@dataclass(frozen=True)
class ScanStage:
    """A stage reading one table's splits. Its sink is `result` in Phase 1."""

    stage_id: int
    sql: str
    parameter: str
    scan: ScanPlan
    splits: tuple[Split, ...]
    limit: int | None = None

    @property
    def table(self) -> str:
        return str(self.scan.source.resolved.qualified_name)

    def tasks(self, query_id: str, exchange: Exchange, *, target_file_bytes: int) -> list[TaskSpec]:
        """One `TaskSpec` per split, each writing under its own prefix of the exchange."""
        return [
            TaskSpec(
                query_id=query_id,
                stage_id=self.stage_id,
                task_index=index,
                sql=self.sql,
                inputs={self.parameter: InputSpec(paths=split.paths)},
                sink=SinkSpec(
                    kind="result",
                    location=exchange.task_location(query_id, self.stage_id, index),
                    target_file_bytes=target_file_bytes,
                ),
            )
            for index, split in enumerate(self.splits)
        ]


@dataclass(frozen=True)
class StagePlan:
    """The stages of one query, in the order they run. Phase 1: exactly one."""

    stages: tuple[ScanStage, ...]

    @property
    def final(self) -> ScanStage:
        return self.stages[-1]

    @property
    def limit(self) -> int | None:
        """A LIMIT the coordinator re-applies after concatenating the tasks' output."""
        return self.final.limit


def plan_stages(
    compiled: CompiledPlan,
    sources: Mapping[str, ScanSource],
    *,
    output_names: Sequence[str],
    output_types: Sequence[str],
    fleet: FleetSettings,
    min_splits: int,
    udf_names: Collection[str] = (),
) -> StagePlan | Uncuttable:
    """Cut `compiled` into stages, or say why it cannot be cut."""
    optimized = compiled.optimized
    if optimized is None:
        return Uncuttable("icetl produced no optimized plan to cut")

    # The shape first: its reason names the phase that will distribute the plan, which is
    # more useful than a type complaint (sum() of an INTEGER is a HUGEINT, for one).
    shape = from_sqlglot(optimized.optimized, sources, udf_names=udf_names)
    if isinstance(shape, Uncuttable):
        return shape
    assert isinstance(shape, Project)

    if len(set(output_names)) != len(output_names):
        return Uncuttable("duplicate output column names, which a result file cannot hold")
    unsafe = [
        f"{name} {kind}"
        for name, kind in zip(output_names, output_types, strict=False)
        if parquet_unsafe_type(kind)
    ]
    if unsafe:
        return Uncuttable(
            f"result column(s) whose type does not survive a parquet exchange: {', '.join(unsafe)}"
        )

    scan = next((s for s in compiled.scans if s.source.key == shape.child.key), None)
    if scan is None:
        return Uncuttable(f"no scan was planned for {shape.child.key}")
    if scan.is_empty:
        return Uncuttable("the scan reads no data files, so there is nothing to distribute")
    if len(scan.groups) != 1:
        return Uncuttable(
            "files with renamed or late-added columns need per-group field-id aliasing -- Phase 7"
        )

    parameter = f"{scan.source.view}_paths_0"
    if set(compiled.parameters) != {parameter}:
        return Uncuttable(
            f"unexpected SQL parameters {sorted(compiled.parameters)}; expected [{parameter}]"
        )

    paths = list(scan.groups[0].paths)
    splits = plan_splits(
        file_weights(scan, paths),
        target_bytes=fleet.split_target_bytes,
        min_splits=min_splits,
        max_splits=fleet.split_max,
    )
    stage = ScanStage(
        stage_id=0,
        sql=compiled.sql,
        parameter=parameter,
        scan=scan,
        splits=splits,
        limit=shape.limit,
    )
    return StagePlan(stages=(stage,))
