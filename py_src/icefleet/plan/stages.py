"""`plan_stages()`: a compiled icetl plan in, a `StagePlan` or `Uncuttable(reason)` out.

Two shapes distribute in Phase 2 (`plan/rel.py` recognises them):

    rows        stage 0  scan -> result                       Project* over a scan
    aggregate   stage 0  scan -> partial or raw -> shuffle    PLAN §4.2, plan/aggregates.py
                stage 1  final per partition -> result

The coordinator then reads the result files, applies the lifted `ORDER BY` / `LIMIT`, and
casts any stand-in column back to its type (`plan/types.py`).

Every stage's SQL is built from the optimized tree's own nodes and rendered once through
icetl's `substitute_sources` + `build_source`, exactly as icetl renders its own query:
the scan's file list is the `$<view>_paths_0` parameter there, and each task binds it to
its own split's files. A shuffle stage's SQL reads `"__icefleet_input__"`, which each task
swaps for `read_parquet($icefleet_input)` over its partition's files, or for a typed empty
relation when no rows reached the partition.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from icetl.exec.source_sql import build_source
from icetl.plan.builder import substitute_sources
from sqlglot import exp

from icefleet.errors import Uncuttable
from icefleet.plan.aggregates import INPUT_TABLE, Describe, cut_aggregate
from icefleet.plan.rel import Query, from_sqlglot
from icefleet.plan.splits import Split, file_weights, plan_splits
from icefleet.plan.tasks import InputSpec, SinkSpec, TaskResult, TaskSpec
from icefleet.plan.types import cast_to, data_type, parquet_safe_type, parquet_unsafe_type

if TYPE_CHECKING:
    from icetl.exec.scan_planner import ScanPlan
    from icetl.plan.builder import ScanSource
    from icetl.sql.session import CompiledPlan

    from icefleet.config import FleetSettings
    from icefleet.exchange.base import Exchange

__all__ = [
    "INPUT_PARAMETER",
    "ScanStage",
    "ShuffleStage",
    "Stage",
    "StagePlan",
    "parquet_unsafe_type",
    "plan_stages",
]

INPUT_PARAMETER = "icefleet_input"
_PLACEHOLDER = exp.to_identifier(INPUT_TABLE, quoted=True).sql(dialect="duckdb")
_READ_INPUT = (
    f"read_parquet(${INPUT_PARAMETER}, union_by_name => false, hive_partitioning => false)"
)


@dataclass(frozen=True)
class ScanStage:
    """A stage reading one table's splits, one task per split."""

    stage_id: int
    sql: str
    parameter: str
    scan: ScanPlan
    splits: tuple[Split, ...]
    sink: str = "result"  # "result" | "shuffle"
    partitions: int = 1
    notes: tuple[str, ...] = ()  # what explain() prints under the scan line

    @property
    def table(self) -> str:
        return str(self.scan.source.resolved.qualified_name)

    def tasks(
        self,
        query_id: str,
        exchange: Exchange,
        upstream: Sequence[TaskResult] = (),
        *,
        target_file_bytes: int,
    ) -> list[TaskSpec]:
        """One `TaskSpec` per split, each writing under its own prefix of the exchange."""
        return [
            TaskSpec(
                query_id=query_id,
                stage_id=self.stage_id,
                task_index=index,
                sql=self.sql,
                inputs={self.parameter: InputSpec(paths=split.paths)},
                sink=SinkSpec(
                    kind=self.sink,
                    location=exchange.task_location(query_id, self.stage_id, index),
                    target_file_bytes=target_file_bytes,
                    partitions=self.partitions,
                ),
            )
            for index, split in enumerate(self.splits)
        ]


@dataclass(frozen=True)
class ShuffleStage:
    """A stage reading another stage's shuffle output, one task per partition with rows."""

    stage_id: int
    sql: str  # reads _PLACEHOLDER
    input_stage: int
    partitions: int
    empty_input: str  # a typed relation with no rows, for a partition nothing reached
    notes: tuple[str, ...] = ()

    def partition_files(self, upstream: Sequence[TaskResult]) -> dict[int, list[str]]:
        """Partition -> the files every upstream task wrote for it, in task order."""
        files: dict[int, list[str]] = defaultdict(list)
        for result in sorted(upstream, key=lambda r: r.task_index):
            if result.stage_id != self.input_stage:
                continue
            for output in result.manifest:
                files[output.partition or 0].append(output.path)
        return files

    def tasks(
        self,
        query_id: str,
        exchange: Exchange,
        upstream: Sequence[TaskResult] = (),
        *,
        target_file_bytes: int,
    ) -> list[TaskSpec]:
        """A task per partition some upstream task wrote to (kickoff answer 3).

        A partition no row reached would aggregate nothing, so it gets no task -- except
        when no partition has rows at all: then partition 0 runs over the empty input, so
        the result still has its schema (and a global aggregate its one row).
        """
        files = self.partition_files(upstream)
        chosen = sorted(p for p, paths in files.items() if paths) or [0]
        specs = []
        for partition in chosen:
            paths = tuple(files.get(partition, ()))
            source = _READ_INPUT if paths else self.empty_input
            specs.append(
                TaskSpec(
                    query_id=query_id,
                    stage_id=self.stage_id,
                    task_index=partition,
                    sql=self.sql.replace(_PLACEHOLDER, source),
                    inputs={INPUT_PARAMETER: InputSpec(paths=paths)} if paths else {},
                    sink=SinkSpec(
                        kind="result",
                        location=exchange.task_location(query_id, self.stage_id, partition),
                        target_file_bytes=target_file_bytes,
                    ),
                )
            )
        return specs


Stage = ScanStage | ShuffleStage


@dataclass(frozen=True)
class StagePlan:
    """The stages of one query, in the order they run, and the coordinator's last step."""

    stages: tuple[Stage, ...]
    columns: tuple[str, ...] = ()  # the result columns the caller sees
    restore: Mapping[str, str] = field(default_factory=dict)  # column -> type to cast back to
    order: tuple[str, ...] = ()  # ORDER BY items over result columns, DuckDB SQL
    limit: int | None = None

    @property
    def final(self) -> Stage:
        return self.stages[-1]

    @property
    def scan(self) -> ScanStage:
        first = self.stages[0]
        assert isinstance(first, ScanStage)
        return first

    def coordinator_note(self) -> str | None:
        parts = []
        if self.order:
            parts.append("order by " + ", ".join(self.order))
        if self.limit is not None:
            parts.append(f"limit {self.limit}")
        return " · ".join(parts) if parts else None


class _Untypable(Exception):
    """DuckDB could not DESCRIBE a stage: the plan runs single-node instead."""


def _no_register(name: str, value: Any) -> None:  # pragma: no cover - empty scans are refused
    raise AssertionError(f"an empty scan reached the stage planner ({name})")


def plan_stages(
    compiled: CompiledPlan,
    sources: Mapping[str, ScanSource],
    *,
    output_names: Sequence[str],
    fleet: FleetSettings,
    min_splits: int,
    describe: Describe,
    register: Callable[[str, Any], None] = _no_register,
    udf_names: Collection[str] = (),
    aggregate_names: Collection[str] = (),
) -> StagePlan | Uncuttable:
    """Cut `compiled` into stages, or say why it cannot be cut.

    `describe` types a SELECT over the plan's sources (DuckDB's DESCRIBE against icetl's
    zero-row views); `aggregate_names` is DuckDB's catalog of aggregate functions.
    """
    optimized = compiled.optimized
    if optimized is None:
        return Uncuttable("icetl produced no optimized plan to cut")
    query = from_sqlglot(
        optimized.optimized, sources, udf_names=udf_names, aggregate_names=aggregate_names
    )
    if isinstance(query, Uncuttable):
        return query
    if len(set(output_names)) != len(output_names):
        return Uncuttable("duplicate output column names, which a result file cannot hold")

    scan = next((s for s in compiled.scans if s.source.key == query.scan.key), None)
    if scan is None:
        return Uncuttable(f"no scan was planned for {query.scan.key}")
    if scan.is_empty:
        return Uncuttable("the scan reads no data files, so there is nothing to distribute")
    if len(scan.groups) != 1:
        return Uncuttable(
            "files with renamed or late-added columns need per-group field-id aliasing -- Phase 7"
        )

    def typed(tree: exp.Expression) -> list[tuple[str, str]]:
        try:
            return describe(tree)
        except Exception as exc:  # DuckDB's binder speaks for the stage SQL
            raise _Untypable(f"{type(exc).__name__}: {exc}") from exc

    try:
        return _plan(
            query,
            scan,
            sources,
            output_names=list(output_names),
            fleet=fleet,
            min_splits=min_splits,
            describe=typed,
            register=register,
            aggregate_names=aggregate_names,
        )
    except _Untypable as exc:
        return Uncuttable(f"the stage planner could not type a stage ({exc})")


def _plan(
    query: Query,
    scan: ScanPlan,
    sources: Mapping[str, ScanSource],
    *,
    output_names: list[str],
    fleet: FleetSettings,
    min_splits: int,
    describe: Describe,
    register: Callable[[str, Any], None],
    aggregate_names: Collection[str],
) -> StagePlan | Uncuttable:
    # The result as the caller will see it, typed before anything is cut.
    result = describe(query.tree)
    if [name for name, _ in result[: len(output_names)]] != output_names:
        return Uncuttable(
            f"the optimized plan's columns {[n for n, _ in result]} are not {output_names}"
        )
    unsafe = [f"{name} {kind}" for name, kind in result if parquet_safe_type(kind) is None]
    if unsafe:
        return Uncuttable(
            f"result column(s) whose type does not survive a parquet exchange: {', '.join(unsafe)}"
        )
    stand_in = {name: parquet_safe_type(kind) or kind for name, kind in result}
    restore = {name: kind for name, kind in result[: len(output_names)] if stand_in[name] != kind}

    parameter = f"{scan.source.view}_paths_0"
    splits = plan_splits(
        file_weights(scan, list(scan.groups[0].paths)),
        target_bytes=fleet.split_target_bytes,
        min_splits=min_splits,
        max_splits=fleet.split_max,
    )

    def render(tree: exp.Expression) -> tuple[str, dict[str, Any]] | Uncuttable:
        parameters: dict[str, Any] = {}
        bound = substitute_sources(
            tree,
            sources,
            lambda source: build_source(scan, parameters=parameters, register=register),
        )
        if set(parameters) != {parameter}:
            return Uncuttable(
                f"unexpected SQL parameters {sorted(parameters)}; expected [{parameter}]"
            )
        return bound.sql(dialect="duckdb"), parameters

    def output(tree: exp.Select) -> exp.Select:
        """The result as written: stand-in casts, then each task's own top-n / first-n."""
        if any(stand_in[name] != kind for name, kind in result):
            tree = exp.select(
                *[
                    exp.alias_(
                        cast_to(exp.column(name, quoted=True), stand_in[name])
                        if stand_in[name] != kind
                        else exp.column(name, quoted=True),
                        name,
                        quoted=True,
                    )
                    for name, kind in result
                ]
            ).from_(tree.subquery("__icefleet_out"))
        if query.limit is not None:
            top = exp.select("*").from_(tree.subquery("__icefleet_top"))
            if query.order:
                top = top.order_by(*[item.copy() for item in query.order])
            tree = top.limit(query.limit)
        return tree

    order = tuple(item.sql(dialect="duckdb") for item in query.order)
    per_task = (
        ""
        if query.limit is None
        else f" · {'top' if query.order else 'first'} {query.limit} per task"
    )
    aggregate = query.aggregate

    if aggregate is None:
        rendered = render(output(query.tree))
        if isinstance(rendered, Uncuttable):
            return rendered
        stage = ScanStage(
            stage_id=0,
            sql=rendered[0],
            parameter=parameter,
            scan=scan,
            splits=splits,
            notes=(f"-> result{per_task}",),
        )
        return StagePlan(
            stages=(stage,),
            columns=tuple(output_names),
            restore=restore,
            order=order,
            limit=query.limit,
        )

    cut = cut_aggregate(
        aggregate,
        partitions=fleet.shuffle_partitions,
        describe=describe,
        aggregate_names=aggregate_names,
    )
    if isinstance(cut, Uncuttable):
        return cut
    partial = render(cut.partial)
    if isinstance(partial, Uncuttable):
        return partial

    keys = ", ".join(cut.keys)
    grouping = f" · group by {keys}" if keys else ""
    if cut.strategy == "partial":
        work = f"partial: {', '.join(cut.partials) or 'distinct keys'}{grouping}"
    else:
        work = f"raw rows, no partial aggregate ({cut.why_raw}){grouping}"
    shuffle = (
        f"-> shuffle hash({keys}) x {cut.partitions}"
        if cut.partitions > 1
        else "-> shuffle to one partition"
    )
    scan_stage = ScanStage(
        stage_id=0,
        sql=partial[0],
        parameter=parameter,
        scan=scan,
        splits=splits,
        sink="shuffle",
        partitions=cut.partitions,
        notes=(work, shuffle),
    )

    if aggregate.select is query.tree:
        final_tree = cut.final
    else:
        aggregate.select.replace(cut.final)
        final_tree = query.tree
    final_sql = output(final_tree).sql(dialect="duckdb")
    if final_sql.count(_PLACEHOLDER) != 1:  # pragma: no cover - one FROM reads the input
        return Uncuttable("the final stage does not read its input exactly once")
    combined = (
        f"final: {', '.join(cut.finals)}"
        if cut.strategy == "partial" and cut.finals
        else (
            "final: distinct keys"
            if cut.strategy == "partial"
            else "final: the aggregate as written"
        )
    )
    spread = (
        f"{cut.partitions} partitions, a task per partition with rows"
        if cut.partitions > 1
        else "one partition"
    )
    final_stage = ShuffleStage(
        stage_id=1,
        sql=final_sql,
        input_stage=0,
        partitions=cut.partitions,
        empty_input=_empty_relation(cut.shuffle_columns),
        notes=(f"{combined}{grouping} · {spread}", f"-> result{per_task}"),
    )
    return StagePlan(
        stages=(scan_stage, final_stage),
        columns=tuple(output_names),
        restore=restore,
        order=order,
        limit=query.limit,
    )


def _empty_relation(columns: Sequence[tuple[str, str]]) -> str:
    """`(SELECT CAST(NULL AS t) AS "c", ... WHERE FALSE)`: the shuffle schema, no rows."""
    projection = [
        exp.alias_(exp.Cast(this=exp.null(), to=data_type(kind)), name, quoted=True)
        for name, kind in columns
    ]
    return "(" + exp.select(*projection).where(exp.false()).sql(dialect="duckdb") + ")"
