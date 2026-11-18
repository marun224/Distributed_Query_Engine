"""`plan_stages()`: a compiled icetl plan in, a `StagePlan` or `Uncuttable(reason)` out.

The planner walks the tree `plan/rel.py` recognised, bottom up, keeping each subtree as an
*open pipeline*: SQL that a task could run, with what drives its tasks --

    a scan       one task per split of an Iceberg table's files
    partitions   one task per hash partition of the stage(s) it reads
    nothing      it reads only broadcast inputs: one task

-- and what its rows are known to be partitioned by. A pipeline grows by wrapping (a
projection, an aggregate in place, a join with a broadcast side) and is *closed* into a
stage where rows must change hands:

    shuffle    COPY ... PARTITION_BY hash(keys) % n   (an aggregate's partial, a join side)
    broadcast  written once, read whole by every task of the other side
    result     the root, which the coordinator reads

    aggregate   partial in the input's pipeline -> shuffle(keys) -> final per partition
                (in place, no shuffle, when the input is already partitioned by a subset
                of the keys -- kickoff answer 4)
    join        broadcast the small side (PLAN §4.3), or shuffle both sides by the keys,
                skipping a side already partitioned that way (kickoff answer 4)

Every table *reference* is its own split input, so a self-join reads its table twice,
in two stages, and a stage never binds two file lists to one icetl parameter. A stage's
SQL is built from the optimized tree's own nodes and rendered once through icetl's
`substitute_sources` + `build_source`, as icetl renders its own query. An input from
another stage is a table named `__icefleet_input_<k>__`, which each task swaps for
`read_parquet($icefleet_input_<k>)` over its files -- its partition's, or all of them for
a broadcast -- or for a typed empty relation when no rows reached the partition.
"""

from __future__ import annotations

import itertools
from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from icetl.exec.scan_planner import format_bytes
from icetl.exec.source_sql import build_source
from icetl.plan.builder import source_key, substitute_sources
from sqlglot import exp

from icefleet.errors import Uncuttable
from icefleet.plan.aggregates import (
    INPUT_TABLE,
    Describe,
    cut_aggregate,
    group_keys,
)
from icefleet.plan.joins import BROADCASTABLE, conjuncts, equi_keys, is_deterministic, null_supplied
from icefleet.plan.partitioning import Hash, hash_type, join_hash_type, partition_expression
from icefleet.plan.rel import (
    Aggregate,
    Join,
    Project,
    Query,
    RelNode,
    Scan,
    from_of,
    from_sqlglot,
    join_kind,
    relation_alias,
    set_from,
)
from icefleet.plan.splits import Split, file_weights, plan_splits
from icefleet.plan.tasks import PARTITION_COLUMN, InputSpec, SinkSpec, TaskResult, TaskSpec
from icefleet.plan.types import cast_to, data_type, parquet_safe_type, parquet_unsafe_type

if TYPE_CHECKING:
    from icetl.exec.scan_planner import ScanPlan
    from icetl.plan.builder import ScanSource
    from icetl.sql.session import CompiledPlan

    from icefleet.config import FleetSettings
    from icefleet.exchange.base import Exchange

__all__ = [
    "Stage",
    "StageInput",
    "StagePlan",
    "parquet_unsafe_type",
    "plan_stages",
]

_INPUT_PREFIX = "__icefleet_input_"


def _read(parameter: str) -> str:
    return f"read_parquet(${parameter}, union_by_name => false, hive_partitioning => false)"


@dataclass(frozen=True)
class StageInput:
    """Another stage's output, read by this one.

    `partition`: each task reads its own hash partition of it. `all`: every task reads
    all of it (a broadcast). `empty` is the typed relation with no rows a task reads
    instead when its partition has no files.
    """

    stage_id: int
    mode: str  # "partition" | "all"
    empty: str

    @property
    def placeholder(self) -> str:
        return f"{_INPUT_PREFIX}{self.stage_id}__"

    @property
    def parameter(self) -> str:
        return f"icefleet_input_{self.stage_id}"

    @property
    def token(self) -> str:
        """How the placeholder appears in rendered DuckDB SQL."""
        return exp.to_identifier(self.placeholder, quoted=True).sql(dialect="duckdb")


@dataclass(frozen=True)
class Stage:
    """One stage: its SQL, what drives its tasks, what it reads, where it writes.

    A stage with a `scan` has a task per split; otherwise a task per partition some
    partitioned input has rows in (partition 0 alone when none has, or when it has no
    partitioned input at all).
    """

    stage_id: int
    sql: str
    inputs: tuple[StageInput, ...] = ()
    sink: str = "result"  # "result" | "shuffle"
    partitions: int = 1  # of the sink
    scan: ScanPlan | None = None
    alias: str | None = None  # the scan's alias in the query
    parameter: str | None = None  # the scan's file-list parameter
    splits: tuple[Split, ...] = ()
    notes: tuple[str, ...] = ()  # what explain() prints

    @property
    def table(self) -> str:
        assert self.scan is not None
        return str(self.scan.source.resolved.qualified_name)

    @property
    def depends_on(self) -> tuple[int, ...]:
        return tuple(sorted({i.stage_id for i in self.inputs}))

    def tasks(
        self,
        query_id: str,
        exchange: Exchange,
        upstream: Sequence[TaskResult] = (),
        *,
        target_file_bytes: int,
    ) -> list[TaskSpec]:
        """The `TaskSpec`s of this stage, built from its inputs' manifests -- never a glob."""
        results: dict[int, list[TaskResult]] = defaultdict(list)
        for result in upstream:
            results[result.stage_id].append(result)
        whole: dict[int, tuple[str, ...]] = {}
        parts: dict[int, dict[int, list[str]]] = {}
        for item in self.inputs:
            ordered = sorted(results[item.stage_id], key=lambda r: r.task_index)
            if item.mode == "all":
                whole[item.stage_id] = tuple(f.path for r in ordered for f in r.manifest)
                if not whole[item.stage_id]:  # pragma: no cover - a result sink always writes
                    raise ValueError(f"stage {item.stage_id} wrote no files to broadcast")
            else:
                files: dict[int, list[str]] = defaultdict(list)
                for r in ordered:
                    for output in r.manifest:
                        files[output.partition or 0].append(output.path)
                parts[item.stage_id] = files

        def spec(index: int, partition: int, extra: Mapping[str, InputSpec]) -> TaskSpec:
            sql = self.sql
            inputs: dict[str, InputSpec] = dict(extra)
            for item in self.inputs:
                paths = (
                    whole[item.stage_id]
                    if item.mode == "all"
                    else tuple(parts[item.stage_id].get(partition, ()))
                )
                if paths:
                    sql = sql.replace(item.token, _read(item.parameter))
                    inputs[item.parameter] = InputSpec(paths=paths)
                else:
                    sql = sql.replace(item.token, item.empty)
            return TaskSpec(
                query_id=query_id,
                stage_id=self.stage_id,
                task_index=index,
                sql=sql,
                inputs=inputs,
                sink=SinkSpec(
                    kind=self.sink,
                    location=exchange.task_location(query_id, self.stage_id, index),
                    target_file_bytes=target_file_bytes,
                    partitions=self.partitions,
                ),
            )

        if self.scan is not None:
            assert self.parameter is not None
            return [
                spec(index, 0, {self.parameter: InputSpec(paths=split.paths)})
                for index, split in enumerate(self.splits)
            ]
        # A partition no row reached would compute nothing, so it gets no task (kickoff
        # answer 3 of Phase 2) -- except that when no partition has rows at all, partition 0
        # runs over empty inputs, so the result keeps its schema and a global aggregate
        # its one row.
        chosen = sorted({p for files in parts.values() for p, paths in files.items() if paths})
        return [spec(p, p, {}) for p in chosen or [0]]


@dataclass(frozen=True)
class StagePlan:
    """The stages of one query, inputs before readers, and the coordinator's last step."""

    stages: tuple[Stage, ...]
    columns: tuple[str, ...] = ()  # the result columns the caller sees
    restore: Mapping[str, str] = field(default_factory=dict)  # column -> type to cast back to
    order: tuple[str, ...] = ()  # ORDER BY items over result columns, DuckDB SQL
    limit: int | None = None

    @property
    def final(self) -> Stage:
        return self.stages[-1]

    def coordinator_note(self) -> str | None:
        parts = []
        if self.order:
            parts.append("order by " + ", ".join(self.order))
        if self.limit is not None:
            parts.append(f"limit {self.limit}")
        return " · ".join(parts) if parts else None


# -- the planner -----------------------------------------------------------------------


class _Untypable(Exception):
    """DuckDB could not DESCRIBE a stage: the plan runs single-node instead."""


class _Refused(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _no_register(name: str, value: Any) -> None:  # pragma: no cover - empty scans are refused
    raise AssertionError(f"an empty scan reached the stage planner ({name})")


def _quoted_alias(name: str) -> exp.TableAlias:
    return exp.TableAlias(this=exp.to_identifier(name, quoted=True))


def _alias(expression: exp.Expression, name: str) -> exp.Alias:
    return exp.Alias(this=expression, alias=exp.to_identifier(name, quoted=True))


def _short(expression: exp.Expression) -> str:
    """SQL for `explain()`, without the table qualifiers."""
    copy = expression.copy()
    for column in copy.find_all(exp.Column):
        column.set("table", None)
    return copy.sql(dialect="duckdb")


def _empty_relation(columns: Sequence[tuple[str, str]]) -> str:
    """`(SELECT CAST(NULL AS t) AS "c", ... WHERE FALSE)`: a schema, no rows."""
    return "(" + _empty_select(columns).sql(dialect="duckdb") + ")"


def _empty_select(columns: Sequence[tuple[str, str]]) -> exp.Select:
    projection = [
        _alias(exp.Cast(this=exp.null(), to=data_type(kind)), name) for name, kind in columns
    ]
    return exp.select(*projection).where(exp.false())


@dataclass(frozen=True)
class _Pipe:
    """An open pipeline: SQL producing a relation's rows inside one stage's tasks.

    `body` is an `exp.Table` (an Iceberg table reference, or another stage's input) or
    an `exp.Select`. `scan` / `parts` say what drives the tasks (neither: a single task
    over broadcast inputs). `columns` are its output names where the planner needs them
    (join sides); `hashes` what its rows are partitioned by; `estimate` an upper bound of
    its size in parquet bytes, or None when unknown (a join's output).
    """

    body: exp.Expression
    columns: tuple[str, ...] = ()
    scan: str | None = None
    alias: str | None = None
    parts: int | None = None
    inputs: tuple[StageInput, ...] = ()
    hashes: tuple[Hash, ...] = ()
    estimate: int | None = None
    notes: tuple[str, ...] = ()

    def relation(self, alias: str) -> exp.Expression:
        """The pipeline as a FROM item named `alias`."""
        if isinstance(self.body, exp.Table):
            table = self.body.copy()
            table.set("alias", _quoted_alias(alias))
            return table
        return exp.Subquery(this=self.body.copy(), alias=_quoted_alias(alias))

    def select(self) -> exp.Select:
        if isinstance(self.body, exp.Select):
            return self.body
        return exp.select(exp.Star()).from_(self.relation("__icefleet_t"))


def _renamed_hashes(hashes: Sequence[Hash], names: Mapping[str, str]) -> tuple[Hash, ...]:
    out = []
    for item in hashes:
        renamed = item.renamed(dict(names))
        if renamed is not None:
            out.append(renamed)
    return tuple(out)


def _projected(select: exp.Select, alias: str | None) -> dict[str, str]:
    """Input column -> the output name a projection passes it through under."""
    names: dict[str, str] = {}
    for projection in select.expressions:
        target = projection.unalias()
        if isinstance(target, exp.Column) and (not target.table or target.table == alias):
            names.setdefault(target.name, projection.alias_or_name)
    return names


def _hash_over(
    columns: Sequence[str | None], positions: Sequence[int], types: tuple[str, ...], n: int
) -> tuple[Hash, ...]:
    """`Hash` over the key columns at `positions`, if every one of them is a plain column."""
    chosen = [columns[i] for i in positions]
    if any(c is None for c in chosen):
        return ()
    return (Hash(tuple(c for c in chosen if c is not None), types, n),)


class _Planner:
    def __init__(
        self,
        *,
        sources: Mapping[str, ScanSource],
        scans: Mapping[str, ScanPlan],
        fleet: FleetSettings,
        min_splits: int,
        describe: Describe,
        register: Callable[[str, Any], None],
        aggregate_names: Collection[str],
    ) -> None:
        self.sources = sources
        self.scans = scans
        self.fleet = fleet
        self.min_splits = min_splits
        self._describe = describe
        self.register = register
        self.aggregate_names = aggregate_names
        self.stages: list[Stage] = []
        self.schemas: dict[str, list[tuple[str, str]]] = {}  # placeholder -> columns as written
        self._splits: dict[str, tuple[Split, ...]] = {}
        self._counter = itertools.count()

    def fresh(self, stem: str) -> str:
        return f"{stem}{next(self._counter)}"

    # -- typing --------------------------------------------------------------------

    def describe(self, tree: exp.Expression) -> list[tuple[str, str]]:
        """DuckDB's DESCRIBE of `tree`, with every stage input typed by its schema."""

        def inline(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Table) and not node.db and node.name in self.schemas:
                return exp.Subquery(
                    this=_empty_select(self.schemas[node.name]),
                    alias=_quoted_alias(node.alias or node.name),
                )
            return node

        return self._describe(tree.transform(inline, copy=True))

    def stand_ins(self, select: exp.Select) -> tuple[list[tuple[str, str]], dict[str, str]]:
        """(columns as written to parquet, {name: type to cast back to})."""
        written: list[tuple[str, str]] = []
        restore: dict[str, str] = {}
        for name, kind in self.describe(select):
            stand_in = parquet_safe_type(kind)
            if stand_in is None:
                raise _Refused(
                    f"a column whose type does not survive a parquet exchange: {name} {kind}"
                )
            written.append((name, stand_in))
            if stand_in != kind:
                restore[name] = kind
        return written, restore

    # -- stages ----------------------------------------------------------------------

    def splits(self, key: str) -> tuple[Split, ...]:
        if key not in self._splits:
            scan = self.scans[key]
            self._splits[key] = plan_splits(
                file_weights(scan, list(scan.groups[0].paths)),
                target_bytes=self.fleet.split_target_bytes,
                min_splits=self.min_splits,
                max_splits=self.fleet.split_max,
            )
        return self._splits[key]

    def render(self, tree: exp.Expression, scan_key: str | None) -> tuple[str, str | None]:
        refs = [
            t
            for t in tree.find_all(exp.Table)
            if not isinstance(t.this, exp.Func) and source_key(t, versioned=True) in self.sources
        ]
        if scan_key is None:
            if refs:  # pragma: no cover - a table reference always drives its pipeline
                raise _Refused("a stage reads a table without splitting it")
            return tree.sql(dialect="duckdb"), None
        if len(refs) != 1:  # pragma: no cover - every reference is its own pipeline
            raise _Refused("a stage would read more than one table reference")
        scan = self.scans[scan_key]
        parameters: dict[str, Any] = {}
        bound = substitute_sources(
            tree,
            self.sources,
            lambda source: build_source(scan, parameters=parameters, register=self.register),
        )
        parameter = f"{scan.source.view}_paths_0"
        if set(parameters) != {parameter}:
            raise _Refused(
                f"unexpected SQL parameters {sorted(parameters)}; expected [{parameter}]"
            )
        return bound.sql(dialect="duckdb"), parameter

    def close(
        self,
        tree: exp.Select,
        pipe: _Pipe,
        *,
        sink: str,
        partitions: int,
        notes: Sequence[str],
    ) -> Stage:
        """Make `tree` -- `pipe`'s work, ready to write -- a stage of its own."""
        sql, parameter = self.render(tree, pipe.scan)
        for item in pipe.inputs:
            if sql.count(item.token) != 1:  # pragma: no cover - one FROM reads each input
                raise _Refused(f"a stage does not read stage {item.stage_id} exactly once")
        stage = Stage(
            stage_id=len(self.stages),
            sql=sql,
            inputs=pipe.inputs,
            sink=sink,
            partitions=partitions,
            scan=self.scans[pipe.scan] if pipe.scan else None,
            alias=pipe.alias,
            parameter=parameter,
            splits=self.splits(pipe.scan) if pipe.scan else (),
            notes=tuple(notes),
        )
        self.stages.append(stage)
        return stage

    def input_pipe(
        self,
        stage: Stage,
        *,
        mode: str,
        written: Sequence[tuple[str, str]],
        restore: Mapping[str, str],
        columns: tuple[str, ...],
        estimate: int | None,
        partitions: int | None,
        hashes: tuple[Hash, ...] = (),
    ) -> _Pipe:
        """A pipeline reading `stage`'s output, stand-in types cast back."""
        item = StageInput(stage_id=stage.stage_id, mode=mode, empty=_empty_relation(written))
        self.schemas[item.placeholder] = list(written)
        table = exp.Table(this=exp.to_identifier(item.placeholder, quoted=True))
        body: exp.Expression = table
        if restore:
            body = exp.select(
                *[
                    _alias(cast_to(exp.column(name, quoted=True), restore[name]), name)
                    if name in restore
                    else exp.column(name, quoted=True)
                    for name, _ in written
                ]
            ).from_(table)
        return _Pipe(
            body=body,
            columns=columns,
            parts=partitions,
            inputs=(item,),
            hashes=hashes,
            estimate=estimate,
        )

    def shuffle(
        self,
        side: _Pipe,
        keys: Sequence[exp.Expression],
        hash_as: Sequence[str],
        n: int,
        *,
        label: str | None = None,
    ) -> _Pipe:
        """Close `side` with a shuffle by `keys` (over its columns); read it per partition."""
        relation = side.relation("__icefleet_s")
        probe = exp.select(*[exp.column(c, quoted=True) for c in side.columns]).from_(relation)
        written, restore = self.stand_ins(probe)
        projection: list[exp.Expression] = [
            _alias(cast_to(exp.column(name, quoted=True), kind), name)
            if name in restore
            else exp.column(name, quoted=True)
            for name, kind in written
        ]
        if n > 1:
            projection.append(
                _alias(partition_expression(keys, n, hash_as=hash_as), PARTITION_COLUMN)
            )
        tree = exp.select(*projection).from_(relation.copy())
        label = label or ", ".join(_short(k) for k in keys)
        note = f"-> shuffle hash({label}) x {n}" if n > 1 else "-> shuffle to one partition"
        stage = self.close(tree, side, sink="shuffle", partitions=n, notes=(*side.notes, note))
        hashes: tuple[Hash, ...] = ()
        if n > 1 and all(isinstance(k, exp.Column) for k in keys):
            hashes = (Hash(tuple(k.name for k in keys), tuple(hash_as), n),)
        return self.input_pipe(
            stage,
            mode="partition",
            written=written,
            restore=restore,
            columns=side.columns,
            estimate=side.estimate,
            partitions=n,
            hashes=hashes,
        )

    def broadcast(self, side: _Pipe) -> tuple[Stage, _Pipe]:
        """Close `side` into result files that every task of the other side reads whole."""
        relation = side.relation("__icefleet_b")
        probe = exp.select(*[exp.column(c, quoted=True) for c in side.columns]).from_(relation)
        written, restore = self.stand_ins(probe)
        projection = [
            _alias(cast_to(exp.column(name, quoted=True), kind), name)
            if name in restore
            else exp.column(name, quoted=True)
            for name, kind in written
        ]
        tree = exp.select(*projection).from_(relation.copy())
        stage = self.close(
            tree, side, sink="result", partitions=1, notes=(*side.notes, "-> broadcast")
        )
        return stage, self.input_pipe(
            stage,
            mode="all",
            written=written,
            restore=restore,
            columns=side.columns,
            estimate=side.estimate,
            partitions=None,
        )

    # -- the tree ----------------------------------------------------------------------

    def plan(self, node: RelNode) -> _Pipe:
        if isinstance(node, Scan):
            assert node.table is not None
            table = node.table.copy()
            table.set("alias", None)
            return _Pipe(
                body=table,
                scan=node.key,
                alias=relation_alias(node.table),
                estimate=self.scans[node.key].bytes_scanned,
            )
        if isinstance(node, Project):
            return self.project(node)
        if isinstance(node, Aggregate):
            return self.aggregate(node)
        return self.join(node)

    def project(self, node: Project) -> _Pipe:
        child = self.plan(node.child)
        alias = relation_alias(from_of(node.select))  # type: ignore[arg-type]
        select = node.select.copy()
        set_from(select, child.relation(alias))
        names = _projected(select, alias)
        return replace(
            child,
            body=select,
            columns=tuple(p.alias_or_name for p in select.expressions),
            hashes=_renamed_hashes(child.hashes, names),
        )

    def aggregate(self, node: Aggregate) -> _Pipe:
        child = self.plan(node.child)
        alias = relation_alias(from_of(node.select))  # type: ignore[arg-type]
        work = node.select.copy()
        set_from(work, child.relation(alias))
        keys = group_keys(work)
        if isinstance(keys, Uncuttable):
            raise _Refused(keys.reason)
        grouping = f" · group by {', '.join(_short(k) for k in keys)}" if keys else ""
        columns = tuple(p.alias_or_name for p in work.expressions)

        # Already partitioned by (a subset of) the keys: equal keys share a partition, so
        # the aggregate runs where the rows are, whole, with no shuffle.
        key_columns = {
            k.name for k in keys if isinstance(k, exp.Column) and (not k.table or k.table == alias)
        }
        if keys and child.parts is not None:
            for known in child.hashes:
                if known.partitions == child.parts and set(known.columns) <= key_columns:
                    note = (
                        f"aggregate in place{grouping} (rows already partitioned by "
                        f"hash({', '.join(known.columns)}))"
                    )
                    return replace(
                        child,
                        body=work,
                        columns=columns,
                        hashes=_renamed_hashes(child.hashes, _projected(work, alias)),
                        notes=(*child.notes, note),
                    )

        cut = cut_aggregate(
            Aggregate(child=node.child, select=work),
            partitions=self.fleet.shuffle_partitions,
            describe=self.describe,
            aggregate_names=self.aggregate_names,
        )
        if isinstance(cut, Uncuttable):
            raise _Refused(cut.reason)
        label = ", ".join(cut.keys)
        if cut.strategy == "partial":
            work_note = f"partial: {', '.join(cut.partials) or 'distinct keys'}{grouping}"
        else:
            work_note = f"raw rows, no partial aggregate ({cut.why_raw}){grouping}"
        shuffle_note = (
            f"-> shuffle hash({label}) x {cut.partitions}"
            if cut.partitions > 1
            else "-> shuffle to one partition"
        )
        stage = self.close(
            cut.partial,
            child,
            sink="shuffle",
            partitions=cut.partitions,
            notes=(*child.notes, work_note, shuffle_note),
        )
        item = StageInput(
            stage_id=stage.stage_id, mode="partition", empty=_empty_relation(cut.shuffle_columns)
        )
        self.schemas[item.placeholder] = list(cut.shuffle_columns)
        final = cut.final.copy()
        for table in final.find_all(exp.Table):
            if table.name == INPUT_TABLE:
                table.set("this", exp.to_identifier(item.placeholder, quoted=True))

        if cut.strategy == "partial" and cut.finals:
            combined = f"final: {', '.join(cut.finals)}"
        elif cut.strategy == "partial":
            combined = "final: distinct keys"
        else:
            combined = "final: the aggregate as written"
        spread = (
            f"{cut.partitions} partitions, a task per partition with rows"
            if cut.partitions > 1
            else "one partition"
        )
        return _Pipe(
            body=final,
            columns=columns,
            parts=cut.partitions,
            inputs=(item,),
            hashes=self.final_hashes(final, keys, cut.strategy, cut.partitions),
            estimate=child.estimate if keys else 0,
            notes=(f"{combined}{grouping} · {spread}",),
        )

    def final_hashes(
        self, final: exp.Select, keys: Sequence[exp.Expression], strategy: str, n: int
    ) -> tuple[Hash, ...]:
        """What a final aggregate's output is partitioned by: its keys, when all projected."""
        if n <= 1 or not keys:
            return ()
        names: list[str] = []
        for index, key in enumerate(keys):
            target = exp.column(f"__k{index}", quoted=True) if strategy == "partial" else key
            found = next(
                (
                    p.alias_or_name
                    for p in final.expressions
                    if p.unalias() == target
                    or (
                        strategy == "partial"
                        and isinstance(p.unalias(), exp.Column)
                        and p.unalias().name == f"__k{index}"
                        and not p.unalias().table
                    )
                ),
                None,
            )
            if found is None:
                return ()
            names.append(found)
        types = dict(self.describe(final))
        return (Hash(tuple(names), tuple(hash_type(types[name]) for name in names), n),)

    # -- joins ---------------------------------------------------------------------------

    def join(self, node: Join) -> _Pipe:
        select = node.select
        joins: list[exp.Join] = list(select.args.get("joins") or [])
        relations = [from_of(select)] + [j.this for j in joins]
        aliases = [relation_alias(r) for r in relations if r is not None]
        kinds = [str(join_kind(j)) for j in joins]

        # Every column the join reads or returns, per relation, under a unique name.
        roots: list[exp.Expression] = list(select.expressions)
        if select.args.get("where") is not None:
            roots.append(select.args["where"])
        roots += [j.args["on"] for j in joins if j.args.get("on") is not None]
        needed: dict[str, list[str]] = {alias: [] for alias in aliases}
        for root in roots:
            for column in root.find_all(exp.Column):
                if column.table not in needed:
                    raise _Refused(
                        f"a column the join cannot place ({column.sql(dialect='duckdb')})"
                    )
                if column.name not in needed[column.table]:
                    needed[column.table].append(column.name)
        counts: dict[str, int] = defaultdict(int)
        for names in needed.values():
            for name in names:
                counts[name.lower()] += 1
        colmap: dict[tuple[str, str], str] = {}
        used: set[str] = set()
        for alias in aliases:
            for name in needed[alias]:
                candidate = name if counts[name.lower()] == 1 else f"{alias}_{name}"
                unique, suffix = candidate, 1
                while unique.lower() in used:
                    unique, suffix = f"{candidate}_{suffix}", suffix + 1
                used.add(unique.lower())
                colmap[(alias, name)] = unique

        def rewrite(expression: exp.Expression) -> exp.Expression:
            def swap(n: exp.Expression) -> exp.Expression:
                if isinstance(n, exp.Column) and (n.table, n.name) in colmap:
                    return exp.column(colmap[(n.table, n.name)], quoted=True)
                return n

            return expression.transform(swap, copy=True)

        # Filters that may run below the join, on one relation's own rows (a copy: the
        # original stays where it is, where it is always correct).
        pushed: dict[str, list[exp.Expression]] = {alias: [] for alias in aliases}
        supplied = null_supplied(aliases, kinds)
        where = select.args.get("where")
        for conjunct in conjuncts(where.this if where is not None else None):
            tables = {c.table for c in conjunct.find_all(exp.Column)}
            if len(tables) == 1 and is_deterministic(conjunct):
                (alias,) = tables
                if alias in pushed and alias not in supplied:
                    pushed[alias].append(conjunct.copy())
        for index, (join, kind) in enumerate(zip(joins, kinds, strict=True), start=1):
            if kind not in ("inner", "left", "semi", "anti"):
                continue
            for conjunct in conjuncts(join.args.get("on")):
                tables = {c.table for c in conjunct.find_all(exp.Column)}
                if tables == {aliases[index]} and is_deterministic(conjunct):
                    pushed[aliases[index]].append(conjunct.copy())

        sides: list[_Pipe] = []
        for child, alias in zip(node.children, aliases, strict=True):
            fragment = self.plan(child)
            projection = [
                _alias(exp.column(name, table=alias, quoted=True), colmap[(alias, name)])
                for name in needed[alias]
            ] or [_alias(exp.true(), self.fresh("__icefleet_row"))]
            side = exp.select(*projection)
            set_from(side, fragment.relation(alias))
            if pushed[alias]:
                side.set("where", exp.Where(this=exp.and_(*pushed[alias])))
            sides.append(
                replace(
                    fragment,
                    body=side,
                    columns=tuple(p.alias_or_name for p in projection),
                    hashes=_renamed_hashes(
                        fragment.hashes, {name: colmap[(alias, name)] for name in needed[alias]}
                    ),
                )
            )

        acc = sides[0]
        for index, (join, kind) in enumerate(zip(joins, kinds, strict=True), start=1):
            on = join.args.get("on")
            keys = equi_keys(on, set(aliases[:index]), aliases[index])
            acc = self.join_two(
                acc,
                sides[index],
                join,
                kind,
                rewrite(on) if on is not None else None,
                [(rewrite(a), rewrite(b)) for a, b in keys],
                shown=[(_short(a), _short(b)) for a, b in keys],
                label=on.sql(dialect="duckdb") if on is not None else "every pair of rows",
            )

        top = exp.select(*[rewrite(p) for p in select.expressions])
        set_from(top, acc.relation(self.fresh("__icefleet_j")))
        if where is not None:
            top.set("where", exp.Where(this=rewrite(where.this)))
        return replace(
            acc,
            body=top,
            columns=tuple(p.alias_or_name for p in top.expressions),
            hashes=_renamed_hashes(acc.hashes, _projected(top, None)),
            estimate=None,
        )

    def join_two(
        self,
        left: _Pipe,
        right: _Pipe,
        node: exp.Join,
        kind: str,
        on: exp.Expression | None,
        keys: Sequence[tuple[exp.Expression, exp.Expression]],
        *,
        shown: Sequence[tuple[str, str]],
        label: str,
    ) -> _Pipe:
        """Join two pipelines: broadcast a side, or shuffle both by the keys (`shown` is
        how each key reads in the query, for explain())."""
        threshold = self.fleet.broadcast_threshold_bytes
        sides = {"left": left, "right": right}
        small = [
            name
            for name in BROADCASTABLE[kind]
            if threshold > 0
            and sides[name].estimate is not None
            and (sides[name].estimate or 0) <= threshold
        ]
        if small:
            order = BROADCASTABLE[kind]
            chosen = min(small, key=lambda name: (sides[name].estimate or 0, order.index(name)))
            stage, built = self.broadcast(sides[chosen])
            if chosen == "left":
                left, right, probe = built, right, right
            else:
                left, right, probe = left, built, left
            note = (
                f"join: {kind}, broadcast hash join · the {chosen} side from stage "
                f"{stage.stage_id} ({format_bytes(sides[chosen].estimate or 0)} est.) · on {label}"
            )
            return replace(
                self.combine(left, right, node, kind, on),
                hashes=probe.hashes,
                notes=(*left.notes, *right.notes, note),
            )

        if not keys:
            raise _Refused(
                f"a {kind} join without an equality condition, and no side it may broadcast is "
                f"under icefleet.broadcast.thresholdBytes ({format_bytes(threshold)})"
            )

        n = self.fleet.shuffle_partitions
        left_types = self.key_types(left, [a for a, _ in keys])
        right_types = self.key_types(right, [b for _, b in keys])
        hash_as: list[str] = []
        for (a, b), lt, rt in zip(shown, left_types, right_types, strict=True):
            canonical = join_hash_type(lt, rt)
            if canonical is None:
                raise _Refused(f"join keys of types that do not hash alike ({a} {lt} = {b} {rt})")
            hash_as.append(canonical)
        left_cols = [a.name if isinstance(a, exp.Column) else None for a, _ in keys]
        right_cols = [b.name if isinstance(b, exp.Column) else None for _, b in keys]

        def reusable(side: _Pipe, cols: Sequence[str | None]) -> tuple[int, ...] | None:
            """The key positions `side` is already hash-partitioned by, if any."""
            if side.parts != n:
                return None
            for item in side.hashes:
                if item.partitions != n:
                    continue
                positions: list[int] = []
                for column, kind_ in zip(item.columns, item.types, strict=True):
                    match = next(
                        (
                            i
                            for i in range(len(keys))
                            if cols[i] == column and hash_as[i] == kind_ and i not in positions
                        ),
                        None,
                    )
                    if match is None:
                        break
                    positions.append(match)
                else:
                    return tuple(positions)
            return None

        reuse_left, reuse_right = reusable(left, left_cols), reusable(right, right_cols)
        reused = ""
        if reuse_left is not None and reuse_right is not None and reuse_left == reuse_right:
            positions = reuse_left
            reused = " · both sides already partitioned by the keys"
        elif reuse_left is not None:
            positions = reuse_left
            right = self.shuffle(
                right,
                [keys[i][1] for i in positions],
                [hash_as[i] for i in positions],
                n,
                label=", ".join(shown[i][1] for i in positions),
            )
            reused = " · the left side already partitioned by the keys"
        elif reuse_right is not None:
            positions = reuse_right
            left = self.shuffle(
                left,
                [keys[i][0] for i in positions],
                [hash_as[i] for i in positions],
                n,
                label=", ".join(shown[i][0] for i in positions),
            )
            reused = " · the right side already partitioned by the keys"
        else:
            positions = tuple(range(len(keys)))
            types = [hash_as[i] for i in positions]
            left = self.shuffle(
                left,
                [keys[i][0] for i in positions],
                types,
                n,
                label=", ".join(shown[i][0] for i in positions),
            )
            right = self.shuffle(
                right,
                [keys[i][1] for i in positions],
                types,
                n,
                label=", ".join(shown[i][1] for i in positions),
            )

        hashed = tuple(hash_as[i] for i in positions)
        by_left = _hash_over(left_cols, positions, hashed, n)
        by_right = _hash_over(right_cols, positions, hashed, n)
        hashes = {
            "inner": by_left + by_right,
            "left": by_left,
            "semi": by_left,
            "anti": by_left,
            "right": by_right,
            "full": (),
        }[kind]
        note = f"join: {kind}, shuffle hash join · on {label} · {n} partitions{reused}"
        return replace(
            self.combine(left, right, node, kind, on),
            hashes=hashes,
            notes=(*left.notes, *right.notes, note),
        )

    def key_types(self, side: _Pipe, keys: Sequence[exp.Expression]) -> list[str]:
        probe = exp.select(*[_alias(k.copy(), f"__key{i}") for i, k in enumerate(keys)]).from_(
            side.relation("__icefleet_k")
        )
        return [kind for _, kind in self.describe(probe)]

    def combine(
        self, left: _Pipe, right: _Pipe, node: exp.Join, kind: str, on: exp.Expression | None
    ) -> _Pipe:
        """`left JOIN right ON on` as one pipeline; both sides must run in the same tasks."""
        if left.scan and right.scan:  # pragma: no cover - one side is always closed first
            raise _Refused("both sides of a join would read a table in one stage")
        if (left.scan or right.scan) and (left.parts or right.parts):  # pragma: no cover
            raise _Refused("a join of a split side with a partitioned side")
        if left.parts and right.parts and left.parts != right.parts:  # pragma: no cover
            raise _Refused("a join of sides with different partition counts")
        join = node.copy()
        join.set("this", right.relation(self.fresh("__icefleet_r")))
        join.set("on", on.copy() if on is not None else None)
        columns = left.columns + (() if kind in ("semi", "anti") else right.columns)
        select = exp.select(*[exp.column(c, quoted=True) for c in columns])
        set_from(select, left.relation(self.fresh("__icefleet_l")))
        select.append("joins", join)
        driver = left if (left.scan or left.parts) else right
        return _Pipe(
            body=select,
            columns=columns,
            scan=left.scan or right.scan,
            alias=driver.alias,
            parts=left.parts or right.parts,
            inputs=left.inputs + right.inputs,
            estimate=None,
        )


# -- the entry point -----------------------------------------------------------------------


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

    scans: dict[str, ScanPlan] = {}
    for node in query.scans:
        scan = next((s for s in compiled.scans if s.source.key == node.key), None)
        if scan is None:
            return Uncuttable(f"no scan was planned for {node.key}")
        if scan.is_empty:
            return Uncuttable(
                f"the scan of {node.key} reads no data files, so there is nothing to distribute"
            )
        if len(scan.groups) != 1:
            return Uncuttable(
                "files with renamed or late-added columns need per-group field-id "
                "aliasing -- Phase 7"
            )
        scans[node.key] = scan

    def typed(tree: exp.Expression) -> list[tuple[str, str]]:
        try:
            return describe(tree)
        except Exception as exc:  # DuckDB's binder speaks for the stage SQL
            raise _Untypable(f"{type(exc).__name__}: {exc}") from exc

    planner = _Planner(
        sources=sources,
        scans=scans,
        fleet=fleet,
        min_splits=min_splits,
        describe=typed,
        register=register,
        aggregate_names=aggregate_names,
    )
    try:
        return _plan(planner, query, list(output_names))
    except _Untypable as exc:
        return Uncuttable(f"the stage planner could not type a stage ({exc})")
    except _Refused as exc:
        return Uncuttable(exc.reason)


def _plan(planner: _Planner, query: Query, output_names: list[str]) -> StagePlan | Uncuttable:
    # The result as the caller will see it, typed before anything is cut.
    result = planner.describe(query.tree)
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

    root = planner.plan(query.root)
    tree = root.select()
    # The result as written: stand-in casts, then each task's own top-n / first-n.
    if any(stand_in[name] != kind for name, kind in result):
        tree = exp.select(
            *[
                _alias(cast_to(exp.column(name, quoted=True), stand_in[name]), name)
                if stand_in[name] != kind
                else exp.column(name, quoted=True)
                for name, kind in result
            ]
        ).from_(tree.subquery("__icefleet_out"))
    if query.limit is not None:
        top = exp.select("*").from_(tree.subquery("__icefleet_top"))
        if query.order:
            top = top.order_by(*[item.copy() for item in query.order])
        tree = top.limit(query.limit)
    per_task = (
        ""
        if query.limit is None
        else f" · {'top' if query.order else 'first'} {query.limit} per task"
    )
    planner.close(
        tree, root, sink="result", partitions=1, notes=(*root.notes, f"-> result{per_task}")
    )
    return StagePlan(
        stages=tuple(planner.stages),
        columns=tuple(output_names),
        restore=restore,
        order=tuple(item.sql(dialect="duckdb") for item in query.order),
        limit=query.limit,
    )
