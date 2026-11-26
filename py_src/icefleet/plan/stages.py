"""`plan_stages()`: a compiled icetl plan in, a `StagePlan` or `Uncuttable(reason)` out.

The planner walks the tree `plan/rel.py` recognised, bottom up, keeping each subtree as an
*open pipeline*: SQL that a task could run, with what drives its tasks --

    a scan       one task per split of an Iceberg table's files
    partitions   one task per hash partition of the stage(s) it reads
    nothing      it reads only broadcast inputs: one task

-- and what its rows are known to be partitioned by. A pipeline grows by wrapping (a
projection, an aggregate in place, a join with a broadcast side, a window over rows
already partitioned by its keys) and is *closed* into a stage where rows must change
hands:

    shuffle    COPY ... PARTITION_BY hash(keys) % n   (an aggregate's partial, a join side,
               a window's or a DISTINCT ON's input, a set operation's branches)
    broadcast  written once, read whole by every task of the other side
    result     the root, which the coordinator reads

    aggregate   partial in the input's pipeline -> shuffle(keys) -> final per partition
                (in place, no shuffle, when the input is already partitioned by a subset
                of the keys); grouping sets in the same one shuffle (`aggregates.py`)
    join        broadcast the small side (PLAN §4.3), or shuffle both sides by the keys,
                skipping a side already partitioned that way
    window      shuffle by the PARTITION BY keys every window shares, else one shuffle per
                group of windows, chained; no PARTITION BY: one partition (decision 18)
    top-n       each task's own top n, then one partition takes the top n
    UNION ALL   no stage of its own: a pipeline of branches, each closed on its own, and
                the reader unions their inputs
    UNION / INTERSECT / EXCEPT   both sides hashed on every column, then per partition
    ORDER BY    at the top, without a LIMIT, over more than icefleet.sort.coordinatorBytes:
                a range sort (`plan/sort.py`)
    a write     (Phase 5) the result cast to the table's types; a partitioned table's rows
                shuffled by their partition values first, so each value is written by one
                task; every final task writes Iceberg data files (`write/target.py`).
                A rewrite whose rows cannot change partition (Phase 5b) skips the shuffle:
                each task writes a file per partition value it holds
    a probe     (Phase 5b) the distinct data files of one scan holding a row its filter
                keeps: DuckDB's `filename` column, added to that scan's relation

A scan can be *restricted* to an allow-list of its files (Phase 5b: a copy-on-write
rewrite reads exactly the files it replaces); a plan that would not read every allowed
file -- icetl's pruning dropped one -- is Uncuttable, never quietly narrower.

Every table *reference* is its own split input, so a self-join reads its table twice,
in two stages, and a stage never binds two file lists to one icetl parameter. A stage's
SQL is built from the optimized tree's own nodes and rendered once through icetl's
`substitute_sources` + `build_source`, as icetl renders its own query. An input from
another stage is a table named `__icefleet_input_<k>__`, which each task swaps for
`read_parquet($icefleet_input_<k>)` over its files -- its partition's, all of them for a
broadcast, one upstream task's for a range partitioning -- or for a typed empty
relation when no rows reached the partition.
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
    grouping_sets_of,
)
from icefleet.plan.joins import BROADCASTABLE, conjuncts, equi_keys, is_deterministic, null_supplied
from icefleet.plan.partitioning import Hash, hash_type, join_hash_type, partition_expression
from icefleet.plan.rel import (
    Aggregate,
    DistinctOn,
    Join,
    Project,
    Query,
    RelNode,
    Scan,
    SetOp,
    TopN,
    Window,
    from_of,
    from_sqlglot,
    join_kind,
    relation_alias,
    set_from,
    windows_of,
)
from icefleet.plan.sort import (
    RANGE_TOKEN,
    RangeSpec,
    boundaries,
    range_expression,
    sample_query,
    sort_keys,
    sortable_by_range,
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
    from icefleet.write.target import WriteTarget

__all__ = [
    "FILE_COLUMN",
    "Fetch",
    "Stage",
    "StageInput",
    "StagePlan",
    "parquet_unsafe_type",
    "plan_stages",
]

_INPUT_PREFIX = "__icefleet_input_"

#: The column a file probe adds to its scan's relation: the data file a row came from.
FILE_COLUMN = "__icefleet_file"

#: Runs a query on the coordinator: (sql, parameters) -> rows. A range sort's boundaries.
Fetch = Callable[[str, Mapping[str, Any]], list[tuple[Any, ...]]]


def _read(parameter: str) -> str:
    return f"read_parquet(${parameter}, union_by_name => false, hive_partitioning => false)"


def _quote(name: str) -> str:
    return exp.to_identifier(name, quoted=True).sql(dialect="duckdb")


@dataclass(frozen=True)
class StageInput:
    """Another stage's output, read by this one.

    `partition`: each task reads its own hash partition of it. `all`: every task reads
    all of it (a broadcast). `task`: a task per upstream task, each reading that task's
    files (a range partitioning). `empty` is the typed relation with no rows a task reads
    instead when its share has no files.
    """

    stage_id: int
    mode: str  # "partition" | "all" | "task"
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
        return _quote(self.placeholder)


@dataclass(frozen=True)
class Stage:
    """One stage: its SQL, what drives its tasks, what it reads, where it writes.

    A stage with a `scan` has a task per split; one with a `task` input a task per
    upstream task; otherwise a task per partition some partitioned input has rows in
    (partition 0 alone when none has, or when it has no partitioned input at all).
    A stage with a `range` has its partition expression filled in at run time, from
    the samples its inputs wrote (`resolved`).
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
    sample_columns: tuple[str, ...] = ()  # a range sort's keys, sampled by every task
    sample_rows: int = 0
    range: RangeSpec | None = None
    write: WriteTarget | None = None  # an `iceberg_files` sink writes into this table

    @property
    def table(self) -> str:
        assert self.scan is not None
        return str(self.scan.source.resolved.qualified_name)

    @property
    def depends_on(self) -> tuple[int, ...]:
        return tuple(sorted({i.stage_id for i in self.inputs}))

    def resolved(self, upstream: Sequence[TaskResult], fetch: Fetch) -> Stage:
        """This stage with its range boundaries computed from the samples upstream."""
        if self.range is None:
            return self
        files = [
            f.path
            for r in upstream
            if r.stage_id in self.range.sample_stages
            for f in r.manifest
            if f.sample
        ]
        rows: list[tuple[Any, ...]] = []
        if files:
            sql, parameters = sample_query(self.range, files)
            rows = fetch(sql, parameters)
        bounds = boundaries(rows, self.range.partitions)
        return replace(
            self, sql=self.sql.replace(RANGE_TOKEN, range_expression(self.range, bounds))
        )

    def tasks(
        self,
        query_id: str,
        exchange: Exchange,
        upstream: Sequence[TaskResult] = (),
        *,
        target_file_bytes: int,
    ) -> list[TaskSpec]:
        """The `TaskSpec`s of this stage, built from its inputs' manifests -- never a glob."""
        if RANGE_TOKEN in self.sql:  # pragma: no cover - the scheduler resolves it first
            raise ValueError(f"stage {self.stage_id}: range boundaries were never filled in")
        results: dict[int, list[TaskResult]] = defaultdict(list)
        for result in upstream:
            results[result.stage_id].append(result)
        whole: dict[int, tuple[str, ...]] = {}
        parts: dict[int, dict[int, list[str]]] = {}
        per_task: list[tuple[int, tuple[str, ...]]] = []  # (input stage, one task's files)
        for item in self.inputs:
            ordered = sorted(results[item.stage_id], key=lambda r: r.task_index)
            if item.mode == "all":
                whole[item.stage_id] = tuple(f.path for r in ordered for f in r.data)
                if not whole[item.stage_id]:  # pragma: no cover - a result sink always writes
                    raise ValueError(f"stage {item.stage_id} wrote no files to broadcast")
            elif item.mode == "task":
                per_task += [(item.stage_id, tuple(f.path for f in r.data)) for r in ordered]
            else:
                files: dict[int, list[str]] = defaultdict(list)
                for r in ordered:
                    for output in r.data:
                        files[output.partition or 0].append(output.path)
                parts[item.stage_id] = files

        def spec(
            index: int,
            partition: int,
            extra: Mapping[str, InputSpec],
            one: tuple[int, tuple[str, ...]] | None = None,
        ) -> TaskSpec:
            sql = self.sql
            inputs: dict[str, InputSpec] = dict(extra)
            for item in self.inputs:
                if item.mode == "all":
                    paths = whole[item.stage_id]
                elif item.mode == "task":
                    paths = one[1] if one is not None and one[0] == item.stage_id else ()
                else:
                    paths = tuple(parts[item.stage_id].get(partition, ()))
                if paths:
                    sql = sql.replace(item.token, _read(item.parameter))
                    inputs[item.parameter] = InputSpec(paths=paths)
                else:
                    sql = sql.replace(item.token, item.empty)
            if self.write is not None:
                # The table's own data directory, a prefix per query: removed whole if the
                # write fails, referenced by the table once it commits.
                sink = SinkSpec(
                    kind="iceberg_files",
                    location=f"{self.write.location}/{query_id}/stage-{self.stage_id}/task-{index}",
                    target_file_bytes=self.write.target_file_bytes,
                    field_ids=self.write.field_ids,
                    partition_columns=self.write.hidden,
                )
            else:
                sink = SinkSpec(
                    kind=self.sink,
                    location=exchange.task_location(query_id, self.stage_id, index),
                    target_file_bytes=target_file_bytes,
                    partitions=self.partitions,
                    sample_columns=self.sample_columns,
                    sample_rows=self.sample_rows,
                )
            return TaskSpec(
                query_id=query_id,
                stage_id=self.stage_id,
                task_index=index,
                sql=sql,
                inputs=inputs,
                sink=sink,
            )

        if self.scan is not None:
            assert self.parameter is not None
            return [
                spec(index, 0, {self.parameter: InputSpec(paths=split.paths)})
                for index, split in enumerate(self.splits)
            ]
        if per_task:
            return [spec(index, 0, {}, one) for index, one in enumerate(per_task)]
        # A partition no row reached would compute nothing, so it gets no task (kickoff
        # answer 3 of Phase 2) -- except that when no partition has rows at all, partition 0
        # runs over empty inputs, so the result keeps its schema and a global aggregate
        # its one row.
        chosen = sorted({p for files in parts.values() for p, paths in files.items() if paths})
        return [spec(p, p, {}) for p in chosen or [0]]


@dataclass(frozen=True)
class StagePlan:
    """The stages of one query, inputs before readers, and the coordinator's last step.

    `final_stages` are the ones whose files are the result, in order (more than one for
    a top-level UNION ALL). `ranged` says the result is already in order across them: a
    range sort, read partition by partition, with nothing left to sort.
    """

    stages: tuple[Stage, ...]
    columns: tuple[str, ...] = ()  # the result columns the caller sees
    restore: Mapping[str, str] = field(default_factory=dict)  # column -> type to cast back to
    order: tuple[str, ...] = ()  # ORDER BY items over result columns, DuckDB SQL
    limit: int | None = None
    offset: int = 0
    final_stages: tuple[int, ...] = ()
    ranged: bool = False
    write: WriteTarget | None = None  # the final stages write Iceberg data files into it

    @property
    def final(self) -> Stage:
        return self.stages[-1]

    @property
    def finals(self) -> tuple[int, ...]:
        return self.final_stages or (self.final.stage_id,)

    def coordinator_note(self) -> str | None:
        parts = []
        if self.ranged:
            parts.append("reads the range partitions in order, sorts nothing")
        if self.order:
            parts.append("order by " + ", ".join(self.order))
        if self.limit is not None:
            parts.append(f"limit {self.limit}")
        if self.offset:
            parts.append(f"offset {self.offset}")
        if len(self.finals) > 1:
            parts.append(f"concatenates stages {', '.join(str(s) for s in self.finals)}")
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


def _column(name: str, table: str | None = None) -> exp.Column:
    return exp.column(name, table=table, quoted=True)


def _short(expression: exp.Expression) -> str:
    """SQL for `explain()`, without the table qualifiers."""
    copy = expression.copy()
    for column in copy.find_all(exp.Column):
        column.set("table", None)
    return copy.sql(dialect="duckdb")


def _unqualified(expression: exp.Expression, alias: str) -> exp.Expression:
    """`expression` with `alias.` dropped from its columns (to read a narrowed pipeline)."""
    copy = expression.copy()
    for column in copy.find_all(exp.Column):
        if column.table == alias:
            column.set("table", None)
    return copy


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

    `body` is an `exp.Table` (an Iceberg table reference) or an `exp.Select`. `scan` /
    `parts` say what drives the tasks (neither: a single task over broadcast inputs).
    `columns` are its output names (empty only for a bare table reference); `hashes`
    what its rows are partitioned by; `estimate` an upper bound of its size in parquet
    bytes, or None when unknown (a join's output). A pipeline with `branches` is their
    UNION ALL: each branch runs in its own stages, and `body` means nothing.
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
    branches: tuple[_Pipe, ...] = ()

    def relation(self, alias: str) -> exp.Expression:
        """The pipeline as a FROM item named `alias`."""
        assert not self.branches, "a UNION ALL pipeline has no one relation"
        if isinstance(self.body, exp.Table):
            table = self.body.copy()
            table.set("alias", _quoted_alias(alias))
            return table
        return exp.Subquery(this=self.body.copy(), alias=_quoted_alias(alias))

    def select(self) -> exp.Select:
        assert not self.branches, "a UNION ALL pipeline has no one SELECT"
        if isinstance(self.body, exp.Select):
            return self.body
        return exp.select(exp.Star()).from_(self.relation("__icefleet_t"))

    def leaves(self) -> tuple[_Pipe, ...]:
        return self.branches or (self,)


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


def _columns_of(select: exp.Select, alias: str) -> list[str]:
    """The columns of relation `alias` that `select` reads, in first-seen order."""
    roots: list[exp.Expression] = list(select.expressions)
    for arg in ("where", "qualify", "distinct", "order"):
        if select.args.get(arg) is not None:
            roots.append(select.args[arg])
    names: list[str] = []
    for root in roots:
        for column in root.find_all(exp.Column):
            if column.table == alias and column.name not in names:
                names.append(column.name)
    return names


def _function_names(windows: Sequence[exp.Window]) -> str:
    names = []
    for window in windows:
        function = window.this
        while isinstance(function, (exp.IgnoreNulls, exp.RespectNulls)):
            function = function.this
        text = function.sql(dialect="duckdb")
        names.append(text.split("(", 1)[0].lower())
    return ", ".join(dict.fromkeys(names))


@dataclass
class _WindowGroup:
    keys: list[exp.Expression]
    texts: set[str]
    windows: list[exp.Window]


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
        restrict: Mapping[str, frozenset[str]] | None = None,
        file_key: str | None = None,
    ) -> None:
        self.sources = sources
        self.scans = scans
        self.fleet = fleet
        self.min_splits = min_splits
        self._describe = describe
        self.register = register
        self.aggregate_names = aggregate_names
        self.restrict = dict(restrict or {})  # scan key -> the only files it may read
        self.file_key = file_key  # the scan whose relation carries FILE_COLUMN
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

    def describe_pipe(self, pipe: _Pipe) -> list[tuple[str, str]]:
        return self.describe(pipe.leaves()[0].select())

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
            paths = list(scan.groups[0].paths)
            if key in self.restrict:
                paths = [p for p in paths if p in self.restrict[key]]
            self._splits[key] = plan_splits(
                file_weights(scan, paths),
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

        def relation(source: Any) -> exp.Expression:
            built = build_source(scan, parameters=parameters, register=self.register)
            if scan_key == self.file_key:
                if not isinstance(built, exp.Select):  # pragma: no cover - one group, files
                    raise _Refused("a file probe over a scan that is not one read_parquet")
                built = built.select(
                    exp.alias_(exp.column("filename"), FILE_COLUMN, quoted=True), copy=False
                )
            return built

        bound = substitute_sources(tree, self.sources, relation)
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
        sample: tuple[str, ...] = (),
        write: WriteTarget | None = None,
    ) -> Stage:
        """Make `tree` -- `pipe`'s work, ready to write -- a stage of its own."""
        assert not pipe.branches
        sql, parameter = self.render(tree, pipe.scan)
        for item in pipe.inputs:
            if sql.count(item.token) != 1:  # pragma: no cover - one FROM reads each input
                raise _Refused(f"a stage does not read stage {item.stage_id} exactly once")
        return self.add(
            Stage(
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
                sample_columns=sample,
                sample_rows=self.fleet.sort_sample_rows if sample else 0,
                write=write,
            )
        )

    def add(self, stage: Stage) -> Stage:
        assert stage.stage_id == len(self.stages)
        self.stages.append(stage)
        return stage

    def reader(
        self,
        stages: Sequence[Stage],
        *,
        mode: str,
        written: Sequence[tuple[str, str]],
        restore: Mapping[str, str],
        columns: tuple[str, ...],
        estimate: int | None,
        partitions: int | None,
        hashes: tuple[Hash, ...] = (),
    ) -> _Pipe:
        """A pipeline reading the output of `stages` (UNION ALL of them), stand-ins cast back."""
        items = [
            StageInput(stage_id=s.stage_id, mode=mode, empty=_empty_relation(written))
            for s in stages
        ]
        selects = []
        for item in items:
            self.schemas[item.placeholder] = list(written)
            table = exp.Table(this=exp.to_identifier(item.placeholder, quoted=True))
            selects.append(
                exp.select(
                    *[
                        _alias(cast_to(_column(name), restore[name]), name)
                        if name in restore
                        else _column(name)
                        for name, _ in written
                    ]
                ).from_(table)
            )
        body: exp.Select = selects[0]
        if len(selects) > 1:
            union: exp.Expression = selects[0]
            for other in selects[1:]:
                union = exp.union(union, other, distinct=False)
            body = exp.select(*[_column(name) for name, _ in written]).from_(
                exp.Subquery(this=union, alias=_quoted_alias(self.fresh("__icefleet_u")))
            )
        return _Pipe(
            body=body,
            columns=columns,
            parts=partitions,
            inputs=tuple(items),
            hashes=hashes,
            estimate=estimate,
        )

    def written_of(self, pipe: _Pipe) -> tuple[list[tuple[str, str]], dict[str, str]]:
        probe = exp.select(*[_column(c) for c in pipe.columns]).from_(
            pipe.leaves()[0].relation("__icefleet_s")
        )
        return self.stand_ins(probe)

    def shuffle(
        self,
        side: _Pipe,
        keys: Sequence[exp.Expression],
        hash_as: Sequence[str],
        n: int,
        *,
        label: str | None = None,
    ) -> _Pipe:
        """Close `side` (each branch) with a shuffle by `keys` over its columns; read it
        back per partition."""
        written, restore = self.written_of(side)
        label = label or ", ".join(_short(k) for k in keys)
        note = f"-> shuffle hash({label}) x {n}" if n > 1 else "-> shuffle to one partition"
        stages = []
        for leaf in side.leaves():
            relation = leaf.relation("__icefleet_s")
            projection: list[exp.Expression] = [
                _alias(cast_to(_column(name), kind), name) if name in restore else _column(name)
                for name, kind in written
            ]
            if n > 1:
                projection.append(
                    _alias(partition_expression(keys, n, hash_as=hash_as), PARTITION_COLUMN)
                )
            tree = exp.select(*projection).from_(relation)
            stages.append(
                self.close(tree, leaf, sink="shuffle", partitions=n, notes=(*leaf.notes, note))
            )
        hashes: tuple[Hash, ...] = ()
        if n > 1 and all(isinstance(k, exp.Column) for k in keys):
            hashes = (Hash(tuple(k.name for k in keys), tuple(hash_as), n),)
        return self.reader(
            stages,
            mode="partition",
            written=written,
            restore=restore,
            columns=side.columns,
            estimate=side.estimate,
            partitions=n,
            hashes=hashes,
        )

    def broadcast(self, side: _Pipe) -> tuple[list[Stage], _Pipe]:
        """Close `side` into result files that every task of the other side reads whole."""
        written, restore = self.written_of(side)
        stages = []
        for leaf in side.leaves():
            projection = [
                _alias(cast_to(_column(name), kind), name) if name in restore else _column(name)
                for name, kind in written
            ]
            tree = exp.select(*projection).from_(leaf.relation("__icefleet_b"))
            stages.append(
                self.close(
                    tree, leaf, sink="result", partitions=1, notes=(*leaf.notes, "-> broadcast")
                )
            )
        return stages, self.reader(
            stages,
            mode="all",
            written=written,
            restore=restore,
            columns=side.columns,
            estimate=side.estimate,
            partitions=None,
        )

    # -- UNION ALL pipelines -------------------------------------------------------------

    def union(self, pipes: Sequence[_Pipe]) -> _Pipe:
        leaves = tuple(leaf for pipe in pipes for leaf in pipe.leaves())
        if len(leaves) == 1:
            return leaves[0]
        estimates = [leaf.estimate for leaf in leaves]
        return _Pipe(
            body=leaves[0].body,
            columns=leaves[0].columns,
            estimate=None if any(e is None for e in estimates) else sum(e or 0 for e in estimates),
            branches=leaves,
        )

    def each(self, pipe: _Pipe, step: Callable[[_Pipe], _Pipe]) -> _Pipe:
        """`step` on every branch of a UNION ALL pipeline, or on the pipeline."""
        if pipe.branches:
            return self.union([step(leaf) for leaf in pipe.branches])
        return step(pipe)

    def conform(self, pipe: _Pipe, result: Sequence[tuple[str, str]]) -> _Pipe:
        """Every branch as the set operation's result: its names, its types, by position."""

        def step(leaf: _Pipe) -> _Pipe:
            types = self.describe(leaf.select())
            if len(types) != len(result):  # pragma: no cover - the binder already refused it
                raise _Refused("set operation branches of different widths")
            projection = [
                _alias(cast_to(_column(own), kind) if own_kind != kind else _column(own), name)
                for (own, own_kind), (name, kind) in zip(types, result, strict=True)
            ]
            select = exp.select(*projection).from_(leaf.relation(self.fresh("__icefleet_c")))
            return replace(leaf, body=select, columns=tuple(n for n, _ in result), hashes=())

        return self.each(pipe, step)

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
        if isinstance(node, Join):
            return self.join(node)
        if isinstance(node, Window):
            return self.window(node)
        if isinstance(node, DistinctOn):
            return self.distinct_on(node)
        if isinstance(node, TopN):
            return self.top_n(node)
        return self.setop(node)

    def project(self, node: Project) -> _Pipe:
        child = self.plan(node.child)
        alias = relation_alias(from_of(node.select))  # type: ignore[arg-type]

        def step(leaf: _Pipe) -> _Pipe:
            select = node.select.copy()
            set_from(select, leaf.relation(alias))
            return replace(
                leaf,
                body=select,
                columns=tuple(p.alias_or_name for p in select.expressions),
                hashes=_renamed_hashes(leaf.hashes, _projected(select, alias)),
            )

        return self.each(child, step)

    def aggregate(self, node: Aggregate) -> _Pipe:
        child = self.plan(node.child)
        alias = relation_alias(from_of(node.select))  # type: ignore[arg-type]
        leaves = child.leaves()
        work = node.select.copy()
        set_from(work, leaves[0].relation(alias))
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
        if (
            keys
            and child.parts is not None
            and not child.branches
            and grouping_sets_of(work) is None
        ):
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
        if cut.grouping_sets:
            work_note += f" · {cut.grouping_sets} grouping sets, one shuffle"
        shuffle_note = (
            f"-> shuffle hash({label}) x {cut.partitions}"
            if cut.partitions > 1
            else "-> shuffle to one partition"
        )
        stages = []
        for leaf in leaves:
            partial = cut.partial.copy()
            set_from(partial, leaf.relation(alias))
            stages.append(
                self.close(
                    partial,
                    leaf,
                    sink="shuffle",
                    partitions=cut.partitions,
                    notes=(*leaf.notes, work_note, shuffle_note),
                )
            )
        items = [
            StageInput(
                stage_id=s.stage_id, mode="partition", empty=_empty_relation(cut.shuffle_columns)
            )
            for s in stages
        ]
        for item in items:
            self.schemas[item.placeholder] = list(cut.shuffle_columns)
        final = _bind_input(cut.final.copy(), items, self.fresh("__icefleet_u"))

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
            inputs=tuple(items),
            hashes=()
            if cut.grouping_sets
            else self.final_hashes(final, keys, cut.strategy, cut.partitions),
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
            found = None
            for projection in final.expressions:
                target = projection.unalias()
                if strategy == "partial":
                    hit = (
                        isinstance(target, exp.Column)
                        and target.name == f"__k{index}"
                        and not target.table
                    )
                else:
                    hit = target == key
                if hit:
                    found = projection.alias_or_name
                    break
            if found is None:
                return ()
            names.append(found)
        types = dict(self.describe(final))
        return (Hash(tuple(names), tuple(hash_type(types[name]) for name in names), n),)

    # -- co-location: windows, DISTINCT ON ---------------------------------------------

    def narrow(self, child: _Pipe, alias: str, select: exp.Select) -> _Pipe:
        """`child` cut to the columns `select` reads from it, with `select`'s WHERE (rows
        are filtered before they move; a window or DISTINCT ON sees them filtered)."""
        names = _columns_of(select, alias)
        where = select.args.get("where")

        def step(leaf: _Pipe) -> _Pipe:
            projection = [_alias(_column(name, alias), name) for name in names] or [
                _alias(exp.true(), self.fresh("__icefleet_row"))
            ]
            side = exp.select(*projection)
            set_from(side, leaf.relation(alias))
            if where is not None:
                side.set("where", where.copy())
            return replace(
                leaf,
                body=side,
                columns=tuple(p.alias_or_name for p in projection),
                hashes=_renamed_hashes(leaf.hashes, {n: n for n in names}),
            )

        return self.each(child, step)

    def colocate(
        self, pipe: _Pipe, keys: Sequence[exp.Expression], alias: str, what: str
    ) -> tuple[_Pipe, str]:
        """`pipe` with rows of equal `keys` (over `alias`'s columns) in one partition; no
        keys: one partition. Returns the pipeline and what explain() says about it."""
        plain = [_unqualified(k, alias) for k in keys]
        if not keys:
            if pipe.parts == 1 and not pipe.branches:
                return pipe, f"{what} · one partition"
            shuffled = self.shuffle(pipe, [], [], 1)
            return (
                shuffled,
                f"{what} · no PARTITION BY: one partition, the whole input in one task "
                "(decision 18)",
            )
        columns = {k.name for k in plain if isinstance(k, exp.Column)}
        if pipe.parts is not None and not pipe.branches:
            for known in pipe.hashes:
                if known.partitions == pipe.parts and set(known.columns) <= columns:
                    return pipe, (
                        f"{what} · partition by {', '.join(_short(k) for k in keys)} "
                        f"(rows already partitioned by hash({', '.join(known.columns)}))"
                    )
        types = self.key_types(pipe, plain)
        n = self.fleet.shuffle_partitions
        shuffled = self.shuffle(
            pipe, plain, [hash_type(t) for t in types], n, label=", ".join(_short(k) for k in keys)
        )
        return (
            shuffled,
            f"{what} · partition by {', '.join(_short(k) for k in keys)} · {n} partitions",
        )

    def window(self, node: Window) -> _Pipe:
        child = self.plan(node.child)
        alias = relation_alias(from_of(node.select))  # type: ignore[arg-type]
        work = node.select.copy()
        windows = windows_of(work)
        qualify = work.args.get("qualify")
        in_qualify = {id(w) for w in qualify.find_all(exp.Window)} if qualify is not None else set()

        # Group the windows: one shuffle by the keys they all share, else one per group.
        groups: list[_WindowGroup] = []
        for window in windows:
            keys = list(window.args.get("partition_by") or [])
            texts = {k.sql(dialect="duckdb") for k in keys}
            for group in groups:
                if (group.texts and texts and group.texts & texts) or (
                    not group.texts and not texts
                ):
                    group.texts &= texts
                    group.keys = [k for k in group.keys if k.sql(dialect="duckdb") in group.texts]
                    group.windows.append(window)
                    break
            else:
                groups.append(
                    _WindowGroup(keys=[k.copy() for k in keys], texts=texts, windows=[window])
                )
        # QUALIFY needs a window of its own in the last SELECT: its group goes last.
        groups.sort(key=lambda g: any(id(w) in in_qualify for w in g.windows))

        pipe = self.narrow(child, alias, work)
        notes: list[str] = []
        for group in groups[:-1]:
            pipe, note = self.colocate(
                pipe, group.keys, alias, f"window: {_function_names(group.windows)}"
            )
            computed: list[tuple[exp.Window, str]] = [
                (w, self.fresh("__icefleet_w")) for w in group.windows
            ]

            def step(
                leaf: _Pipe, computed: list[tuple[exp.Window, str]] = computed, note: str = note
            ) -> _Pipe:
                select = exp.select(
                    *[_column(c, alias) for c in leaf.columns],
                    *[_alias(w.copy(), name) for w, name in computed],
                )
                set_from(select, leaf.relation(alias))
                return replace(
                    leaf,
                    body=select,
                    columns=(*leaf.columns, *[name for _, name in computed]),
                    notes=(*leaf.notes, note),
                )

            pipe = self.each(pipe, step)
            for window, name in computed:
                window.replace(_column(name, alias))
            notes.append(note)
        last = groups[-1]
        pipe, note = self.colocate(
            pipe, last.keys, alias, f"window: {_function_names(last.windows)}"
        )
        work.set("where", None)
        set_from(work, pipe.relation(alias))
        columns = tuple(p.alias_or_name for p in work.expressions)
        return replace(
            pipe,
            body=work,
            columns=columns,
            hashes=_renamed_hashes(pipe.hashes, _projected(work, alias)),
            estimate=child.estimate,
            notes=(*pipe.notes, note),
        )

    def distinct_on(self, node: DistinctOn) -> _Pipe:
        child = self.plan(node.child)
        alias = relation_alias(from_of(node.select))  # type: ignore[arg-type]
        work = node.select.copy()
        on = work.args["distinct"].args["on"]
        keys = list(on.expressions) if isinstance(on, exp.Tuple) else [on]
        pipe = self.narrow(child, alias, work)
        pipe, note = self.colocate(pipe, keys, alias, "distinct on")
        work.set("where", None)
        set_from(work, pipe.relation(alias))
        return replace(
            pipe,
            body=work,
            columns=tuple(p.alias_or_name for p in work.expressions),
            hashes=_renamed_hashes(pipe.hashes, _projected(work, alias)),
            estimate=child.estimate,
            notes=(*pipe.notes, note),
        )

    def top_n(self, node: TopN) -> _Pipe:
        child = self.plan(node.child)
        order = [item.copy() for item in node.order]
        keep = None if node.limit is None else node.limit + node.offset
        what = (
            f"top {node.limit}"
            if node.order and node.limit is not None
            else f"first {node.limit}"
            if node.limit is not None
            else "everything"
        )
        if node.offset:
            what += f" after {node.offset}"

        def final_over(pipe: _Pipe) -> exp.Select:
            select = exp.select(*[_column(c) for c in node.columns]).from_(
                pipe.relation(self.fresh("__icefleet_t"))
            )
            if order:
                select = select.order_by(*[item.copy() for item in order])
            if node.limit is not None:
                select = select.limit(node.limit)
            if node.offset:
                select = select.offset(node.offset)
            return select

        if child.parts == 1 and not child.branches:  # already in one partition
            return replace(
                child,
                body=final_over(child),
                columns=node.columns,
                hashes=(),
                notes=(*child.notes, f"{what}, in place"),
            )

        def local(leaf: _Pipe) -> _Pipe:
            if keep is None:
                return leaf
            select = exp.select(*[_column(c) for c in leaf.columns]).from_(
                leaf.relation(self.fresh("__icefleet_t"))
            )
            if order:
                select = select.order_by(*[item.copy() for item in order])
            select = select.limit(keep)
            return replace(
                leaf,
                body=select,
                notes=(*leaf.notes, f"{'top' if order else 'first'} {keep} per task"),
            )

        gathered = self.shuffle(self.each(child, local), [], [], 1)
        return replace(
            gathered,
            body=final_over(gathered),
            columns=node.columns,
            hashes=(),
            estimate=child.estimate,
            notes=(f"{what} · one partition",),
        )

    def setop(self, node: SetOp) -> _Pipe:
        result = self.describe(node.node)
        left = self.conform(self.plan(node.left), result)
        right = self.conform(self.plan(node.right), result)
        if node.kind == "union" and not node.distinct:
            return self.union([left, right])
        names = [name for name, _ in result]
        types = [hash_type(kind) for _, kind in result]
        n = self.fleet.shuffle_partitions
        keys = [_column(name) for name in names]
        label = "every column"
        left = self.shuffle(left, keys, types, n, label=label)
        right = self.shuffle(right, keys, types, n, label=label)
        operation: type[exp.SetOperation] = {
            "union": exp.Union,
            "intersect": exp.Intersect,
            "except": exp.Except,
        }[node.kind]
        combined = operation(
            this=left.select().copy(), expression=right.select().copy(), distinct=node.distinct
        )
        body = exp.select(*[_column(name) for name in names]).from_(
            exp.Subquery(this=combined, alias=_quoted_alias(self.fresh("__icefleet_x")))
        )
        word = f"{node.kind}{'' if node.distinct else ' all'}"
        return _Pipe(
            body=body,
            columns=tuple(names),
            parts=n,
            inputs=left.inputs + right.inputs,
            hashes=(Hash(tuple(names), tuple(types), n),),
            estimate=None
            if left.estimate is None or right.estimate is None
            else left.estimate + right.estimate,
            notes=(f"{word} · per partition of hash(every column) x {n}",),
        )

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
                    return _column(colmap[(n.table, n.name)])
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
                _alias(_column(name, alias), colmap[(alias, name)]) for name in needed[alias]
            ] or [_alias(exp.true(), self.fresh("__icefleet_row"))]

            def step(
                leaf: _Pipe,
                alias: str = alias,
                projection: list[exp.Alias] = projection,
            ) -> _Pipe:
                side = exp.select(*[p.copy() for p in projection])
                set_from(side, leaf.relation(alias))
                if pushed[alias]:
                    side.set("where", exp.Where(this=exp.and_(*[c.copy() for c in pushed[alias]])))
                return replace(
                    leaf,
                    body=side,
                    columns=tuple(p.alias_or_name for p in projection),
                    hashes=_renamed_hashes(
                        leaf.hashes, {name: colmap[(alias, name)] for name in needed[alias]}
                    ),
                )

            sides.append(self.each(fragment, step))

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

        def top_of(leaf: _Pipe) -> _Pipe:
            top = exp.select(*[rewrite(p) for p in select.expressions])
            set_from(top, leaf.relation(self.fresh("__icefleet_j")))
            if where is not None:
                top.set("where", exp.Where(this=rewrite(where.this)))
            return replace(
                leaf,
                body=top,
                columns=tuple(p.alias_or_name for p in top.expressions),
                hashes=_renamed_hashes(leaf.hashes, _projected(top, None)),
                estimate=None,
            )

        return self.each(acc, top_of)

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
            stages, built = self.broadcast(sides[chosen])
            if chosen == "left":
                left, right, probe = built, right, right
            else:
                left, right, probe = left, built, left
            note = (
                f"join: {kind}, broadcast hash join · the {chosen} side from stage "
                f"{', '.join(str(s.stage_id) for s in stages)} "
                f"({format_bytes(sides[chosen].estimate or 0)} est.) · on {label}"
            )
            return self.combine(left, right, node, kind, on, hashes=probe.hashes, note=note)

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
            if side.parts != n or side.branches:
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

        def shuffled(side: _Pipe, which: int, positions: Sequence[int]) -> _Pipe:
            return self.shuffle(
                side,
                [keys[i][which] for i in positions],
                [hash_as[i] for i in positions],
                n,
                label=", ".join(shown[i][which] for i in positions),
            )

        reuse_left, reuse_right = reusable(left, left_cols), reusable(right, right_cols)
        reused = ""
        if reuse_left is not None and reuse_right is not None and reuse_left == reuse_right:
            positions: tuple[int, ...] = reuse_left
            reused = " · both sides already partitioned by the keys"
        elif reuse_left is not None:
            positions = reuse_left
            right = shuffled(right, 1, positions)
            reused = " · the left side already partitioned by the keys"
        elif reuse_right is not None:
            positions = reuse_right
            left = shuffled(left, 0, positions)
            reused = " · the right side already partitioned by the keys"
        else:
            positions = tuple(range(len(keys)))
            left = shuffled(left, 0, positions)
            right = shuffled(right, 1, positions)

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
        return self.combine(left, right, node, kind, on, hashes=hashes, note=note)

    def key_types(self, side: _Pipe, keys: Sequence[exp.Expression]) -> list[str]:
        probe = exp.select(*[_alias(k.copy(), f"__key{i}") for i, k in enumerate(keys)]).from_(
            side.leaves()[0].relation("__icefleet_k")
        )
        return [kind for _, kind in self.describe(probe)]

    def combine(
        self,
        left: _Pipe,
        right: _Pipe,
        node: exp.Join,
        kind: str,
        on: exp.Expression | None,
        *,
        hashes: tuple[Hash, ...],
        note: str,
    ) -> _Pipe:
        """`left JOIN right ON on` as one pipeline per branch of a UNION ALL probe side;
        both sides must run in the same tasks."""
        if left.branches:
            return self.union(
                [
                    self.combine(b, right, node, kind, on, hashes=(), note=note)
                    for b in left.branches
                ]
            )
        if right.branches:
            return self.union(
                [
                    self.combine(left, b, node, kind, on, hashes=(), note=note)
                    for b in right.branches
                ]
            )
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
        select = exp.select(*[_column(c) for c in columns])
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
            hashes=hashes,
            estimate=None,
            notes=(*left.notes, *right.notes, note),
        )


def _bind_input(final: exp.Select, items: Sequence[StageInput], alias: str) -> exp.Select:
    """Point a final aggregate's input table at its stage input(s) -- a UNION ALL of
    several, one per branch, when the aggregate's input was one."""
    for table in list(final.find_all(exp.Table)):
        if table.name != INPUT_TABLE:
            continue
        if len(items) == 1:
            table.set("this", exp.to_identifier(items[0].placeholder, quoted=True))
            continue
        union: exp.Expression = exp.select("*").from_(
            exp.Table(this=exp.to_identifier(items[0].placeholder, quoted=True))
        )
        for item in items[1:]:
            union = exp.union(
                union,
                exp.select("*").from_(
                    exp.Table(this=exp.to_identifier(item.placeholder, quoted=True))
                ),
                distinct=False,
            )
        own = table.args.get("alias")
        table.replace(
            exp.Subquery(this=union, alias=own.copy() if own is not None else _quoted_alias(alias))
        )
    return final


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
    write: WriteTarget | None = None,
    restrict: Mapping[str, frozenset[str]] | None = None,
    probe: str | None = None,
) -> StagePlan | Uncuttable:
    """Cut `compiled` into stages, or say why it cannot be cut. With `write`, the final
    stages write the result into that table instead of returning it (Phase 5).

    `describe` types a SELECT over the plan's sources (DuckDB's DESCRIBE against icetl's
    zero-row views); `aggregate_names` is DuckDB's catalog of aggregate functions.
    `restrict` limits a scan (by source key) to those of its files; `probe` (a source
    key) plans the query -- a filter over that one scan -- as the distinct files holding
    a row it keeps, in the single column FILE_COLUMN (Phase 5b).
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
        allowed = (restrict or {}).get(node.key)
        if allowed is not None and not allowed <= set(scan.groups[0].paths):
            # A rewrite replaces exactly the files it reads; one it would not read
            # would lose its rows (Phase 5b).
            return Uncuttable("the rewrite would not read every data file it replaces")
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
        restrict=restrict,
        file_key=probe,
    )
    try:
        if probe is not None:
            return _plan_probe(planner, query, probe)
        if write is not None:
            return _plan_write(planner, query, list(output_names), write)
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
    fleet = planner.fleet
    keys = sort_keys(query.order, stand_in) if query.order else ()
    ranged = (
        bool(query.order)
        and query.limit is None
        and (root.estimate is None or root.estimate > fleet.sort_coordinator_bytes)
        and sortable_by_range(keys)
    )
    keep = None if query.limit is None else query.limit + query.offset
    per_task = "" if keep is None else f" · {'top' if query.order else 'first'} {keep} per task"

    def written(leaf: _Pipe) -> exp.Select:
        """The result as written: stand-in casts, then each task's own top-n / first-n."""
        tree = leaf.select()
        if any(stand_in[name] != kind for name, kind in result):
            tree = exp.select(
                *[
                    _alias(cast_to(_column(name), stand_in[name]), name)
                    if stand_in[name] != kind
                    else _column(name)
                    for name, kind in result
                ]
            ).from_(tree.subquery("__icefleet_out"))
        if keep is not None:
            top = exp.select("*").from_(tree.subquery("__icefleet_top"))
            if query.order:
                top = top.order_by(*[item.copy() for item in query.order])
            tree = top.limit(keep)
        return tree

    sample = tuple(key.column for key in keys) if ranged else ()
    sink_note = "-> result, with a sample of the sort keys" if ranged else f"-> result{per_task}"
    finals = [
        planner.close(
            written(leaf),
            leaf,
            sink="result",
            partitions=1,
            notes=(*leaf.notes, sink_note),
            sample=sample,
        )
        for leaf in root.leaves()
    ]
    order = tuple(item.sql(dialect="duckdb") for item in query.order)
    if not ranged:
        return StagePlan(
            stages=tuple(planner.stages),
            columns=tuple(output_names),
            restore=restore,
            order=order,
            limit=query.limit,
            offset=query.offset,
            final_stages=tuple(s.stage_id for s in finals),
        )

    # The range sort: partition every row by the sampled boundaries, then sort each part.
    n = fleet.shuffle_partitions
    columns = [(name, stand_in[name]) for name, _ in result]
    spec = RangeSpec(keys=keys, partitions=n, sample_stages=tuple(s.stage_id for s in finals))
    items = [
        StageInput(stage_id=s.stage_id, mode="task", empty=_empty_relation(columns)) for s in finals
    ]
    listing = ", ".join(_quote(name) for name, _ in columns)
    inner = " UNION ALL ".join(f"SELECT {listing} FROM {item.token}" for item in items)
    ranger = planner.add(
        Stage(
            stage_id=len(planner.stages),
            sql=(
                f'SELECT {listing}, {RANGE_TOKEN} AS "{PARTITION_COLUMN}" '
                f'FROM ({inner}) AS "__icefleet_r"'
            ),
            inputs=tuple(items),
            sink="shuffle",
            partitions=n,
            range=spec,
            notes=(
                f"range partition by {spec.order_sql()} · a task per task of stage "
                f"{', '.join(str(s.stage_id) for s in finals)}; boundaries from their samples",
                f"-> shuffle by range x {n}",
            ),
        )
    )
    part = StageInput(stage_id=ranger.stage_id, mode="partition", empty=_empty_relation(columns))
    sorter = planner.add(
        Stage(
            stage_id=len(planner.stages),
            sql=f"SELECT {listing} FROM {part.token} ORDER BY {spec.order_sql()}",
            inputs=(part,),
            sink="result",
            partitions=1,
            notes=(
                f"sort: {spec.order_sql()} · a task per range partition with rows",
                "-> result",
            ),
        )
    )
    return StagePlan(
        stages=tuple(planner.stages),
        columns=tuple(output_names),
        restore=restore,
        offset=query.offset,
        final_stages=(sorter.stage_id,),
        ranged=True,
    )


def _plan_write(
    planner: _Planner, query: Query, output_names: list[str], target: WriteTarget
) -> StagePlan | Uncuttable:
    """The stages that write the query's result into `target` (PLAN §4.6, kickoff answer 3).

    The result's order means nothing to a table, so a top-level ORDER BY is dropped; a
    LIMIT / OFFSET still decides *which* rows, so it is applied first, through one
    partition. Each branch of the result is cast to the table's columns (a column the
    frame lacks is NULL); a partitioned table's rows are then shuffled by their partition
    values, so every value is written by one task, into one file per task and value.
    """
    node: RelNode = query.root
    if query.limit is not None or query.offset:
        node = TopN(
            child=query.root,
            order=query.order,
            limit=query.limit,
            offset=query.offset,
            columns=tuple(output_names),
        )
    root = planner.plan(node)
    hidden = target.hidden

    def shaped(leaf: _Pipe) -> _Pipe:
        casts = [
            _alias(
                cast_to(_column(source) if source is not None else exp.null(), kind),
                name,
            )
            for (name, kind), source in zip(target.columns, target.sources, strict=True)
        ]
        select = exp.select(*casts).from_(leaf.relation(planner.fresh("__icefleet_w")))
        if target.partitions:
            grouping = [
                _alias(exp.maybe_parse(value, dialect="duckdb"), name)
                for value, name in zip(target.partitions, hidden, strict=True)
            ]
            select = exp.select(*[_column(name) for name, _ in target.columns], *grouping).from_(
                exp.Subquery(this=select, alias=_quoted_alias(planner.fresh("__icefleet_v")))
            )
        return replace(
            leaf,
            body=select,
            columns=(*[name for name, _ in target.columns], *hidden),
            hashes=(),
        )

    pipe = planner.each(root, shaped)
    shown = "-> Iceberg data files"
    if target.partitions and not target.shuffle:
        # A rewrite whose rows stay in their partition (Phase 5b): every task reads whole
        # files of one partition each, so it writes a file per value it holds.
        shown += ", a file per partition value per task"
    elif target.partitions:
        types = [kind for name, kind in planner.describe_pipe(pipe) if name in hidden]
        n = planner.fleet.shuffle_partitions
        pipe = planner.shuffle(
            pipe,
            [_column(name) for name in hidden],
            [hash_type(kind) for kind in types],
            n,
            label="the partition values",
        )
        shown += ", a file per partition value"
    else:
        shown += f", rolled at {format_bytes(target.target_file_bytes)}"
    finals = [
        planner.close(
            leaf.select(),
            leaf,
            sink="result",
            partitions=1,
            notes=(*leaf.notes, shown),
            write=target,
        )
        for leaf in pipe.leaves()
    ]
    return StagePlan(
        stages=tuple(planner.stages),
        columns=tuple(name for name, _ in target.columns),
        final_stages=tuple(s.stage_id for s in finals),
        write=target,
    )


def _plan_probe(planner: _Planner, query: Query, key: str) -> StagePlan | Uncuttable:
    """The files of scan `key` holding a row the query's filter keeps (Phase 5b).

    Only a filter straight over the scan is probed -- `SELECT ... FROM t WHERE ...`, one
    stage, no join, no aggregate -- because only there is every row still a row of one
    file. Each task writes the distinct file names of its split; the coordinator reads
    them all. Anything else is Uncuttable, and the caller rewrites every candidate file.
    """
    if query.order or query.limit is not None or query.offset:
        return Uncuttable("a file probe with an ORDER BY or a LIMIT")
    pipe = planner.plan(query.root)
    if pipe.branches or pipe.inputs or pipe.scan != key or not isinstance(pipe.body, exp.Select):
        return Uncuttable("a file probe that is not a filter over the one table")
    select = pipe.body
    relation = from_of(select)
    if not isinstance(relation, exp.Table) or select.args.get("joins") or select.args.get("group"):
        return Uncuttable("a file probe that is not a filter over the one table")
    probe = select.copy()
    probe.set(
        "expressions",
        [_alias(_column(FILE_COLUMN, relation_alias(relation)), FILE_COLUMN)],
    )
    probe.set("distinct", exp.Distinct())
    final = planner.close(
        probe,
        pipe,
        sink="result",
        partitions=1,
        notes=(*pipe.notes, "-> the data files holding a row it changes"),
    )
    return StagePlan(
        stages=tuple(planner.stages),
        columns=(FILE_COLUMN,),
        final_stages=(final.stage_id,),
    )
