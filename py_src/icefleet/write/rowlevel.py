"""`DELETE`, `UPDATE` and `MERGE` on a `ClusterSession`: copy-on-write, file by file (Phase 5b).

icetl's planner (`icetl/sql/rowlevel.py`) computes the rows in scope on the coordinator
and swaps them in with `Table.overwrite(rows, overwrite_filter=P)`, which is only right
when the Iceberg filter `P` and the SQL `WHERE` select exactly the same rows -- and
PyIceberg then rewrites every partly matching file on the coordinator. Here the unit is
the **file** (kickoff answer 2):

    1. the files the statement may touch: Iceberg's pruning at the statement's snapshot,
       narrowed by a distributed probe to the files holding a matching row (answer 4)
    2. the workers rewrite those files *whole* -- every row of them, through the
       statement's SQL -- into new data files (`write/target.py`, Phase 5's sink)
    3. one OVERWRITE snapshot: those files out, the new ones in (`replace_files`)

Every row of a replaced file is written back by SQL, so the Iceberg filter only picks
files and may be as wide as it likes: wider costs reading, never rows. The rewrite reads
the table `VERSION AS OF` the snapshot it was planned on, pinned to exactly the files it
replaces (`ClusterSession._pinning`; a plan that would not read one of them is refused);
that pinned reference is its own scan, so a statement that also reads the target in a
subquery or a MERGE source reads all of it.

    DELETE   SELECT <columns> FROM t WHERE NOT COALESCE(<where>, FALSE)
    UPDATE   SELECT CASE WHEN <where> THEN <new> ELSE <old> END, ... FROM t
    MERGE    matched    t INNER JOIN s ON ...      (the WHEN MATCHED clauses)
             UNION ALL  t LEFT ANTI JOIN s ON ...  (WHEN NOT MATCHED BY SOURCE)
             UNION ALL  s LEFT ANTI JOIN t ON ...  (WHEN NOT MATCHED ... INSERT)

The MERGE halves are icetl's, with its `EXISTS` spelled as semi / anti joins (answer 7),
which distribute; its rules are kept: a target row matching two source rows is refused,
the scope narrowed by the source's own key values, an insert-only MERGE is an append.

**Concurrency** (answer 6): the commit is made on the table object the statement was
planned on, so a commit by anyone else in between fails it; the written files are then
deleted and the statement planned again from scratch, as icetl does.

What stays single-node: a `DELETE` PyIceberg answers from metadata alone (no condition,
or an exact one over identity partition columns) runs icetl's own path, which reads no
data file (answer 3); a target or a rewrite this cannot distribute -- a column type or
partition transform Phase 5 does not write, renamed columns, an expression a stage
cannot hold -- runs icetl's planner, whose reads are still distributed (answer 8), and
`icefleet.fallback=error` refuses it.
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

from icetl.errors import (
    AnalysisException,
    ParseException,
    QueryExecutionException,
    UnsupportedFeatureError,
)
from icetl.paths import engine_path
from icetl.plan.builder import source_key
from icetl.plan.pushdown import conjuncts, scope_predicate
from icetl.sql.rowlevel import (
    _ATTEMPTS,
    _BACKOFF_SECONDS,
    _aliased,
    _all_of,
    _assignments,
    _clause_projections,
    _columns_of,
    _distinct_values,
    _is_true,
    _key_pairs,
    _qualified,
    _resolve_target,
    _restrict,
    _survives,
    _whens,
    _where,
    run_delete,
    run_merge,
    run_update,
)
from sqlglot import exp

from icefleet.errors import Uncuttable
from icefleet.plan.joins import is_deterministic
from icefleet.write.commit import replace_files
from icefleet.write.target import WriteTarget, iceberg_path, write_target

if TYPE_CHECKING:
    from icetl.sql.rowlevel import _Target, _When
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.manifest import DataFile

    from icefleet.session import ClusterSession

__all__ = ["run_row_level"]

logger = logging.getLogger(__name__)


class _SingleNode(Exception):
    """This statement runs on icetl's planner, for `reason`."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class _Rewrite:
    """What one planned attempt writes and replaces.

    `query` produces the new contents of `remove` (and any inserted rows), reading the
    pinned target reference `key` restricted to `pinned`, the engine paths of `remove`.
    """

    query: exp.Expression
    remove: tuple[DataFile, ...]
    key: str | None
    pinned: frozenset[str]
    shuffle: bool


def run_row_level(session: ClusterSession, plan: exp.Expression) -> None:
    """Run one row-level statement (temporary views already inlined)."""
    if isinstance(plan, exp.Delete):
        _delete(session, plan)
    elif isinstance(plan, exp.Update):
        _update(session, plan)
    else:
        assert isinstance(plan, exp.Merge)
        _merge(session, plan)


# -- the shared machinery --------------------------------------------------------------


def _optimistic(
    session: ClusterSession,
    node: exp.Table,
    plan_one: Callable[[_Target], _Rewrite | None],
    fallback: Callable[[], None],
) -> None:
    """Plan against a freshly loaded table, write, commit on that very snapshot; on a
    concurrent commit, throw the files away and plan again (icetl's `_run_optimistic`)."""
    try:
        for attempt in range(_ATTEMPTS):
            target = _resolve_target(session, node)
            rewrite = plan_one(target)
            if rewrite is None:
                session._invalidate_source(target.ref)
                return
            if _apply(session, target, rewrite):
                session._invalidate_source(target.ref)
                return
            logger.info("%s moved while it was rewritten; planning again", target.name)
            time.sleep(_BACKOFF_SECONDS * (2**attempt))
    except _SingleNode as single:
        session._write_single_node(single.reason)
        fallback()
        return
    raise QueryExecutionException(
        f"Gave up after {_ATTEMPTS} attempts: another writer committed to "
        f"{node.sql(dialect='spark')} each time this statement was planned."
    )


def _apply(session: ClusterSession, target: _Target, rewrite: _Rewrite) -> bool:
    """Write `rewrite` and commit it; False when another commit got in first."""
    from pyiceberg.exceptions import CommitFailedException

    table = target.table
    df = session._frame_for(rewrite.query)
    writing = write_target(table, df.columns, positional=False)
    if isinstance(writing, str):  # pragma: no cover - `_writable` checked the table first
        raise _SingleNode(writing)
    writing = replace(writing, shuffle=rewrite.shuffle)
    pins = {rewrite.key: rewrite.pinned} if rewrite.key is not None else {}
    with session._pinning(pins):
        written = session._write_files(df, writing)
    if isinstance(written, Uncuttable):
        raise _SingleNode(written.reason)
    files, discard = written
    try:
        paths = [iceberg_path(f.path, table.location()) for f in files]
        replace_files(table, rewrite.remove, paths)
    except CommitFailedException:
        discard()
        return False
    except BaseException:
        discard()
        raise
    logger.info(
        "rewrote %d file(s) of %s as %d (%d row(s))",
        len(rewrite.remove),
        target.name,
        len(files),
        sum(f.rows for f in files),
    )
    return True


def _nothing_to_do(session: ClusterSession, why: str) -> None:
    from icefleet.session import QueryRun

    session._last_write = QueryRun(mode="distributed", reason=why)


def _writable(target: _Target) -> WriteTarget:
    """Refuse, before reading anything, a table the distributed writer cannot write."""
    writing = write_target(target.table, target.columns, positional=False)
    if isinstance(writing, str):
        raise _SingleNode(writing)
    return writing


def _pinned_node(target: _Target) -> exp.Table:
    """The target, `VERSION AS OF` the snapshot planned on: a scan of its own."""
    parsed: exp.Expression = exp.maybe_parse(
        f"SELECT * FROM x VERSION AS OF {int(target.snapshot_id or 0)}", dialect="spark"
    )
    table = parsed.find(exp.Table)
    assert table is not None
    node = target.node.copy()
    node.set("version", table.args["version"].copy())
    return node


def _candidates(target: _Target, predicate: BooleanExpression) -> list[DataFile]:
    """The data files at the planned snapshot that `predicate` cannot rule out."""
    tasks = list(
        target.table.scan(row_filter=predicate, snapshot_id=target.snapshot_id).plan_files()
    )
    if any(task.delete_files for task in tasks):
        raise _SingleNode("delete files (merge-on-read), which icetl's planner refuses")
    return [task.file for task in tasks]


def _narrowed(
    session: ClusterSession,
    pinned: exp.Table,
    candidates: list[DataFile],
    terms: Sequence[exp.Expression],
) -> list[DataFile]:
    """The candidates holding a row every one of `terms` keeps (kickoff answer 4)."""
    if not terms or not candidates:
        return candidates
    probe = (
        exp.select(exp.alias_(exp.Literal.number(1), "__icefleet_m", quoted=True))
        .from_(pinned.copy())
        .where(_all_of(terms))
    )
    matching = session._matching_files(probe, source_key(pinned, versioned=True))
    if matching is None:
        return candidates
    return [f for f in candidates if engine_path(f.file_path) in matching]


def _probe_terms(target: _Target, condition: exp.Expression | None) -> list[exp.Expression]:
    """The conjuncts of `condition` a filter over the target alone can evaluate."""
    return [
        term
        for term in conjuncts(condition)
        if target.resolver.owns_every_column(term)
        and term.find(exp.Select, exp.Subquery) is None
        and is_deterministic(term)
    ]


def _partition_sources(target: _Target) -> set[str]:
    """The columns the table's current partition spec is computed from."""
    by_id = {f.field_id: f.name for f in target.schema.fields}
    return {by_id[f.source_id] for f in target.table.spec().fields if f.source_id in by_id}


def _rewrite(
    target: _Target,
    query: exp.Expression,
    files: Sequence[DataFile],
    pinned: exp.Table,
    *,
    shuffle: bool,
) -> _Rewrite:
    return _Rewrite(
        query=query,
        remove=tuple(files),
        key=source_key(pinned, versioned=True),
        pinned=frozenset(engine_path(f.file_path) for f in files),
        shuffle=shuffle and bool(target.table.spec().fields),
    )


# -- DELETE --------------------------------------------------------------------------------


def _delete(session: ClusterSession, plan: exp.Delete) -> None:
    node = plan.this
    if not isinstance(node, exp.Table):
        raise ParseException(f"DELETE needs a table, got {type(node).__name__}.")
    if plan.args.get("using"):
        raise UnsupportedFeatureError(
            "DELETE ... USING",
            hint="Express the join as a subquery in the WHERE clause instead",
        )
    condition = _where(plan)
    if _from_metadata(_resolve_target(session, node), condition):
        from icefleet.session import QueryRun

        # PyIceberg drops whole files by their partition, reading none (kickoff answer 3).
        session._last_write = QueryRun(mode="metadata", reason="whole files, by partition")
        run_delete(session, plan)
        return

    def planned(target: _Target) -> _Rewrite | None:
        if target.snapshot_id is None:
            return None  # an empty table holds nothing to delete
        _writable(target)
        predicate, _, _ = scope_predicate(conjuncts(condition), target.resolver, target.schema)
        pinned = _pinned_node(target)
        files = _narrowed(
            session, pinned, _candidates(target, predicate), _probe_terms(target, condition)
        )
        if not files:
            _nothing_to_do(session, "no data file holds a row it deletes")
            return None
        survivors = exp.Select(
            expressions=[
                exp.alias_(_qualified(target.alias, column), column, quoted=True)
                for column in target.columns
            ]
        ).from_(pinned.copy())
        assert condition is not None  # no condition at all is answered from metadata
        survivors = survivors.where(exp.not_(_is_true(condition)))
        return _rewrite(target, survivors, files, pinned, shuffle=False)

    _optimistic(session, node, planned, lambda: run_delete(session, plan))


def _from_metadata(target: _Target, condition: exp.Expression | None) -> bool:
    """True when PyIceberg's own delete removes whole files and rewrites none: no
    condition, or one translated exactly that reads only identity partition columns."""
    if condition is None:
        return True
    table = target.table
    predicate, _, dropped = scope_predicate(
        conjuncts(condition), target.resolver, target.schema, exact_only=True
    )
    if dropped or target.snapshot_id is None:
        return False
    current = table.spec().spec_id
    touched = table.scan(row_filter=predicate, snapshot_id=target.snapshot_id).plan_files()
    if any(task.file.spec_id != current for task in touched):
        return False  # a file of an older spec may straddle the condition
    from pyiceberg.transforms import IdentityTransform

    by_id = {f.field_id: f.name for f in target.schema.fields}
    identity = {
        by_id[f.source_id]
        for f in table.spec().fields
        if isinstance(f.transform, IdentityTransform) and f.source_id in by_id
    }
    named = {target.resolver.name(column) for column in condition.find_all(exp.Column)}
    return bool(named) and None not in named and named <= identity


# -- UPDATE --------------------------------------------------------------------------------


def _update(session: ClusterSession, plan: exp.Update) -> None:
    node = plan.this
    if not isinstance(node, exp.Table):
        raise ParseException(f"UPDATE needs a table, got {type(node).__name__}.")
    if plan.args.get("from_") or plan.args.get("from"):
        raise UnsupportedFeatureError(
            "UPDATE ... FROM",
            hint="Express the other table as a subquery in the SET expression or WHERE",
        )
    condition = _where(plan)

    def planned(target: _Target) -> _Rewrite | None:
        assignments = _assignments(plan.expressions, target)
        if target.snapshot_id is None:
            return None
        _writable(target)
        predicate, _, _ = scope_predicate(conjuncts(condition), target.resolver, target.schema)
        pinned = _pinned_node(target)
        files = _narrowed(
            session, pinned, _candidates(target, predicate), _probe_terms(target, condition)
        )
        if not files:
            _nothing_to_do(session, "no data file holds a row it updates")
            return None
        projections = []
        for column in target.columns:
            old = _qualified(target.alias, column)
            new = assignments.get(column)
            if new is None:
                value: exp.Expression = old
            elif condition is None:
                value = new.copy()
            else:
                value = exp.Case(ifs=[exp.If(this=condition.copy(), true=new.copy())], default=old)
            projections.append(exp.alias_(value, column, quoted=True))
        # No WHERE: every row of the files is written back, the CASE deciding which change.
        updated = exp.Select(expressions=projections).from_(pinned.copy())
        moves = bool(set(assignments) & _partition_sources(target))
        return _rewrite(target, updated, files, pinned, shuffle=moves)

    _optimistic(session, node, planned, lambda: run_update(session, plan))


# -- MERGE ---------------------------------------------------------------------------------


def _merge(session: ClusterSession, plan: exp.Merge) -> None:
    node = plan.this
    if not isinstance(node, exp.Table):
        raise ParseException(f"MERGE INTO needs a table, got {type(node).__name__}.")
    using = plan.args.get("using")
    if not isinstance(using, (exp.Table, exp.Subquery)):
        raise ParseException("MERGE ... USING needs a table or a subquery.")
    on = plan.args.get("on")
    if not isinstance(on, exp.Expression):
        raise ParseException("MERGE needs an ON condition.")
    source_alias = using.alias or (using.name if isinstance(using, exp.Table) else "")
    if not source_alias:
        raise AnalysisException(
            "MERGE ... USING (subquery) needs an alias, so the WHEN clauses can name "
            "its columns: USING (SELECT ...) AS s"
        )
    source = _aliased(using, source_alias)
    source_columns = _columns_of(session, source)

    def planned(target: _Target) -> _Rewrite | None:
        whens = _whens(plan, target, source_alias, source_columns)
        matched = [w for w in whens if w.kind == "matched"]
        by_source = [w for w in whens if w.kind == "by_source"]
        inserts = [w for w in whens if w.kind == "not_matched"]
        types = [kind for _, kind in _writable(target).columns]
        pinned = _pinned_node(target)

        files: list[DataFile] = []
        if target.snapshot_id is not None:
            scope = _merge_scope(session, target, on, source, source_alias, by_source)
            if scope is not None:
                predicate, _, _ = scope_predicate(scope, target.resolver, target.schema)
                probe = [t for t in scope if is_deterministic(t) and t.find(exp.Select) is None]
                files = _narrowed(session, pinned, _candidates(target, predicate), probe)

        if not files or not (matched or by_source):
            # No target row is touched: the inserts alone, appended (icetl's shortcut).
            if not inserts:
                _nothing_to_do(session, "no target row can match and nothing is inserted")
                return None
            against = pinned if files else None  # with no candidate file, nothing matches
            added = _inserted(target, types, source, on, inserts, against)
            rewrite = _rewrite(target, added, files, pinned, shuffle=True)
            return replace(rewrite, remove=(), key=rewrite.key if files else None)

        rewrite_key = source_key(pinned, versioned=True)
        paths = frozenset(engine_path(f.file_path) for f in files)
        if matched:
            with session._pinning({rewrite_key: paths}):
                _assert_one_match_per_target_row(session, target, pinned, source, on)
        halves: list[exp.Expression] = [
            _restrict(
                _joined(target, pinned, source, on, matched, kind="INNER"), _survives(matched)
            ),
            _restrict(
                _joined(target, pinned, source, on, by_source, kind="ANTI"), _survives(by_source)
            ),
        ]
        if inserts:
            halves.append(_inserted(target, types, source, on, inserts, pinned))
        combined = functools.reduce(lambda a, b: exp.union(a, b, distinct=False), halves)
        assigned = {c for w in matched + by_source for c in (w.assignments or {})}
        moves = bool(inserts) or bool(assigned & _partition_sources(target))
        return _rewrite(target, combined, files, pinned, shuffle=moves)

    _optimistic(session, node, planned, lambda: run_merge(session, plan))


def _merge_scope(
    session: ClusterSession,
    target: _Target,
    on: exp.Expression,
    source: exp.Expression,
    source_alias: str,
    by_source: Sequence[_When],
) -> list[exp.Expression] | None:
    """The target-only terms every target row the ON condition can match satisfies, or
    None when no target row can match at all.

    icetl's `_merge_scope` (its target-only ON conjuncts, and an IN list of the source's
    own values per `t.k = s.k` key), without its last step: icetl keeps only the terms
    PyIceberg evaluates *exactly*, because its delete filter must equal its SQL. Here a
    term only picks files -- the probe evaluates it in DuckDB, Iceberg's pruning may be
    wider -- so every term narrows, a date IN list included. A WHEN NOT MATCHED BY SOURCE
    clause acts on rows the source never names: the whole table, no terms.
    """
    if by_source:
        return []
    terms = conjuncts(on)
    scope = [term for term in terms if target.resolver.owns_every_column(term)]
    for column, expression in _key_pairs(terms, target, source_alias):
        values = _distinct_values(session, source, expression)
        if values is None:
            continue
        if not values:
            return None  # the source holds no usable key, and ON is a conjunction
        scope.append(
            exp.In(
                this=_qualified(target.alias, column),
                expressions=[exp.convert(value) for value in values],
            )
        )
    return scope


def _join(relation: exp.Expression, on: exp.Expression, kind: str) -> exp.Join:
    if kind == "INNER":
        return exp.Join(this=relation.copy(), kind="INNER", on=on.copy())
    return exp.Join(this=relation.copy(), side="LEFT", kind=kind, on=on.copy())


def _joined(
    target: _Target,
    pinned: exp.Table,
    source: exp.Expression,
    on: exp.Expression,
    clauses: Sequence[_When],
    *,
    kind: str,
) -> exp.Select:
    """The target rows that join a source row (INNER) or none (ANTI), after `clauses`."""
    select = exp.Select(expressions=_clause_projections(target, clauses)).from_(pinned.copy())
    select.set("joins", [_join(source, on, kind)])
    return select


def _inserted(
    target: _Target,
    types: Sequence[str],
    source: exp.Expression,
    on: exp.Expression,
    clauses: Sequence[_When],
    against: exp.Table | None,
) -> exp.Select:
    """Source rows that join no target row, expanded by the `WHEN NOT MATCHED` clauses.

    icetl's `_merge_inserts`, with its `NOT EXISTS` as an anti join against the pinned
    files (they hold every row the ON condition could match), and a column the INSERT
    does not name as a NULL *of that column's type*: an untyped NULL is an integer to
    DuckDB, which no timestamp or decimal column accepts (icetl fails there; Phase 5b
    kickoff answer 7). `against` None: no target row can match, so no join at all.
    `types` are the table's columns as DuckDB types (`WriteTarget.columns`).
    """
    projections: list[exp.Expression] = []
    for position, column in enumerate(target.columns):
        typed_null = exp.Cast(
            this=exp.Null(), to=exp.DataType.build(types[position], dialect="duckdb")
        )
        ifs: list[exp.If] = []
        default: exp.Expression = typed_null
        for when in clauses:
            assert when.inserts is not None
            value = when.inserts[position].copy()
            if isinstance(value, exp.Null):
                value = typed_null.copy()
            if when.condition is None:
                default = value
                break
            ifs.append(exp.If(this=when.condition.copy(), true=value))
        projections.append(
            cast(
                exp.Expression,
                exp.alias_(
                    exp.Case(ifs=ifs, default=default) if ifs else default, column, quoted=True
                ),
            )
        )
    select = exp.Select(expressions=projections).from_(source.copy())
    if against is not None:
        select.set("joins", [_join(against, on, "ANTI")])
    claimed = [when.condition.copy() for when in clauses if when.condition is not None]
    if len(claimed) == len(clauses):
        select = select.where(
            cast(exp.Expression, exp.or_(*claimed)) if len(claimed) > 1 else claimed[0]
        )
    return select


def _assert_one_match_per_target_row(
    session: ClusterSession,
    target: _Target,
    pinned: exp.Table,
    source: exp.Expression,
    on: exp.Expression,
) -> None:
    """icetl's refusal of a target row matching two source rows, as two distributed
    counts: (target, source) pairs against target rows with a partner (a semi join)."""
    one = exp.alias_(exp.Literal.number(1), "n", quoted=True)
    pairs = exp.Select(expressions=[one.copy()]).from_(pinned.copy())
    pairs.set("joins", [_join(source, on, "INNER")])
    partnered = exp.Select(expressions=[one.copy()]).from_(pinned.copy())
    partnered.set("joins", [_join(source, on, "SEMI")])
    if session._frame_for(pairs).count() > session._frame_for(partnered).count():
        raise AnalysisException(
            f"MERGE INTO {target.name}: a row of the target matched more than one row of "
            f"the source. Which one would win is undefined, so the statement is refused. "
            f"Deduplicate the source on the ON columns first."
        )
