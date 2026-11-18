"""Cutting an `Aggregate` in two: a partial stage over the splits, a final stage per
hash partition (PLAN §4.2, P2: correct by co-location, fast by decomposition).

**Partial path** -- when every aggregate call is in the decomposition table:

    partial  SELECT key AS __k0, ..., <partial aggs> AS __p0, ..., hash(key, ...) % n AS __part
             FROM <the aggregate's own FROM> WHERE <its WHERE> GROUP BY key, ...
    final    the aggregate's SELECT, its keys replaced by __k<i>, each aggregate call by
             CAST(<combinator over __p<j>> AS <the type the call has single-node>),
             FROM the shuffle input GROUP BY __k0, ... HAVING <rewritten the same way>

**Raw path** -- otherwise (`count(distinct)`, `first`, `string_agg`, `FILTER (...)`, ...):
the columns the aggregate reads are shuffled by its keys as they are, and the final stage
runs the aggregate's SELECT unchanged over them. Always correct, sometimes slower.

A global aggregate (no keys) has one partition. `SELECT DISTINCT` is an aggregate whose
keys are its projection. Columns whose type does not survive parquet travel as a lossless
stand-in (`plan/types.py`) and are cast back on the final side.

Everything is built from copies of the optimized tree's own nodes; the only new SQL is
the combinators, the casts and the partition expression.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass

from sqlglot import exp

from icefleet.errors import Uncuttable
from icefleet.plan.partitioning import partition_expression
from icefleet.plan.rel import Aggregate, aggregate_calls, from_of
from icefleet.plan.tasks import PARTITION_COLUMN
from icefleet.plan.types import cast_to, parquet_safe_type

__all__ = [
    "INPUT_ALIAS",
    "INPUT_TABLE",
    "AggregateCut",
    "Describe",
    "cut_aggregate",
    "group_keys",
    "partition_expression",
]

#: (name, DuckDB type) of every column a SELECT produces, from DuckDB's DESCRIBE.
Describe = Callable[[exp.Expression], list[tuple[str, str]]]

#: The table name the final stage reads its shuffle input under; each task swaps it for
#: `read_parquet($files)`, or for a typed empty relation when its partition has no rows.
INPUT_TABLE = "__icefleet_input__"
INPUT_ALIAS = "__icefleet_in"

_SAME_COMBINATOR: tuple[type[exp.Expression], ...] = tuple(
    cls
    for cls in (
        exp.Min,
        exp.Max,
        exp.LogicalAnd,
        exp.LogicalOr,
        getattr(exp, "BitwiseAndAgg", None),
        getattr(exp, "BitwiseOrAgg", None),
        getattr(exp, "BitwiseXorAgg", None),
    )
    if cls is not None
)


@dataclass(frozen=True)
class AggregateCut:
    """The two halves of one aggregate, and what `explain()` says about them."""

    strategy: str  # "partial" | "raw"
    partial: exp.Select  # over the aggregate's own FROM; carries __part when partitions > 1
    final: exp.Select  # over INPUT_TABLE
    shuffle_columns: tuple[tuple[str, str], ...]  # (name, DuckDB type) as written
    partitions: int
    keys: tuple[str, ...]
    partials: tuple[str, ...] = ()
    finals: tuple[str, ...] = ()
    why_raw: str | None = None


def _short(expression: exp.Expression) -> str:
    """SQL for `explain()`, without the table qualifiers."""
    copy = expression.copy()
    for column in copy.find_all(exp.Column):
        column.set("table", None)
    return copy.sql(dialect="duckdb")


def _column(name: str) -> exp.Column:
    return exp.column(name, quoted=True)


def _alias(expression: exp.Expression, name: str) -> exp.Alias:
    return exp.Alias(this=expression, alias=exp.to_identifier(name, quoted=True))


# -- the decomposition table (§4.2) ----------------------------------------------------


def _not_decomposable(call: exp.Expression) -> str | None:
    """Why `call` cannot be split into partial + final, or None when it can."""
    if isinstance(call.parent, exp.Filter):
        return f"{_short(call.parent)} has a FILTER clause"
    if call.find(exp.Distinct) is not None:
        return f"{_short(call)} is a DISTINCT aggregate"
    # `big_int` is sqlglot's note that DuckDB's COUNT returns BIGINT, not an argument.
    extra = [key for key, value in call.args.items() if key not in ("this", "big_int") and value]
    if extra:
        return f"{_short(call)} takes more than one argument"
    if isinstance(call, (exp.Count, exp.Sum, exp.Avg, *_SAME_COMBINATOR)):
        return None
    return f"{_short(call)} has no partial form"


def _partials(call: exp.Expression) -> list[exp.Expression]:
    if isinstance(call, exp.Avg):
        argument = call.this
        return [exp.Sum(this=argument.copy()), exp.Count(this=argument.copy())]
    return [call.copy()]


def _combinator(call: exp.Expression, parts: Sequence[str]) -> exp.Expression:
    if isinstance(call, exp.Count):
        return exp.Coalesce(
            this=exp.Sum(this=_column(parts[0])), expressions=[exp.Literal.number(0)]
        )
    if isinstance(call, exp.Sum):
        return exp.Sum(this=_column(parts[0]))
    if isinstance(call, exp.Avg):
        total = cast_to(exp.Sum(this=_column(parts[0])), "DOUBLE")
        count = exp.Nullif(this=exp.Sum(this=_column(parts[1])), expression=exp.Literal.number(0))
        return exp.Div(this=total, expression=count)
    return type(call)(this=_column(parts[0]))


# -- rewriting ---------------------------------------------------------------------------


def _rewrite(
    node: exp.Expression,
    replacements: Mapping[int, exp.Expression],
    keys: Sequence[tuple[exp.Expression, str]],
) -> exp.Expression:
    """Replace aggregate calls (by identity) and key expressions (by structure)."""
    replacement = replacements.get(id(node))
    if replacement is not None:
        return replacement.copy()
    for key, name in keys:
        if node == key:
            return _column(name)
    for arg, value in list(node.args.items()):
        if isinstance(value, exp.Expression):
            node.set(arg, _rewrite(value, replacements, keys))
        elif isinstance(value, list):
            node.set(
                arg,
                [
                    _rewrite(item, replacements, keys) if isinstance(item, exp.Expression) else item
                    for item in value
                ],
            )
    return node


def _set_from(select: exp.Select, relation: exp.Expression) -> None:
    key = "from_" if "from_" in type(select).arg_types else "from"
    select.set(key, exp.From(this=relation))


def _input_relation(
    alias: str, columns: Sequence[tuple[str, str]], restore: dict[str, str]
) -> exp.Expression:
    """The final stage's FROM: the shuffle input, with stand-in types cast back."""
    table = exp.Table(this=exp.to_identifier(INPUT_TABLE, quoted=True))
    if not restore:
        table.set("alias", exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
        return table
    projection = [
        _alias(cast_to(_column(name), restore[name]), name) if name in restore else _column(name)
        for name, _ in columns
    ]
    inner = exp.select(*projection).from_(table)
    return exp.Subquery(
        this=inner, alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True))
    )


def _stand_ins(
    columns: Sequence[tuple[str, str]],
) -> tuple[list[tuple[str, str]], dict[str, str]] | Uncuttable:
    """(columns as written, {name: original type} for the ones that need casting back)."""
    written: list[tuple[str, str]] = []
    restore: dict[str, str] = {}
    for name, kind in columns:
        stand_in = parquet_safe_type(kind)
        if stand_in is None:
            return Uncuttable(f"a shuffled column whose type does not survive parquet: {kind}")
        written.append((name, stand_in))
        if stand_in != kind:
            restore[name] = kind
    return written, restore


def group_keys(select: exp.Select) -> list[exp.Expression] | Uncuttable:
    """The grouping keys of `select` (its projection for a SELECT DISTINCT), copied."""
    if select.args.get("distinct"):
        return [projection.unalias().copy() for projection in select.expressions]
    group = select.args.get("group")
    if group is None:
        return []
    for arg in ("rollup", "cube", "grouping_sets"):
        if group.args.get(arg):
            return Uncuttable("GROUPING SETS / ROLLUP / CUBE -- Phase 4")
    if group.args.get("all"):
        return Uncuttable("GROUP BY ALL the optimizer did not expand")
    grouping_sets = (exp.Rollup, exp.Cube, exp.GroupingSets)
    if any(isinstance(key, grouping_sets) for key in group.expressions):
        return Uncuttable("GROUPING SETS / ROLLUP / CUBE -- Phase 4")
    return [key.copy() for key in group.expressions]


def cut_aggregate(
    aggregate: Aggregate,
    *,
    partitions: int,
    describe: Describe,
    aggregate_names: Collection[str] = (),
) -> AggregateCut | Uncuttable:
    """Cut `aggregate` into a partial and a final SELECT. `describe` may raise."""
    select = aggregate.select
    relation = from_of(select)
    if relation is None:  # pragma: no cover - rel.from_sqlglot only admits a FROM
        return Uncuttable("an aggregate without a FROM")
    keys = group_keys(select)
    if isinstance(keys, Uncuttable):
        return keys
    n = partitions if keys else 1

    work = select.copy()
    calls = aggregate_calls(work, aggregate_names)
    reasons = [_not_decomposable(call) for call in calls]
    why_raw = next((reason for reason in reasons if reason is not None), None)
    if why_raw is None:
        cut = _partial_path(work, relation, keys, calls, n, describe)
        if isinstance(cut, (AggregateCut, Uncuttable)):
            return cut
        why_raw = cut
    return _raw_path(select, relation, keys, n, describe, why_raw)


def _partial_path(
    work: exp.Select,
    relation: exp.Expression,
    keys: list[exp.Expression],
    calls: list[exp.Expression],
    n: int,
    describe: Describe,
) -> AggregateCut | Uncuttable | str:
    """The partial path, or (a string) why the raw path must be taken after all."""
    key_names = [f"__k{i}" for i in range(len(keys))]
    parts: list[list[tuple[exp.Expression, str]]] = []
    counter = 0
    for call in calls:
        named = []
        for partial in _partials(call):
            named.append((partial, f"__p{counter}"))
            counter += 1
        parts.append(named)

    def grouped(projection: Sequence[exp.Expression]) -> exp.Select:
        query = exp.select(*projection).from_(relation.copy())
        where = work.args.get("where")
        if where is not None:
            query.set("where", where.copy())
        if keys:
            query = query.group_by(*[key.copy() for key in keys])
        return query

    # One DESCRIBE types the keys, the partials and every call as it is single-node.
    probe = grouped(
        [_alias(key.copy(), name) for key, name in zip(keys, key_names, strict=True)]
        + [_alias(partial.copy(), name) for named in parts for partial, name in named]
        + [_alias(call.copy(), f"__o{j}") for j, call in enumerate(calls)]
    )
    types = dict(describe(probe))
    original = [types[f"__o{j}"] for j in range(len(calls))]
    for call, kind in zip(calls, original, strict=True):
        if isinstance(call, exp.Avg) and kind != "DOUBLE":
            return f"{_short(call)} averages a {kind}, not a number"

    shuffled = [(name, types[name]) for name in key_names] + [
        (name, types[name]) for named in parts for _, name in named
    ]
    stand_ins = _stand_ins(shuffled)
    if isinstance(stand_ins, Uncuttable):
        return stand_ins
    written, restore = stand_ins
    written_types = dict(written)

    def as_written(expression: exp.Expression, name: str) -> exp.Expression:
        kind = written_types[name]
        return _alias(cast_to(expression, kind) if name in restore else expression, name)

    projection = [as_written(key.copy(), name) for key, name in zip(keys, key_names, strict=True)]
    projection += [as_written(partial.copy(), name) for named in parts for partial, name in named]
    if n > 1:
        key_types = [types[name] for name in key_names]
        projection.append(_alias(partition_expression(keys, n, key_types), PARTITION_COLUMN))
    partial_select = grouped(projection)

    # The final side: keys restored, partials combined, every call cast to its own type.
    replacements = {
        id(call): cast_to(_combinator(call, [name for _, name in named]), kind)
        for call, named, kind in zip(calls, parts, original, strict=True)
    }
    key_restore = {name: kind for name, kind in restore.items() if name in key_names}
    pairs = list(zip(keys, key_names, strict=True))
    work.set("expressions", [_rewrite(e, replacements, pairs) for e in work.expressions])
    having = work.args.get("having")
    if having is not None:
        work.set("having", _rewrite(having, replacements, pairs))
    work.set("where", None)
    work.set("distinct", None)
    work.set(
        "group", exp.Group(expressions=[_column(name) for name in key_names]) if keys else None
    )
    _set_from(work, _input_relation(INPUT_ALIAS, written, key_restore))
    allowed = {name for name, _ in written}
    for root in [*work.expressions, work.args.get("having")]:
        if root is None:
            continue
        for column in root.find_all(exp.Column):
            if column.table or column.name not in allowed:
                return f"{_short(column)} is neither grouped nor aggregated"
        if root.find(exp.Star) is not None:
            return "a * the partial path cannot carry"

    return AggregateCut(
        strategy="partial",
        partial=partial_select,
        final=work,
        shuffle_columns=tuple(written),
        partitions=n,
        keys=tuple(_short(key) for key in keys),
        partials=tuple(_short(partial) for named in parts for partial, _ in named),
        finals=tuple(
            _short(_combinator(call, [name for _, name in named]))
            for call, named in zip(calls, parts, strict=True)
        ),
    )


def _raw_path(
    select: exp.Select,
    relation: exp.Expression,
    keys: list[exp.Expression],
    n: int,
    describe: Describe,
    why_raw: str,
) -> AggregateCut | Uncuttable:
    work = select.copy()
    alias = relation.alias_or_name
    names: list[str] = []
    roots: list[exp.Expression] = list(work.expressions)
    for arg in ("group", "having"):
        if work.args.get(arg) is not None:
            roots.append(work.args[arg])
    for root in roots:
        for column in root.find_all(exp.Column):
            if column.args.get("db") or column.args.get("catalog"):
                return Uncuttable(f"a column reference the shuffle cannot carry ({_short(column)})")
            if column.table and column.table != alias:
                return Uncuttable(f"a column of another relation ({column.sql(dialect='duckdb')})")
            if column.name not in names:
                names.append(column.name)

    def projected(expressions: Sequence[exp.Expression]) -> exp.Select:
        query = exp.select(*expressions).from_(relation.copy())
        where = work.args.get("where")
        if where is not None:
            query.set("where", where.copy())
        return query

    plain = [_alias(exp.column(name, table=alias, quoted=True), name) for name in names] or [
        _alias(exp.true(), "__icefleet_row")
    ]
    hash_keys = [_alias(key.copy(), f"__hk{i}") for i, key in enumerate(keys)]
    described = describe(projected(plain + hash_keys))  # the keys' types, for the hash
    columns, key_types = described[: len(plain)], [kind for _, kind in described[len(plain) :]]
    stand_ins = _stand_ins(columns)
    if isinstance(stand_ins, Uncuttable):
        return stand_ins
    written, restore = stand_ins
    written_types = dict(written)
    projection = [
        _alias(cast_to(item.this.copy(), written_types[item.alias]), item.alias)
        if item.alias in restore
        else item
        for item in plain
    ]
    if n > 1:
        projection.append(_alias(partition_expression(keys, n, key_types), PARTITION_COLUMN))
    partial = projected(projection)  # before the WHERE moves off the final side

    work.set("where", None)
    _set_from(work, _input_relation(alias, written, restore))
    return AggregateCut(
        strategy="raw",
        partial=partial,
        final=work,
        shuffle_columns=tuple(written),
        partitions=n,
        keys=tuple(_short(key) for key in keys),
        why_raw=why_raw,
    )
