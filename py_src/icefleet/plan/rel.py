"""The thin relational layer over icetl's optimized sqlglot tree (PLAN §4.1).

A recognised plan is a tree of node kinds, each wrapping the sqlglot `SELECT` it came
from -- the nodes never re-implement an expression; every stage's SQL is built from the
very nodes of the optimized tree:

    Scan        an Iceberg table reference
    Project     a SELECT over one relation, row at a time (filter, map, function calls)
    Aggregate   a GROUP BY (grouping sets too), a global aggregate or a SELECT DISTINCT
                over one relation, with its HAVING
    Join        a SELECT whose FROM joins two or more relations (Phase 3): its projection,
                FROM, JOINs and WHERE; one child per relation, in FROM order
    Window      a SELECT over one relation computing window functions, in its projection
                or QUALIFY (Phase 4)
    DistinctOn  a SELECT DISTINCT ON (keys) over one relation (Phase 4)
    TopN        a subquery's ORDER BY / LIMIT / OFFSET over its rows (Phase 4)
    SetOp       UNION / INTERSECT / EXCEPT [ALL] of two relations (Phase 4)

icetl's optimizer leaves a join as one flat SELECT (`FROM a JOIN b ON ... WHERE ...
GROUP BY ...`). An aggregating (or windowing) one is split here: the join becomes a
subquery `__icefleet_j` that projects every column the upper half reads -- so an
aggregate or a window always has exactly one relation below it. A window over an
aggregate and a DISTINCT over a window are split the same way, the lower half under
`__icefleet_w`.

An uncorrelated `x IN (SELECT y ...)` that is a top-level conjunct of a WHERE becomes a
`SEMI JOIN` on `x = y` (Phase 3): in a WHERE, NULL and false both drop the row, so the
two agree on every input, NULLs included. `NOT IN`, `EXISTS` and correlated subqueries
are refused.

`ORDER BY` / `LIMIT` / `OFFSET` on the top are lifted into the `Query`: the coordinator
applies them while reading the result (or the stages sort it by range), and each final
task applies a `LIMIT` locally first. An `ORDER BY` key that is not a result column
becomes a hidden result column, which the coordinator orders by and then drops. A
subquery's `ORDER BY` without a `LIMIT` is moved up through plain projections, so
`df.orderBy(...).select(...)` stays ordered, and dropped under an aggregate, a join, a
window or a set operation, where it means nothing (Phase 4, kickoff answer 7).

Anything else -- a subquery in an expression, `UNION BY NAME`, a LATERAL -- is
`Uncuttable` and runs single-node (P3), with the reason.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from icetl.plan.builder import source_key
from sqlglot import exp

from icefleet.errors import Uncuttable
from icefleet.plan.joins import conjuncts

if TYPE_CHECKING:
    from icetl.plan.builder import ScanSource

__all__ = [
    "JOIN_ALIAS",
    "WINDOW_ALIAS",
    "Aggregate",
    "DistinctOn",
    "Join",
    "Project",
    "Query",
    "RelNode",
    "Scan",
    "SetOp",
    "TopN",
    "Window",
    "aggregate_calls",
    "from_of",
    "from_sqlglot",
    "is_aggregate_call",
    "join_kind",
    "relation_alias",
    "set_from",
    "windows_of",
]

HIDDEN_ORDER_PREFIX = "__icefleet_order_"
#: The alias of the join subquery an aggregating join SELECT is split into.
JOIN_ALIAS = "__icefleet_j"
#: The alias of the lower half a window over an aggregate, or a DISTINCT over a window, reads.
WINDOW_ALIAS = "__icefleet_w"
_SEMI_ALIAS = "__icefleet_in"


@dataclass(frozen=True)
class Scan:
    """An Iceberg table reference, by the source key icetl resolved it under."""

    key: str
    table: exp.Table | None = field(default=None, compare=False, repr=False)


@dataclass(frozen=True, eq=False)
class Project:
    """Row-at-a-time work over `child`: `select` reads `child` in its FROM."""

    child: RelNode
    select: exp.Select


@dataclass(frozen=True, eq=False)
class Aggregate:
    """`GROUP BY` / global aggregate / `SELECT DISTINCT` over `child`, with its `HAVING`.

    `select` still carries its own `WHERE`: rows are filtered before they are aggregated,
    so the `WHERE` belongs to the partial (input) side of the cut.
    """

    child: RelNode
    select: exp.Select


@dataclass(frozen=True, eq=False)
class Join:
    """`select` joins `children` (its FROM relation, then each JOIN's), in that order.

    No GROUP BY, HAVING, DISTINCT or window: such a SELECT is split into an Aggregate (or
    a Window) over a Join first.
    """

    select: exp.Select
    children: tuple[RelNode, ...]


@dataclass(frozen=True, eq=False)
class Window:
    """Window functions over `child`, in `select`'s projection or QUALIFY. No GROUP BY:
    a window over an aggregate is split into a Window over an Aggregate first."""

    child: RelNode
    select: exp.Select


@dataclass(frozen=True, eq=False)
class DistinctOn:
    """`SELECT DISTINCT ON (keys) ...` over `child`: one row per key, any one."""

    child: RelNode
    select: exp.Select


@dataclass(frozen=True, eq=False)
class TopN:
    """`child`'s rows ordered by `order`, `limit` of them after skipping `offset`.

    `order` reads `child`'s output columns by name; `columns` are the ones its reader
    sees (a hidden order column is dropped).
    """

    child: RelNode
    order: tuple[exp.Ordered, ...]
    limit: int | None
    offset: int
    columns: tuple[str, ...]


@dataclass(frozen=True, eq=False)
class SetOp:
    """`left <kind> [ALL] right`: `union`, `intersect` or `except`, by position."""

    kind: str
    distinct: bool
    left: RelNode
    right: RelNode
    node: exp.SetOperation


RelNode = Scan | Project | Aggregate | Join | Window | DistinctOn | TopN | SetOp


@dataclass(frozen=True, eq=False)
class Query:
    """A recognised plan: the tree, and what the coordinator applies to its result.

    `tree` is a private copy of the optimized tree, with the top's `ORDER BY`, `LIMIT`
    and `OFFSET` removed and any hidden order columns appended to its projection.
    """

    tree: exp.Select | exp.SetOperation
    root: RelNode
    order: tuple[exp.Ordered, ...] = ()
    limit: int | None = None
    hidden: tuple[str, ...] = ()
    offset: int = 0

    def nodes(self) -> Iterator[RelNode]:
        """Every node, top down, depth first, children in FROM order."""
        stack: list[RelNode] = [self.root]
        while stack:
            node = stack.pop()
            yield node
            if isinstance(node, (Project, Aggregate, Window, DistinctOn, TopN)):
                stack.append(node.child)
            elif isinstance(node, Join):
                stack.extend(reversed(node.children))
            elif isinstance(node, SetOp):
                stack.extend([node.right, node.left])

    @property
    def scans(self) -> list[Scan]:
        return [node for node in self.nodes() if isinstance(node, Scan)]

    @property
    def scan(self) -> Scan:
        """The first table reference (the only one, for a plan without joins)."""
        return self.scans[0]

    @property
    def aggregate(self) -> Aggregate | None:
        return next((n for n in self.nodes() if isinstance(n, Aggregate)), None)


# -- aggregate and window calls ----------------------------------------------------------

_CALL_NAME = re.compile(r'^\s*"?([A-Za-z_][A-Za-z0-9_]*)"?\s*\(')


def is_aggregate_call(node: exp.Expression, aggregate_names: Collection[str] = ()) -> bool:
    """True for an aggregate function call.

    sqlglot's `AggFunc` classes first, then DuckDB's own catalog by name, because sqlglot
    does not know every aggregate: `list(x)` parses as a scalar `exp.List`, and a plan
    that took it for row-at-a-time work would return one list per split. Calling a scalar
    an aggregate only costs speed (the raw path still runs the original SELECT).
    """
    if isinstance(node, exp.AggFunc):
        return True
    if not aggregate_names or not isinstance(node, exp.Func):
        return False
    if isinstance(node, exp.Anonymous):
        name = str(node.this)
    else:
        # sqlglot makes operators such as AND functions too, and `COUNT(*) > 1 AND ...`
        # starts like a call: only text that is one call from end to end is a call.
        rendered = node.sql(dialect="duckdb")
        match = _CALL_NAME.match(rendered)
        if match is None or _closing_paren(rendered, match.end() - 1) != len(rendered) - 1:
            return False
        name = match.group(1)
    return name.lower() in aggregate_names


def _closing_paren(text: str, start: int) -> int:
    """The index of the parenthesis closing the one at `start`, skipping quoted text."""
    depth = 0
    quote: str | None = None
    for index in range(start, len(text)):
        char = text[index]
        if quote is not None:
            if char == quote:
                quote = None
        elif char in "'\"":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
    return -1


def _roots(select: exp.Select, args: tuple[str, ...]) -> list[exp.Expression]:
    roots: list[exp.Expression] = list(select.expressions)
    for arg in args:
        if select.args.get(arg) is not None:
            roots.append(select.args[arg])
    return roots


def aggregate_calls(
    select: exp.Select, aggregate_names: Collection[str] = ()
) -> list[exp.Expression]:
    """The outermost aggregate calls of `select`'s projection, `HAVING` and `QUALIFY`.

    A window function (`sum(x) OVER (...)`) is not an aggregate of its SELECT; an
    aggregate inside its arguments or its window (`rank() OVER (ORDER BY sum(x))`) is.
    """
    found: list[exp.Expression] = []

    def visit(node: exp.Expression) -> None:
        if isinstance(node, exp.Select):
            return  # a subquery is refused separately; its aggregates are its own
        if isinstance(node, exp.Window):
            function = node.this
            while isinstance(function, (exp.IgnoreNulls, exp.RespectNulls)):
                function = function.this
            for child in function.iter_expressions():
                visit(child)
            for arg in ("partition_by", "order"):
                value = node.args.get(arg)
                items = value if isinstance(value, list) else [value] if value else []
                for item in items:
                    visit(item)
            return
        if is_aggregate_call(node, aggregate_names):
            found.append(node)
            return
        for child in node.iter_expressions():
            visit(child)

    for root in _roots(select, ("having", "qualify")):
        visit(root)
    return found


def windows_of(select: exp.Select) -> list[exp.Window]:
    """The window function calls of `select`'s own projection and QUALIFY."""
    found: list[exp.Window] = []

    def visit(node: exp.Expression) -> None:
        if isinstance(node, exp.Select):
            return
        if isinstance(node, exp.Window):
            found.append(node)
            return
        for child in node.iter_expressions():
            visit(child)

    for root in _roots(select, ("qualify",)):
        visit(root)
    return found


# -- small helpers -----------------------------------------------------------------------


def from_of(select: exp.Select) -> exp.Expression | None:
    """The relation in `select`'s FROM (sqlglot names the arg `from` or `from_`)."""
    source = select.args.get("from") or select.args.get("from_")
    return source.this if isinstance(source, exp.From) else None


def set_from(select: exp.Select, relation: exp.Expression) -> None:
    key = "from_" if "from_" in type(select).arg_types else "from"
    select.set(key, exp.From(this=relation))


def relation_alias(relation: exp.Expression) -> str:
    """The name a relation's columns are qualified with."""
    return relation.alias_or_name


def _subquery(select: exp.Select, alias: str) -> exp.Subquery:
    return exp.Subquery(
        this=select, alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True))
    )


def join_kind(join: exp.Join) -> str | Uncuttable:
    """`inner`, `left`, `right`, `full`, `semi`, `anti` or `cross`, or why not one of them."""
    if join.args.get("method"):
        return Uncuttable(f"a {join.args['method']} join")
    if join.args.get("using"):
        return Uncuttable("a JOIN ... USING the optimizer did not rewrite")
    side = str(join.args.get("side") or "").upper()
    kind = str(join.args.get("kind") or "").upper()
    has_on = join.args.get("on") is not None
    if kind in ("SEMI", "ANTI"):
        if side not in ("", "LEFT"):
            return Uncuttable(f"a {side} {kind} join")
        return kind.lower() if has_on else Uncuttable(f"a {kind} join without ON")
    if kind == "CROSS":
        return "cross"
    if side in ("LEFT", "RIGHT", "FULL"):
        if not has_on:
            return Uncuttable(f"a {side} join without ON")
        return side.lower()
    if kind not in ("", "INNER", "OUTER"):
        return Uncuttable(f"a {kind} join")
    return "inner" if has_on else "cross"


def _constant(node: exp.Expression | None, what: str) -> int | Uncuttable | None:
    if node is None:
        return None
    value = node.args.get("expression")
    if isinstance(value, exp.Literal) and not value.is_string:
        try:
            return int(value.this)
        except ValueError:
            pass
    return Uncuttable(
        f"a{'n' if what == 'OFFSET' else ''} {what} that is not a constant "
        f"({node.sql(dialect='duckdb')})"
    )


_CHECKS: list[tuple[str, str]] = [
    ("with", "a WITH clause the optimizer could not inline"),
    ("laterals", "a LATERAL view"),
    ("sample", "TABLESAMPLE"),
    ("pivots", "a PIVOT"),
]


def _is_aggregating(select: exp.Select, aggregate_names: Collection[str]) -> bool:
    return bool(
        select.args.get("group")
        or select.args.get("having")
        or select.args.get("distinct")
        or aggregate_calls(select, aggregate_names)
    )


# -- ORDER BY / LIMIT / OFFSET -------------------------------------------------------------


def _order_target(ordered: exp.Ordered, top: exp.Select, names: list[str]) -> str | None:
    """The result column an ORDER BY key already is, or None."""
    target = ordered.this
    if isinstance(target, exp.Literal) and not target.is_string:
        index = int(target.this) - 1
        return names[index] if 0 <= index < len(names) else None
    if isinstance(target, exp.Column) and not target.table and target.name in names:
        return target.name
    for projection, name in zip(top.expressions, names, strict=True):
        if projection.unalias() == target:
            return name
    return None


def _first_select(node: exp.Expression) -> exp.Select | None:
    """The left-most SELECT of a set operation: it names the result's columns."""
    while isinstance(node, (exp.SetOperation, exp.Subquery)):
        node = node.this
    return node if isinstance(node, exp.Select) else None


Lifted = tuple[tuple[exp.Ordered, ...], int | None, int, tuple[str, ...]]


def _lift_order_and_limit(
    top: exp.Select | exp.SetOperation, *, hide: bool = True
) -> Lifted | Uncuttable:
    """Remove ORDER BY / LIMIT / OFFSET from `top`; express the order over result columns.

    An ORDER BY key that is not a result column becomes a hidden result column when
    `hide` (never on a set operation, whose columns are positional)."""
    limit = _constant(top.args.get("limit"), "LIMIT")
    if isinstance(limit, Uncuttable):
        return limit
    offset = _constant(top.args.get("offset"), "OFFSET")
    if isinstance(offset, Uncuttable):
        return offset
    order = top.args.get("order")
    for arg in ("limit", "order", "offset"):
        top.set(arg, None)
    if order is None:
        return (), limit, offset or 0, ()

    first = top if isinstance(top, exp.Select) else _first_select(top)
    if first is None:  # pragma: no cover - sqlglot always ends in a SELECT
        return Uncuttable("a set operation without a SELECT")
    names = [projection.alias_or_name for projection in first.expressions]
    lifted: list[exp.Ordered] = []
    hidden: list[str] = []
    for ordered in order.expressions:
        if not isinstance(ordered, exp.Ordered):  # pragma: no cover - sqlglot always wraps
            return Uncuttable(f"an ORDER BY item sqlglot did not wrap ({ordered.sql()})")
        name = _order_target(ordered, first, names)
        if name is None:
            if not isinstance(top, exp.Select) or not hide:
                return Uncuttable(
                    f"ORDER BY {ordered.this.sql(dialect='duckdb')}, not a result column, "
                    "where no column can be added"
                )
            if top.args.get("distinct") and not _is_distinct_on(top):
                return Uncuttable("ORDER BY a column outside a SELECT DISTINCT")
            name = f"{HIDDEN_ORDER_PREFIX}{len(hidden)}"
            top.append("expressions", exp.alias_(ordered.this.copy(), name, quoted=True))
            hidden.append(name)
        item = ordered.copy()
        item.set("this", exp.column(name, quoted=True))
        lifted.append(item)
    if _is_distinct_on(top):
        # DISTINCT ON keeps the first row of each key *in this ORDER BY*: the order is part
        # of what it computes, so it stays (and the coordinator orders the result too).
        top.set("order", order)
    return tuple(lifted), limit, offset or 0, tuple(hidden)


def _is_distinct_on(select: exp.Expression) -> bool:
    distinct = select.args.get("distinct") if isinstance(select, exp.Select) else None
    return distinct is not None and distinct.args.get("on") is not None


def _keeps_order(select: exp.Select, aggregate_names: Collection[str]) -> bool:
    """True when `select` passes its FROM's row order through: projections and filters."""
    return not (
        select.args.get("joins")
        or _is_aggregating(select, aggregate_names)
        or windows_of(select)
        or select.args.get("qualify")
        or select.args.get("order")
    )


def _settle_subquery_orders(select: exp.Select, aggregate_names: Collection[str]) -> None:
    """Move each FROM subquery's ORDER BY without a LIMIT up into `select`, or drop it.

    Bottom up, so an order climbs through several projections. It moves when `select`
    only projects and filters (and has no ORDER BY of its own) and every key is one of
    the subquery's columns; it is dropped when `select` aggregates, joins, windows or
    orders on its own -- the order then means nothing. Otherwise it stays, and the plan
    runs single-node.
    """
    relations = [from_of(select)] + [j.this for j in select.args.get("joins") or []]
    for relation in relations:
        if not (isinstance(relation, exp.Subquery) and isinstance(relation.this, exp.Select)):
            continue
        inner = relation.this
        _settle_subquery_orders(inner, aggregate_names)
        order = inner.args.get("order")
        if order is None or inner.args.get("limit") or inner.args.get("offset"):
            continue
        if _is_distinct_on(inner):
            continue  # its ORDER BY chooses the row DISTINCT ON keeps: it stays
        if len(relations) > 1 or not _keeps_order(select, aggregate_names):
            inner.set("order", None)
            continue
        names = [p.alias_or_name for p in inner.expressions]
        moved: list[exp.Ordered] = []
        for ordered in order.expressions:
            name = _order_target(ordered, inner, names)
            if name is None:
                break
            item = ordered.copy()
            item.set("this", exp.column(name, table=relation_alias(relation), quoted=True))
            moved.append(item)
        else:
            inner.set("order", None)
            select.set("order", exp.Order(expressions=moved))


# -- IN (subquery) -> SEMI JOIN ------------------------------------------------------------


def _bound_aliases(select: exp.Select) -> set[str]:
    """Every relation alias defined anywhere inside `select`."""
    names: set[str] = set()
    for node in select.find_all(exp.Table, exp.Subquery):
        alias = node.alias_or_name
        if alias:
            names.add(alias)
    return names


def _in_subquery_to_semi_join(select: exp.Select, counter: list[int]) -> None:
    """Rewrite each uncorrelated `x IN (SELECT y ...)` conjunct of the WHERE in place."""
    where = select.args.get("where")
    if where is None:
        return
    kept: list[exp.Expression] = []
    joins: list[exp.Join] = []
    for conjunct in conjuncts(where.this):
        query = conjunct.args.get("query") if isinstance(conjunct, exp.In) else None
        inner = query.this if isinstance(query, exp.Subquery) else query
        if (
            not isinstance(inner, exp.Select)
            or from_of(inner) is None
            or conjunct.args.get("expressions")
            or len(inner.expressions) != 1
            or isinstance(inner.expressions[0].unalias(), exp.Star)
        ):
            kept.append(conjunct)
            continue
        bound = _bound_aliases(inner)
        if any(c.table and c.table not in bound for c in inner.find_all(exp.Column)):
            kept.append(conjunct)  # correlated: refused later as "a subquery"
            continue
        alias = f"{_SEMI_ALIAS}{counter[0]}"
        counter[0] += 1
        name = inner.expressions[0].alias_or_name
        if not name:
            kept.append(conjunct)
            continue
        condition = exp.EQ(
            this=conjunct.this.copy(), expression=exp.column(name, table=alias, quoted=True)
        )
        joins.append(exp.Join(this=_subquery(inner.copy(), alias), kind="SEMI", on=condition))
    if not joins:
        return
    for join in joins:
        select.append("joins", join)
    select.set("where", exp.Where(this=exp.and_(*kept)) if kept else None)


# -- splitting a SELECT in two -----------------------------------------------------------


def _split_aggregating_join(select: exp.Select, aliases: set[str]) -> exp.Select:
    """`SELECT agg FROM a JOIN b ... WHERE w GROUP BY g` ->
    `SELECT agg' FROM (SELECT <columns> FROM a JOIN b ... WHERE w) AS __icefleet_j GROUP BY g'`.

    `select` is changed in place into the upper half, which is returned too; the join is
    its FROM. Column references to the joined relations become references to the
    subquery's columns, named after the column where that is unambiguous. (Windows and
    QUALIFY move up with the projection.)
    """
    # Only what moves to the upper half: its projection, GROUP BY, HAVING and QUALIFY.
    # (ON and WHERE stay below; a joined subquery's own columns are its own.)
    referenced: list[tuple[str, str]] = []
    for root in _roots(select, ("group", "having", "qualify")):
        for column in root.find_all(exp.Column):
            pair = (column.table, column.name)
            if column.table in aliases and pair not in referenced:
                referenced.append(pair)
    counts: dict[str, int] = {}
    for _, name in referenced:
        counts[name.lower()] = counts.get(name.lower(), 0) + 1
    names: dict[tuple[str, str], str] = {}
    used: set[str] = set()
    for table, name in referenced:
        candidate = name if counts[name.lower()] == 1 else f"{table}_{name}"
        unique, suffix = candidate, 1
        while unique.lower() in used:
            unique, suffix = f"{candidate}_{suffix}", suffix + 1
        used.add(unique.lower())
        names[(table, name)] = unique

    inner = exp.select(
        *[
            exp.alias_(
                exp.column(name, table=table, quoted=True), names[(table, name)], quoted=True
            )
            for table, name in referenced
        ]
        or [exp.alias_(exp.true(), "__icefleet_row", quoted=True)]
    )
    set_from(inner, from_of(select).copy())  # type: ignore[union-attr]
    for join in select.args.get("joins") or []:
        inner.append("joins", join.copy())
    if select.args.get("where") is not None:
        inner.set("where", select.args["where"].copy())

    def rewrite(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and (node.table, node.name) in names:
            return exp.column(names[(node.table, node.name)], table=JOIN_ALIAS, quoted=True)
        return node

    for arg in ("expressions", "group", "having", "qualify"):
        value = select.args.get(arg)
        if isinstance(value, list):
            select.set(arg, [item.transform(rewrite) for item in value])
        elif isinstance(value, exp.Expression):
            select.set(arg, value.transform(rewrite))
    select.set("joins", None)
    select.set("where", None)
    set_from(select, _subquery(inner, JOIN_ALIAS))
    return select


def _split_window_over_aggregate(
    select: exp.Select, aggregate_names: Collection[str]
) -> exp.Select | Uncuttable:
    """`SELECT k, sum(x), rank() OVER (ORDER BY sum(x)) FROM t GROUP BY k` ->
    `SELECT w.__g0, w.__a0, rank() OVER (ORDER BY w.__a0) FROM (SELECT k AS __g0,
    sum(x) AS __a0 FROM t GROUP BY k) AS __icefleet_w`.

    The aggregate (with its WHERE, JOINs, GROUP BY and HAVING) is the lower half; the
    windows and QUALIFY read its columns. Returns the upper half.
    """
    group = select.args.get("group")
    if group is not None and (
        any(group.args.get(a) for a in ("rollup", "cube", "grouping_sets", "all"))
        or any(isinstance(k, (exp.Rollup, exp.Cube, exp.GroupingSets)) for k in group.expressions)
    ):
        return Uncuttable("a window over GROUPING SETS / ROLLUP / CUBE")
    upper = select.copy()
    keys = [k.copy() for k in (group.expressions if group is not None else [])]
    calls = aggregate_calls(upper, aggregate_names)
    lower = exp.select(
        *[exp.alias_(k.copy(), f"__g{i}", quoted=True) for i, k in enumerate(keys)],
        *[exp.alias_(c.copy(), f"__a{j}", quoted=True) for j, c in enumerate(calls)],
    )
    if not lower.expressions:  # pragma: no cover - a window over an aggregate has one
        lower = exp.select(exp.alias_(exp.true(), "__icefleet_row", quoted=True))
    set_from(lower, from_of(select).copy())  # type: ignore[union-attr]
    for join in select.args.get("joins") or []:
        lower.append("joins", join.copy())
    for arg in ("where", "group", "having"):
        if select.args.get(arg) is not None:
            lower.set(arg, select.args[arg].copy())

    for j, call in enumerate(calls):
        call.replace(exp.column(f"__a{j}", table=WINDOW_ALIAS, quoted=True))

    def swap(node: exp.Expression) -> exp.Expression:
        for i, key in enumerate(keys):
            if node == key:
                return exp.column(f"__g{i}", table=WINDOW_ALIAS, quoted=True)
        return node

    for arg in ("expressions", "qualify"):
        value = upper.args.get(arg)
        if isinstance(value, list):
            upper.set(arg, [item.transform(swap) for item in value])
        elif isinstance(value, exp.Expression):
            upper.set(arg, value.transform(swap))
    for arg in ("joins", "where", "group", "having"):
        upper.set(arg, None)
    set_from(upper, _subquery(lower, WINDOW_ALIAS))
    return upper


def _split_distinct_over_window(select: exp.Select) -> exp.Select:
    """`SELECT DISTINCT a, rank() OVER (...) AS r FROM t` -> `SELECT DISTINCT w.a, w.r FROM
    (SELECT a, rank() OVER (...) AS r FROM t) AS __icefleet_w`: windows come first."""
    lower = select.copy()
    lower.set("distinct", None)
    upper = exp.select(
        *[
            exp.alias_(
                exp.column(p.alias_or_name, table=WINDOW_ALIAS, quoted=True),
                p.alias_or_name,
                quoted=True,
            )
            for p in lower.expressions
        ]
    )
    upper.set("distinct", exp.Distinct())
    set_from(upper, _subquery(lower, WINDOW_ALIAS))
    return upper


# -- recognising the tree ------------------------------------------------------------------


class _Recogniser:
    def __init__(self, sources: Mapping[str, ScanSource], aggregate_names: Collection[str]) -> None:
        self.sources = sources
        self.aggregate_names = aggregate_names
        self.counter = [0]

    def relation(self, relation: exp.Expression | None) -> RelNode | Uncuttable:
        if isinstance(relation, exp.Subquery) and isinstance(relation.this, exp.Select):
            return self.limited(relation.this, hide=True)
        if isinstance(relation, exp.Subquery) and isinstance(relation.this, exp.SetOperation):
            return self.setop(relation.this)
        if not isinstance(relation, exp.Table) or isinstance(relation.this, exp.Func):
            return Uncuttable("a FROM that is not a single Iceberg table")
        key = source_key(relation, versioned=True)
        if key not in self.sources:
            return Uncuttable(f"a relation the session holds locally ({key}), not an Iceberg table")
        return Scan(key=key, table=relation)

    def limited(self, select: exp.Select, *, hide: bool) -> RelNode | Uncuttable:
        """A SELECT that may carry its own ORDER BY / LIMIT / OFFSET (not the top one)."""
        if not (select.args.get("limit") or select.args.get("offset")):
            if select.args.get("order") and not _is_distinct_on(select):
                if hide:
                    # _settle_subquery_orders left it: a key that is not a column.
                    return Uncuttable("an ORDER BY inside a subquery, on something not selected")
                select = select.copy()  # a set-operation branch: the order means nothing
                select.set("order", None)
            return self.select(select)
        inner = select.copy()
        names = tuple(p.alias_or_name for p in inner.expressions)
        lifted = _lift_order_and_limit(inner, hide=hide)
        if isinstance(lifted, Uncuttable):
            return lifted
        order, limit, offset, _ = lifted
        child = self.select(inner)
        if isinstance(child, Uncuttable):
            return child
        return TopN(child=child, order=order, limit=limit, offset=offset, columns=names)

    def setop(self, node: exp.SetOperation) -> RelNode | Uncuttable:
        if node.args.get("by_name"):
            return Uncuttable("UNION BY NAME")
        if node.args.get("order") or node.args.get("limit") or node.args.get("offset"):
            return Uncuttable("ORDER BY / LIMIT on a set operation inside a subquery")
        kinds: dict[type, str] = {
            exp.Union: "union",
            exp.Intersect: "intersect",
            exp.Except: "except",
        }
        kind = kinds.get(type(node))
        if kind is None:  # pragma: no cover - sqlglot has these three
            return Uncuttable(f"a {type(node).__name__}")
        sides: list[RelNode] = []
        for branch in (node.this, node.expression):
            if isinstance(branch, exp.Subquery):
                branch = branch.this
            side: RelNode | Uncuttable
            if isinstance(branch, exp.SetOperation):
                side = self.setop(branch)
            elif isinstance(branch, exp.Select):
                side = self.limited(branch, hide=False)
            else:
                side = Uncuttable(f"a set operation over a {type(branch).__name__}")
            if isinstance(side, Uncuttable):
                return side
            sides.append(side)
        return SetOp(
            kind=kind,
            distinct=bool(node.args.get("distinct")),
            left=sides[0],
            right=sides[1],
            node=node,
        )

    def select(self, select: exp.Select) -> RelNode | Uncuttable:
        for arg, reason in _CHECKS:
            if select.args.get(arg):
                return Uncuttable(reason)
        distinct = select.args.get("distinct")
        distinct_on = distinct is not None and distinct.args.get("on") is not None
        _in_subquery_to_semi_join(select, self.counter)

        relations = [from_of(select)] + [j.this for j in select.args.get("joins") or []]
        relation_ids = {id(r) for r in relations if r is not None}
        for nested in select.find_all(exp.Select):
            if nested is select:
                continue
            # Only a relation of this SELECT (or something inside one) may be a SELECT.
            parent = nested.parent
            while parent is not None and parent is not select and id(parent) not in relation_ids:
                parent = parent.parent
            if parent is select or parent is None:
                return Uncuttable("a subquery")

        windows = windows_of(select)
        if select.args.get("qualify") is not None and not windows:
            return Uncuttable("QUALIFY without a window function")
        aggregating = bool(
            select.args.get("group")
            or select.args.get("having")
            or aggregate_calls(select, self.aggregate_names)
        )
        if distinct and not distinct_on and aggregating:
            return Uncuttable("SELECT DISTINCT over an aggregate")
        if distinct_on and (aggregating or windows):
            return Uncuttable("DISTINCT ON with an aggregate or a window function")
        if windows and distinct:
            return self.over(_split_distinct_over_window(select), Aggregate)
        if windows and aggregating:
            upper = _split_window_over_aggregate(select, self.aggregate_names)
            if isinstance(upper, Uncuttable):
                return upper
            return self.over(upper, Window)

        joins = select.args.get("joins") or []
        if not joins:
            child = self.relation(relations[0])
            if isinstance(child, Uncuttable):
                return child
            if windows:
                return Window(child=child, select=select)
            if distinct_on:
                return DistinctOn(child=child, select=select)
            if aggregating or distinct:
                return Aggregate(child=child, select=select)
            return Project(child=child, select=select)

        for join in joins:
            kind = join_kind(join)
            if isinstance(kind, Uncuttable):
                return kind
        aliases = [relation_alias(r) for r in relations if r is not None]
        if len(set(aliases)) != len(aliases) or len(aliases) != len(relations):
            return Uncuttable("two relations of one join share an alias")
        if any(isinstance(p.unalias(), exp.Star) for p in select.expressions) or any(
            isinstance(c.this, exp.Star) for c in select.find_all(exp.Column)
        ):
            return Uncuttable("a * over a join the optimizer did not expand")
        if aggregating or distinct or windows:
            if distinct_on:
                return Uncuttable("DISTINCT ON over a join")
            upper = _split_aggregating_join(select, set(aliases))
            return self.over(upper, Window if windows else Aggregate)

        children: list[RelNode] = []
        for relation in relations:
            child = self.relation(relation)
            if isinstance(child, Uncuttable):
                return child
            children.append(child)
        return Join(select=select, children=tuple(children))

    def over(self, upper: exp.Select, kind: type[Aggregate] | type[Window]) -> RelNode | Uncuttable:
        """`kind` over the lower half a split put in `upper`'s FROM."""
        inner = from_of(upper)
        assert isinstance(inner, exp.Subquery) and isinstance(inner.this, exp.Select)
        lower = self.select(inner.this)
        if isinstance(lower, Uncuttable):
            return lower
        return kind(child=lower, select=upper)


def from_sqlglot(
    tree: exp.Expression,
    sources: Mapping[str, ScanSource],
    *,
    udf_names: Collection[str] = (),
    aggregate_names: Collection[str] = (),
) -> Query | Uncuttable:
    """Recognise `tree` as a plan over Iceberg tables, or say why it is not one."""
    if not isinstance(tree, (exp.Select, exp.SetOperation)):
        return Uncuttable(f"a {type(tree).__name__} statement, not a SELECT")
    aggregate_names = {name.lower() for name in aggregate_names}
    top = tree.copy()

    wanted = {name.lower() for name in udf_names}
    if wanted:
        for call in top.find_all(exp.Anonymous):
            if str(call.this).lower() in wanted:
                return Uncuttable(f"a Python UDF call ({call.this}) -- Phase 8")

    # Bottom up from every SELECT that is not inside another's FROM is enough: the
    # settling recurses into FROM subqueries itself.
    for select in list(top.find_all(exp.Select)):
        if not isinstance(select.parent, exp.Subquery):
            _settle_subquery_orders(select, aggregate_names)

    lifted = _lift_order_and_limit(top)
    if isinstance(lifted, Uncuttable):
        return lifted
    order, limit, offset, hidden = lifted

    recogniser = _Recogniser(sources, aggregate_names)
    root = recogniser.setop(top) if isinstance(top, exp.SetOperation) else recogniser.select(top)
    if isinstance(root, Uncuttable):
        return root
    assert not isinstance(root, Scan)
    return Query(tree=top, root=root, order=order, limit=limit, hidden=hidden, offset=offset)
