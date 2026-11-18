"""The thin relational layer over icetl's optimized sqlglot tree (PLAN §4.1).

A recognised plan is a tree of four node kinds, each wrapping the sqlglot `SELECT` it came
from -- the nodes never re-implement an expression; every stage's SQL is built from the
very nodes of the optimized tree:

    Scan       an Iceberg table reference
    Project    a SELECT over one relation, row at a time (filter, map, function calls)
    Aggregate  a GROUP BY, a global aggregate or a SELECT DISTINCT over one relation,
               with its HAVING
    Join       a SELECT whose FROM joins two or more relations (Phase 3): its projection,
               FROM, JOINs and WHERE; one child per relation, in FROM order

icetl's optimizer leaves a join as one flat SELECT (`FROM a JOIN b ON ... WHERE ...
GROUP BY ...`). An aggregating one is split here: the join becomes a subquery
`__icefleet_j` that projects every column the aggregate reads, and the aggregate reads it
-- so an aggregate always has exactly one relation below it, as in Phase 2.

An uncorrelated `x IN (SELECT y ...)` that is a top-level conjunct of a WHERE becomes a
`SEMI JOIN` on `x = y` (kickoff answer 1): in a WHERE, NULL and false both drop the row,
so the two agree on every input, NULLs included. `NOT IN`, `EXISTS` and correlated
subqueries are refused.

`ORDER BY` / `LIMIT` on the top `SELECT` are lifted into the `Query`: the coordinator
applies them while reading the result, and each final task applies a `LIMIT` locally
first. An `ORDER BY` key that is not a result column becomes a hidden result column,
which the coordinator orders by and then drops.

Anything else -- a window, a set operation, a subquery in an expression, an aggregate
directly over an aggregate -- is `Uncuttable` and runs single-node (P3), with the reason
naming the phase that will distribute it.
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
    "Aggregate",
    "Join",
    "Project",
    "Query",
    "RelNode",
    "Scan",
    "aggregate_calls",
    "from_of",
    "from_sqlglot",
    "is_aggregate_call",
    "join_kind",
    "relation_alias",
]

HIDDEN_ORDER_PREFIX = "__icefleet_order_"
#: The alias of the join subquery an aggregating join SELECT is split into.
JOIN_ALIAS = "__icefleet_j"
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

    No GROUP BY, HAVING or DISTINCT: an aggregating SELECT is split into an Aggregate
    over a Join first.
    """

    select: exp.Select
    children: tuple[RelNode, ...]


RelNode = Scan | Project | Aggregate | Join


@dataclass(frozen=True, eq=False)
class Query:
    """A recognised plan: the tree, and what the coordinator applies to its result.

    `tree` is a private copy of the optimized tree, with the top `SELECT`'s `ORDER BY`
    and `LIMIT` removed and any hidden order columns appended to its projection. The
    nodes in `root` point into `tree`.
    """

    tree: exp.Select
    root: Project | Aggregate | Join
    order: tuple[exp.Ordered, ...] = ()
    limit: int | None = None
    hidden: tuple[str, ...] = ()

    def nodes(self) -> Iterator[RelNode]:
        """Every node, top down, depth first, children in FROM order."""
        stack: list[RelNode] = [self.root]
        while stack:
            node = stack.pop()
            yield node
            if isinstance(node, (Project, Aggregate)):
                stack.append(node.child)
            elif isinstance(node, Join):
                stack.extend(reversed(node.children))

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


# -- aggregate calls -----------------------------------------------------------------

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


def aggregate_calls(
    select: exp.Select, aggregate_names: Collection[str] = ()
) -> list[exp.Expression]:
    """The outermost aggregate calls of `select`'s projection and `HAVING`, in order."""
    found: list[exp.Expression] = []
    roots: list[exp.Expression] = list(select.expressions)
    having = select.args.get("having")
    if having is not None:
        roots.append(having)

    def visit(node: exp.Expression) -> None:
        if isinstance(node, exp.Select):
            return  # a subquery is refused separately; its aggregates are its own
        if is_aggregate_call(node, aggregate_names):
            found.append(node)
            return
        for child in node.iter_expressions():
            visit(child)

    for root in roots:
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


def _limit_of(select: exp.Select) -> int | Uncuttable | None:
    node = select.args.get("limit")
    if node is None:
        return None
    value = node.args.get("expression") if isinstance(node, exp.Limit) else None
    if isinstance(value, exp.Literal) and not value.is_string:
        try:
            return int(value.this)
        except ValueError:
            pass
    return Uncuttable(f"a LIMIT that is not a constant ({node.sql(dialect='duckdb')})")


_CHECKS: list[tuple[str, str]] = [
    ("with", "a WITH clause the optimizer could not inline"),
    ("laterals", "a LATERAL view -- Phase 3"),
    ("qualify", "QUALIFY -- Phase 4"),
    ("offset", "OFFSET -- Phase 4"),
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


# -- ORDER BY / LIMIT ----------------------------------------------------------------------


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


def _lift_order_and_limit(
    top: exp.Select,
) -> tuple[tuple[exp.Ordered, ...], int | None, tuple[str, ...]] | Uncuttable:
    """Remove the top SELECT's ORDER BY / LIMIT; express the order over result columns."""
    limit = _limit_of(top)
    if isinstance(limit, Uncuttable):
        return limit
    order = top.args.get("order")
    top.set("limit", None)
    top.set("order", None)
    if order is None:
        return (), limit, ()

    names = [projection.alias_or_name for projection in top.expressions]
    lifted: list[exp.Ordered] = []
    hidden: list[str] = []
    for ordered in order.expressions:
        if not isinstance(ordered, exp.Ordered):  # pragma: no cover - sqlglot always wraps
            return Uncuttable(f"an ORDER BY item sqlglot did not wrap ({ordered.sql()})")
        name = _order_target(ordered, top, names)
        if name is None:
            if top.args.get("distinct"):
                return Uncuttable("ORDER BY a column outside a SELECT DISTINCT")
            name = f"{HIDDEN_ORDER_PREFIX}{len(hidden)}"
            top.append("expressions", exp.alias_(ordered.this.copy(), name, quoted=True))
            hidden.append(name)
        item = ordered.copy()
        item.set("this", exp.column(name, quoted=True))
        lifted.append(item)
    return tuple(lifted), limit, tuple(hidden)


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
        subquery = exp.Subquery(
            this=inner.copy(), alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True))
        )
        condition = exp.EQ(
            this=conjunct.this.copy(), expression=exp.column(name, table=alias, quoted=True)
        )
        joins.append(exp.Join(this=subquery, kind="SEMI", on=condition))
    if not joins:
        return
    for join in joins:
        select.append("joins", join)
    select.set("where", exp.Where(this=exp.and_(*kept)) if kept else None)


# -- splitting an aggregating join SELECT --------------------------------------------------


def _split_aggregating_join(select: exp.Select, aliases: set[str]) -> exp.Select:
    """`SELECT agg FROM a JOIN b ... WHERE w GROUP BY g` ->
    `SELECT agg' FROM (SELECT <columns> FROM a JOIN b ... WHERE w) AS __icefleet_j GROUP BY g'`.

    `select` is changed in place into the upper half, which is returned too; the join is
    its FROM. Column references to the joined relations become references to the
    subquery's columns, named after the column where that is unambiguous.
    """
    # Only what moves to the upper half: its projection, GROUP BY and HAVING. (ON and
    # WHERE stay below; a joined subquery's own columns are its own.)
    referenced: list[tuple[str, str]] = []
    roots: list[exp.Expression] = list(select.expressions)
    for arg in ("group", "having"):
        if select.args.get(arg) is not None:
            roots.append(select.args[arg])
    for root in roots:
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

    for arg in ("expressions", "group", "having"):
        value = select.args.get(arg)
        if isinstance(value, list):
            select.set(arg, [item.transform(rewrite) for item in value])
        elif isinstance(value, exp.Expression):
            select.set(arg, value.transform(rewrite))
    select.set("joins", None)
    select.set("where", None)
    set_from(
        select,
        exp.Subquery(
            this=inner, alias=exp.TableAlias(this=exp.to_identifier(JOIN_ALIAS, quoted=True))
        ),
    )
    return select


# -- recognising the tree ------------------------------------------------------------------


class _Recogniser:
    def __init__(self, sources: Mapping[str, ScanSource], aggregate_names: Collection[str]) -> None:
        self.sources = sources
        self.aggregate_names = aggregate_names
        self.counter = [0]

    def relation(self, relation: exp.Expression | None) -> RelNode | Uncuttable:
        if isinstance(relation, exp.Subquery) and isinstance(relation.this, exp.Select):
            inner = relation.this
            if inner.args.get("order") or inner.args.get("limit"):
                return Uncuttable("ORDER BY / LIMIT inside a subquery -- Phase 4")
            return self.select(inner)
        if isinstance(relation, exp.Subquery) and isinstance(relation.this, exp.SetOperation):
            return Uncuttable("a set operation (UNION / INTERSECT / EXCEPT) -- Phase 4")
        if not isinstance(relation, exp.Table) or isinstance(relation.this, exp.Func):
            return Uncuttable("a FROM that is not a single Iceberg table")
        key = source_key(relation, versioned=True)
        if key not in self.sources:
            return Uncuttable(f"a relation the session holds locally ({key}), not an Iceberg table")
        return Scan(key=key, table=relation)

    def select(self, select: exp.Select) -> RelNode | Uncuttable:
        for arg, reason in _CHECKS:
            if select.args.get(arg):
                return Uncuttable(reason)
        distinct = select.args.get("distinct")
        if distinct and distinct.args.get("on"):
            return Uncuttable("DISTINCT ON -- Phase 4")
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

        aggregating = _is_aggregating(select, self.aggregate_names)
        if (
            aggregating
            and distinct
            and (select.args.get("group") or aggregate_calls(select, self.aggregate_names))
        ):
            return Uncuttable("SELECT DISTINCT over an aggregate")

        joins = select.args.get("joins") or []
        if not joins:
            child = self.relation(relations[0])
            if isinstance(child, Uncuttable):
                return child
            if aggregating:
                if _reaches_aggregate(child):
                    return Uncuttable("an aggregate over an aggregate -- Phase 4")
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
        if aggregating:
            upper = _split_aggregating_join(select, set(aliases))
            inner = from_of(upper)
            assert isinstance(inner, exp.Subquery) and isinstance(inner.this, exp.Select)
            joined = self.select(inner.this)
            if isinstance(joined, Uncuttable):
                return joined
            return Aggregate(child=joined, select=upper)

        children: list[RelNode] = []
        for relation in relations:
            child = self.relation(relation)
            if isinstance(child, Uncuttable):
                return child
            children.append(child)
        return Join(select=select, children=tuple(children))


def _reaches_aggregate(node: RelNode) -> bool:
    """True when `node`, through projections only, is an aggregate."""
    while isinstance(node, Project):
        node = node.child
    return isinstance(node, Aggregate)


def from_sqlglot(
    tree: exp.Expression,
    sources: Mapping[str, ScanSource],
    *,
    udf_names: Collection[str] = (),
    aggregate_names: Collection[str] = (),
) -> Query | Uncuttable:
    """Recognise `tree` as a plan over Iceberg tables, or say why it is not one."""
    if isinstance(tree, exp.SetOperation):
        return Uncuttable("a set operation (UNION / INTERSECT / EXCEPT) -- Phase 4")
    if not isinstance(tree, exp.Select):
        return Uncuttable(f"a {type(tree).__name__} statement, not a SELECT")
    aggregate_names = {name.lower() for name in aggregate_names}
    top = tree.copy()

    wanted = {name.lower() for name in udf_names}
    if wanted:
        for call in top.find_all(exp.Anonymous):
            if str(call.this).lower() in wanted:
                return Uncuttable(f"a Python UDF call ({call.this}) -- Phase 8")
    if top.find(exp.Window) is not None:
        return Uncuttable("a window function -- Phase 4")
    if top.find(exp.SetOperation) is not None:
        return Uncuttable("a set operation (UNION / INTERSECT / EXCEPT) -- Phase 4")
    for select in top.find_all(exp.Select):
        if select.args.get("offset"):
            return Uncuttable("OFFSET -- Phase 4")

    lifted = _lift_order_and_limit(top)
    if isinstance(lifted, Uncuttable):
        return lifted
    order, limit, hidden = lifted

    root = _Recogniser(sources, aggregate_names).select(top)
    if isinstance(root, Uncuttable):
        return root
    assert not isinstance(root, Scan)
    return Query(tree=top, root=root, order=order, limit=limit, hidden=hidden)
