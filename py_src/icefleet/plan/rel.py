"""The thin relational layer over icetl's optimized sqlglot tree (PLAN §4.1).

Phase 2 recognises a *chain* of single-table `SELECT`s, each reading the one below it as a
subquery, the bottom one reading an Iceberg table:

    Project* -> Aggregate? -> Project* -> Scan

`Project` is anything DuckDB does row at a time (filter, map, function calls). `Aggregate`
is a `GROUP BY`, a global aggregate or a `SELECT DISTINCT`, with its `HAVING`; at most one
in the chain. That covers how icetl's optimizer leaves both surfaces: a flat aggregate,
a filter or sort over an aggregate subquery, an aggregate over a projection subquery.

`ORDER BY` / `LIMIT` on the top `SELECT` are lifted off it into the `Query`: the
coordinator applies them while reading the result (kickoff answer 2), and each task
applies a `LIMIT` locally first. An `ORDER BY` key that is not a result column becomes a
hidden result column, which the coordinator orders by and then drops.

Anything else -- a join, a window, a subquery in an expression, a set operation, an
aggregate over an aggregate -- is `Uncuttable` and runs single-node (P3), with the reason
naming the phase that will distribute it.

The nodes wrap sqlglot nodes and never re-implement an expression: every stage's SQL is
built from the very nodes of the optimized tree.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from icetl.plan.builder import collect_source_keys, source_key
from sqlglot import exp

from icefleet.errors import Uncuttable

if TYPE_CHECKING:
    from icetl.plan.builder import ScanSource

__all__ = [
    "Aggregate",
    "Project",
    "Query",
    "RelNode",
    "Scan",
    "aggregate_calls",
    "from_of",
    "from_sqlglot",
    "is_aggregate_call",
]

HIDDEN_ORDER_PREFIX = "__icefleet_order_"


@dataclass(frozen=True)
class Scan:
    """An Iceberg table reference, by the source key icetl resolved it under."""

    key: str


@dataclass(frozen=True, eq=False)
class Project:
    """Row-at-a-time work over `child`: `select` reads `child` in its FROM."""

    child: RelNode
    select: exp.Select


@dataclass(frozen=True, eq=False)
class Aggregate:
    """`GROUP BY` / global aggregate / `SELECT DISTINCT` over `child`, with its `HAVING`.

    `select` still carries its own `WHERE`: rows are filtered before they are aggregated,
    so the `WHERE` belongs to the partial (scan) side of the cut.
    """

    child: RelNode
    select: exp.Select


RelNode = Scan | Project | Aggregate


@dataclass(frozen=True, eq=False)
class Query:
    """A recognised plan: the chain, and what the coordinator applies to its result.

    `tree` is a private copy of the optimized tree, with the top `SELECT`'s `ORDER BY`
    and `LIMIT` removed and any hidden order columns appended to its projection. The
    nodes in `root` point into `tree`, so a stage planner may replace one in place.
    """

    tree: exp.Select
    root: Project | Aggregate
    order: tuple[exp.Ordered, ...] = ()
    limit: int | None = None
    hidden: tuple[str, ...] = ()

    def nodes(self) -> Iterator[RelNode]:
        node: RelNode = self.root
        while True:
            yield node
            if isinstance(node, Scan):
                return
            node = node.child

    @property
    def scan(self) -> Scan:
        last = list(self.nodes())[-1]
        assert isinstance(last, Scan)
        return last

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


# -- recognising the chain -------------------------------------------------------------


def from_of(select: exp.Select) -> exp.Expression | None:
    """The relation in `select`'s FROM (sqlglot names the arg `from` or `from_`)."""
    source = select.args.get("from") or select.args.get("from_")
    return source.this if isinstance(source, exp.From) else None


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
    ("joins", "a join -- Phase 3"),
    ("laterals", "a LATERAL view -- Phase 3"),
    ("qualify", "QUALIFY -- Phase 4"),
    ("offset", "OFFSET -- Phase 4"),
    ("sample", "TABLESAMPLE"),
]


def _is_aggregating(select: exp.Select, aggregate_names: Collection[str]) -> bool:
    return bool(
        select.args.get("group")
        or select.args.get("having")
        or select.args.get("distinct")
        or aggregate_calls(select, aggregate_names)
    )


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


def from_sqlglot(
    tree: exp.Expression,
    sources: Mapping[str, ScanSource],
    *,
    udf_names: Collection[str] = (),
    aggregate_names: Collection[str] = (),
) -> Query | Uncuttable:
    """Recognise `tree` as a chain over one Iceberg table, or say why it is not one."""
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
    if top.args.get("offset"):
        return Uncuttable("OFFSET -- Phase 4")

    lifted = _lift_order_and_limit(top)
    if isinstance(lifted, Uncuttable):
        return lifted
    order, limit, hidden = lifted

    # Walk down the chain, top to bottom.
    selects: list[exp.Select] = []
    node: exp.Select = top
    key: str | None = None
    while True:
        for arg, reason in _CHECKS:
            if node.args.get(arg):
                return Uncuttable(reason)
        if node is not top and (node.args.get("order") or node.args.get("limit")):
            return Uncuttable("ORDER BY / LIMIT inside a subquery -- Phase 4")
        if node.args.get("distinct") and node.args["distinct"].args.get("on"):
            return Uncuttable("DISTINCT ON -- Phase 4")
        selects.append(node)
        relation = from_of(node)
        if isinstance(relation, exp.Subquery) and isinstance(relation.this, exp.Select):
            node = relation.this
            continue
        if isinstance(relation, exp.Subquery) and isinstance(relation.this, exp.SetOperation):
            return Uncuttable("a set operation (UNION / INTERSECT / EXCEPT) -- Phase 4")
        if not isinstance(relation, exp.Table) or isinstance(relation.this, exp.Func):
            return Uncuttable("a FROM that is not a single Iceberg table")
        key = source_key(relation, versioned=True)
        if key not in sources:
            return Uncuttable(f"a relation the session holds locally ({key}), not an Iceberg table")
        break

    chain_ids = {id(select) for select in selects}
    if any(id(select) not in chain_ids for select in top.find_all(exp.Select)):
        return Uncuttable("a subquery")
    if collect_source_keys(top) != [key]:
        return Uncuttable("more than one table reference")

    aggregating = [s for s in selects if _is_aggregating(s, aggregate_names)]
    if len(aggregating) > 1:
        return Uncuttable("an aggregate over an aggregate")
    for select in aggregating:
        distinct = select.args.get("distinct")
        if distinct and (select.args.get("group") or aggregate_calls(select, aggregate_names)):
            return Uncuttable("SELECT DISTINCT over an aggregate")

    aggregating_ids = {id(select) for select in aggregating}  # identity: sqlglot's == is structural
    rel: RelNode = Scan(key=key)
    for select in reversed(selects):
        rel = (
            Aggregate(child=rel, select=select)
            if id(select) in aggregating_ids
            else Project(child=rel, select=select)
        )
    assert not isinstance(rel, Scan)
    return Query(tree=top, root=rel, order=order, limit=limit, hidden=hidden)
