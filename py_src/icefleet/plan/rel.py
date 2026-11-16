"""The thin relational layer over icetl's optimized sqlglot tree (PLAN §4.1).

Phase 1 recognises one shape -- `Project(Scan)`: a single `SELECT` over one Iceberg table
with any row-at-a-time expressions, a `WHERE`, and an optional `LIMIT`. That is exactly
the shape that distributes with no exchange: every split runs the same SQL and the
coordinator concatenates. Everything else is `Uncuttable` and runs single-node (P3), with
the reason naming the phase that will distribute it.

The nodes wrap sqlglot nodes and never re-implement an expression: the stage SQL is the
SQL icetl compiled, so a stage can never mean something different from the plan.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from icetl.plan.builder import collect_source_keys, source_key
from sqlglot import exp

from icefleet.errors import Uncuttable

if TYPE_CHECKING:
    from icetl.plan.builder import ScanSource

__all__ = ["Project", "Scan", "from_sqlglot"]


@dataclass(frozen=True)
class Scan:
    """An Iceberg table reference, by the source key icetl resolved it under."""

    key: str


@dataclass(frozen=True)
class Project:
    """Anything DuckDB does row at a time over its child: filter, map, function calls."""

    child: Scan
    select: exp.Select
    limit: int | None = None


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


def from_sqlglot(
    tree: exp.Expression,
    sources: Mapping[str, ScanSource],
    *,
    udf_names: Collection[str] = (),
) -> Project | Uncuttable:
    """Recognise `tree` as `Project(Scan)`, or say why it is not distributable in Phase 1."""
    if isinstance(tree, exp.SetOperation):
        return Uncuttable("a set operation (UNION / INTERSECT / EXCEPT) -- Phase 4")
    if not isinstance(tree, exp.Select):
        return Uncuttable(f"a {type(tree).__name__} statement, not a SELECT")

    checks: list[tuple[str, str]] = [
        ("with", "a WITH clause the optimizer could not inline"),
        ("joins", "a join -- Phase 3"),
        ("laterals", "a LATERAL view -- Phase 3"),
        ("group", "GROUP BY / aggregation -- Phase 2"),
        ("having", "HAVING -- Phase 2"),
        ("qualify", "QUALIFY -- Phase 4"),
        ("order", "ORDER BY -- Phase 4"),
        ("distinct", "DISTINCT -- Phase 2"),
        ("offset", "OFFSET -- Phase 4"),
        ("sample", "TABLESAMPLE"),
    ]
    for arg, reason in checks:
        if tree.args.get(arg):
            return Uncuttable(reason)
    if tree.find(exp.AggFunc) is not None:
        return Uncuttable("an aggregate function -- Phase 2")
    if tree.find(exp.Window) is not None:
        return Uncuttable("a window function -- Phase 4")
    if any(node is not tree for node in tree.find_all(exp.Select)):
        return Uncuttable("a subquery")

    source = tree.args.get("from") or tree.args.get("from_")
    relation = source.this if isinstance(source, exp.From) else None
    if not isinstance(relation, exp.Table) or isinstance(relation.this, exp.Func):
        return Uncuttable("a FROM that is not a single Iceberg table")
    key = source_key(relation, versioned=True)
    if key not in sources:
        return Uncuttable(f"a relation the session holds locally ({key}), not an Iceberg table")
    if collect_source_keys(tree) != [key]:
        return Uncuttable("more than one table reference")

    wanted = {name.lower() for name in udf_names}
    if wanted:
        for call in tree.find_all(exp.Anonymous):
            if str(call.this).lower() in wanted:
                return Uncuttable(f"a Python UDF call ({call.this}) -- Phase 8")

    limit = _limit_of(tree)
    if isinstance(limit, Uncuttable):
        return limit
    return Project(child=Scan(key=key), select=tree, limit=limit)
