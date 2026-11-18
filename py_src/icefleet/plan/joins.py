"""Join strategy (PLAN §4.3): equality keys, which side may be broadcast, and which
filters may run below a join.

    equi-join, a side <= icefleet.broadcast.thresholdBytes   broadcast that side
    equi-join, both larger                                    shuffle hash join on the keys
    non-equi or cross, a side small enough                    broadcast it
    non-equi or cross, both larger                            single-node (Uncuttable)

Only a side whose unmatched rows are *dropped* may be broadcast: every task sees all of
it, so a row of it that matched nothing would be emitted once per task. So inner and
cross joins may broadcast either side, left / semi / anti joins the right side, a right
join the left side, and a full join neither.

The size of a side is estimated when planning (kickoff answer 2): a scan is the parquet
bytes of its selected columns (`ScanPlan.bytes_scanned`), an aggregate at most its
input's size, and a join's output is never broadcast. Choosing at run time, from the
measured manifest, is Phase 7's.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlglot import exp

__all__ = [
    "BROADCASTABLE",
    "conjuncts",
    "equi_keys",
    "is_deterministic",
    "null_supplied",
    "tables_of",
]

#: Join kind -> the sides that may be broadcast, the preferred one first.
BROADCASTABLE: dict[str, tuple[str, ...]] = {
    "inner": ("right", "left"),
    "cross": ("right", "left"),
    "left": ("right",),
    "semi": ("right",),
    "anti": ("right",),
    "right": ("left",),
    "full": (),
}

#: Calls whose value changes from row to row or run to run: never moved below a join.
_VOLATILE = ("random", "rand", "uuid", "gen_random_uuid", "nextval", "setseed", "now")


def conjuncts(condition: exp.Expression | None) -> list[exp.Expression]:
    """`a AND (b AND c)` -> `[a, b, c]`."""
    if condition is None:
        return []
    if isinstance(condition, exp.And):
        return conjuncts(condition.this) + conjuncts(condition.expression)
    if isinstance(condition, exp.Paren):
        return conjuncts(condition.this)
    return [condition]


def tables_of(expression: exp.Expression) -> set[str]:
    """The relation aliases `expression`'s columns are qualified with."""
    return {column.table for column in expression.find_all(exp.Column)}


def is_deterministic(expression: exp.Expression) -> bool:
    if expression.find(exp.Rand) is not None:
        return False
    text = expression.sql(dialect="duckdb").lower()
    return not any(f"{name}(" in text for name in _VOLATILE)


def equi_keys(
    on: exp.Expression | None, left: set[str], right: str
) -> list[tuple[exp.Expression, exp.Expression]]:
    """The `l = r` (or `l IS NOT DISTINCT FROM r`) conjuncts of `on`, each as (left, right).

    `l` reads only the relations already joined (`left`), `r` only the one being joined
    (`right`). Everything else in `on` is a residual condition: it stays in the ON and
    is evaluated per partition, which is correct once the keys co-locate the rows.
    """
    keys = []
    for conjunct in conjuncts(on):
        if not isinstance(conjunct, (exp.EQ, exp.NullSafeEQ)) or not is_deterministic(conjunct):
            continue
        a, b = conjunct.this, conjunct.expression
        ta, tb = tables_of(a), tables_of(b)
        if not ta or not tb or "" in ta | tb:
            continue
        if ta <= left and tb == {right}:
            keys.append((a, b))
        elif tb <= left and ta == {right}:
            keys.append((b, a))
    return keys


def null_supplied(aliases: Sequence[str], kinds: Sequence[str]) -> set[str]:
    """The relations of a left-deep join chain whose columns an outer join may make NULL.

    `aliases[0]` is the FROM relation, `aliases[i]` is joined by `kinds[i - 1]`.
    """
    supplied: set[str] = set()
    for index, kind in enumerate(kinds, start=1):
        if kind == "left":
            supplied.add(aliases[index])
        elif kind == "right":
            supplied.update(aliases[:index])
        elif kind == "full":
            supplied.update(aliases[: index + 1])
    return supplied
