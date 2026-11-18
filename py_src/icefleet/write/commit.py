"""The coordinator's commit: the files the workers wrote, in one Iceberg transaction
(PLAN §4.6 step 4, kickoff answer 4).

    append       add_files(paths)                                   one snapshot
    overwrite    delete(everything) + add_files(paths)              one commit, two snapshots
    dynamic      delete(the partitions written) + add_files(paths)  one commit, two snapshots

-- the same snapshots icetl's own `table.overwrite` / `dynamic_partition_overwrite` make.
`commit_with_retry` (icetl's) reruns the whole transaction against freshly loaded
metadata when a concurrent writer wins the race; `add_files` refuses a file already in
the table, so a retry cannot add one twice.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time
from typing import TYPE_CHECKING, Any

from icetl.sql.writer import commit_with_retry

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.table import Table

__all__ = ["commit_files", "partition_filter"]


def _literal(value: Any) -> Any:
    """A value PyIceberg binds to the column's type: dates and times as ISO text."""
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    return value


def partition_filter(partitions: Sequence[Mapping[str, Any]]) -> BooleanExpression:
    """`(a = 1 AND b IS NULL) OR (a = 2 AND b = 'x') ...` over identity partition columns."""
    from pyiceberg import expressions

    # The constructors take (term, literal) at run time; their pydantic field signature
    # is what a type checker sees, so they are called through `Any`.
    equal: Any = expressions.EqualTo
    is_null: Any = expressions.IsNull
    And, Or = expressions.And, expressions.Or

    terms: list[BooleanExpression] = []
    for values in partitions:
        parts: list[BooleanExpression] = [
            is_null(name) if value is None else equal(name, _literal(value))
            for name, value in values.items()
        ]
        terms.append(functools.reduce(And, parts) if len(parts) > 1 else parts[0])
    if not terms:
        return expressions.AlwaysFalse()
    return functools.reduce(Or, terms) if len(terms) > 1 else terms[0]


def commit_files(
    load: Callable[[], Table],
    paths: Sequence[str],
    *,
    mode: str,
    partitions: Sequence[Mapping[str, Any]] = (),
) -> None:
    """Commit `paths` to the table `load` returns, as `mode` says, in one transaction."""
    from pyiceberg.expressions import AlwaysTrue

    if mode == "append" and not paths:
        return  # nothing to add: no commit, as an empty append adds no snapshot
    if mode == "dynamic" and not partitions:
        return  # nothing written, so no partition to replace

    def operation() -> None:
        table = load()  # fresh metadata on every attempt
        with table.transaction() as transaction:
            if mode == "overwrite":
                transaction.delete(delete_filter=AlwaysTrue())
            elif mode == "dynamic":
                transaction.delete(delete_filter=partition_filter(partitions))
            if paths:
                transaction.add_files(file_paths=list(paths))

    commit_with_retry(operation)
