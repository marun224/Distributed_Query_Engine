"""Range partitioning for a distributed `ORDER BY` (PLAN decision 17; Phase 4, kickoff
answers 2 and 3).

A top-level `ORDER BY` without a `LIMIT` whose input may be larger than
`icefleet.sort.coordinatorBytes` is sorted by the stages, not the coordinator:

    stage a   the query, written once, plus a reservoir sample of the sort keys per task
              (`icefleet.sort.sampleRows`)
              -- the coordinator reads the samples, sorts them, and picks n - 1 boundaries
    stage b   a task per task of stage a: each row goes to the range partition its keys
              fall in -- a CASE over "is this row before boundary i", filled in at run time
    stage c   a task per partition: ORDER BY within it
              -- the coordinator reads the partitions in order and sorts nothing

Correctness does not hang on the sample. The partition of a row is a monotone function
of its keys in the query's own order (ASC / DESC, NULLS FIRST / LAST), so every row of
partition p sorts at or before every row of partition p + 1, and equal keys always share
a partition, whatever the boundaries are; a good sample only makes the partitions even.
A boundary is carried as `CAST('<text>' AS <type>)`, DuckDB's own text form of the value,
which round-trips scalar types; a key of a nested type keeps the coordinator sort.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from sqlglot import exp

__all__ = [
    "RANGE_TOKEN",
    "RangeSpec",
    "SortKey",
    "boundaries",
    "range_expression",
    "sample_query",
    "sort_keys",
    "sortable_by_range",
]

#: Stands in a range stage's SQL for the partition expression until the boundaries exist.
RANGE_TOKEN = "__ICEFLEET_RANGE_PARTITION__"
SAMPLE_PARAMETER = "icefleet_sample"

_NESTED = re.compile(r"\[|\bSTRUCT\b|\bMAP\b|\bUNION\b|\bLIST\b|\bJSON\b", re.IGNORECASE)


def _quote(name: str) -> str:
    return exp.to_identifier(name, quoted=True).sql(dialect="duckdb")


@dataclass(frozen=True)
class SortKey:
    """One ORDER BY item over a result column, as written to parquet."""

    column: str
    kind: str  # the column's type in the files (a stand-in type where there is one)
    desc: bool
    nulls_first: bool

    def order_sql(self) -> str:
        direction = "DESC" if self.desc else "ASC"
        nulls = "FIRST" if self.nulls_first else "LAST"
        return f"{_quote(self.column)} {direction} NULLS {nulls}"


@dataclass(frozen=True)
class RangeSpec:
    """What the coordinator needs to fill in a range stage: its keys, partition count,
    and the stages whose tasks wrote the samples."""

    keys: tuple[SortKey, ...]
    partitions: int
    sample_stages: tuple[int, ...]

    def order_sql(self) -> str:
        return ", ".join(key.order_sql() for key in self.keys)


def sort_keys(order: Sequence[exp.Ordered], written: Mapping[str, str]) -> tuple[SortKey, ...]:
    """The lifted ORDER BY (items over result column names) as sort keys.

    DuckDB orders NULLs last unless told otherwise, in both directions; sqlglot leaves
    `nulls_first` unset for that default, so unset means last here too.
    """
    keys = []
    for item in order:
        column = item.this
        assert isinstance(column, exp.Column)
        keys.append(
            SortKey(
                column=column.name,
                kind=written[column.name],
                desc=bool(item.args.get("desc")),
                nulls_first=bool(item.args.get("nulls_first")),
            )
        )
    return tuple(keys)


def sortable_by_range(keys: Sequence[SortKey]) -> bool:
    """Every key is a scalar type whose text form DuckDB casts back exactly."""
    return bool(keys) and not any(_NESTED.search(key.kind) for key in keys)


def sample_query(spec: RangeSpec, files: Sequence[str]) -> tuple[str, dict[str, list[str]]]:
    """The coordinator's read of the samples: every sampled key as text, in sort order."""
    columns = ", ".join(
        f"CAST({_quote(key.column)} AS VARCHAR) AS {_quote(f'k{i}')}"
        for i, key in enumerate(spec.keys)
    )
    sql = (
        f"SELECT {columns} FROM read_parquet(${SAMPLE_PARAMETER}, union_by_name => false, "
        f"hive_partitioning => false) ORDER BY {spec.order_sql()}"
    )
    return sql, {SAMPLE_PARAMETER: list(files)}


def boundaries(
    rows: Sequence[tuple[str | None, ...]], partitions: int
) -> list[tuple[str | None, ...]]:
    """n - 1 evenly spaced boundaries from the sorted sample, repeats dropped."""
    if not rows or partitions <= 1:
        return []
    picked: list[tuple[str | None, ...]] = []
    for index in range(1, partitions):
        row = tuple(rows[(index * len(rows)) // partitions])
        if not picked or picked[-1] != row:
            picked.append(row)
    return picked


def _literal(value: str | None, kind: str) -> str | None:
    if value is None:
        return None
    return "CAST('" + value.replace("'", "''") + f"' AS {kind})"


def _before(keys: Sequence[SortKey], bound: Sequence[str | None]) -> str:
    """SQL that is true when a row sorts strictly before `bound` (lexicographically)."""
    expression = "FALSE"
    for key, value in reversed(list(zip(keys, bound, strict=True))):
        column = _quote(key.column)
        literal = _literal(value, key.kind)
        if literal is None:
            before = "FALSE" if key.nulls_first else f"({column} IS NOT NULL)"
            equal = f"({column} IS NULL)"
        else:
            op = ">" if key.desc else "<"
            before = (
                f"({column} IS NULL OR {column} {op} {literal})"
                if key.nulls_first
                else f"({column} IS NOT NULL AND {column} {op} {literal})"
            )
            equal = f"({column} IS NOT NULL AND {column} = {literal})"
        expression = f"({before} OR ({equal} AND {expression}))"
    return expression


def range_expression(spec: RangeSpec, bounds: Sequence[Sequence[str | None]]) -> str:
    """The partition of a row: how many boundaries it does not sort before."""
    if not bounds:
        return "0"
    cases = " ".join(
        f"WHEN {_before(spec.keys, bound)} THEN {index}" for index, bound in enumerate(bounds)
    )
    return f"CASE {cases} ELSE {len(bounds)} END"
