"""Comparing a distributed answer with icetl's (PLAN §9: the oracle is icetl).

Exact equality of schema and of rows. Row order is not compared -- without an ORDER BY
neither engine promises one -- so both sides are sorted by a total key first. Phase 1
moves rows without arithmetic on them, so no floating-point tolerance is needed yet.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

__all__ = ["assert_same_rows", "sorted_rows"]


def _key(row: dict[str, Any]) -> str:
    return repr(sorted(row.items(), key=lambda kv: kv[0]))


def sorted_rows(table: pa.Table) -> list[dict[str, Any]]:
    return sorted(table.to_pylist(), key=_key)


def assert_same_rows(actual: pa.Table, expected: pa.Table) -> None:
    assert actual.schema.equals(expected.schema), (
        f"schema differs:\n  distributed: {actual.schema}\n  icetl:       {expected.schema}"
    )
    assert actual.num_rows == expected.num_rows, (
        f"{actual.num_rows} rows distributed, {expected.num_rows} from icetl"
    )
    assert sorted_rows(actual) == sorted_rows(expected)
