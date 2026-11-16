"""Comparing a distributed answer with icetl's (PLAN §9: the oracle is icetl).

Exact equality of schema and of rows, except that floating-point values compare with a
relative tolerance of 1e-9 (PLAN §4.7, `docs/divergence.md`): a distributed sum adds the
same numbers in a different order, which changes the last digits of a double. Row order
is not compared -- without an ORDER BY neither engine promises one -- so both sides are
sorted by a total key first; `assert_same_ordered_rows` is for when order is the answer.
"""

from __future__ import annotations

import math
from typing import Any

import pyarrow as pa

__all__ = ["assert_same_ordered_rows", "assert_same_rows", "sorted_rows"]

REL_TOL = 1e-9


def _rounded(value: Any) -> Any:
    """`value` with floats cut to 9 significant digits: a sort key two runs agree on."""
    if isinstance(value, float):
        return value if math.isnan(value) or math.isinf(value) else float(f"{value:.9g}")
    if isinstance(value, list):
        return [_rounded(v) for v in value]
    if isinstance(value, dict):
        return {k: _rounded(v) for k, v in value.items()}
    return value


def _key(row: dict[str, Any]) -> str:
    return repr(sorted(((k, _rounded(v)) for k, v in row.items()), key=lambda kv: kv[0]))


def sorted_rows(table: pa.Table) -> list[dict[str, Any]]:
    return sorted(table.to_pylist(), key=_key)


def _close(actual: Any, expected: Any) -> bool:
    if isinstance(actual, float) and isinstance(expected, float):
        if math.isnan(actual) or math.isnan(expected):
            return math.isnan(actual) and math.isnan(expected)
        return math.isclose(actual, expected, rel_tol=REL_TOL, abs_tol=1e-12)
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(map(_close, actual, expected))
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _close(actual[k], expected[k]) for k in actual
        )
    return bool(actual == expected)


def _same_schema_and_count(actual: pa.Table, expected: pa.Table) -> None:
    assert actual.schema.equals(expected.schema), (
        f"schema differs:\n  distributed: {actual.schema}\n  icetl:       {expected.schema}"
    )
    assert actual.num_rows == expected.num_rows, (
        f"{actual.num_rows} rows distributed, {expected.num_rows} from icetl"
    )


def _same_rows(actual: list[dict[str, Any]], expected: list[dict[str, Any]]) -> None:
    for index, (a, e) in enumerate(zip(actual, expected, strict=True)):
        assert _close(a, e), f"row {index} differs:\n  distributed: {a}\n  icetl:       {e}"


def assert_same_rows(actual: pa.Table, expected: pa.Table) -> None:
    _same_schema_and_count(actual, expected)
    _same_rows(sorted_rows(actual), sorted_rows(expected))


def assert_same_ordered_rows(actual: pa.Table, expected: pa.Table) -> None:
    """For ORDER BY queries: the same rows in the same order (keys without ties)."""
    _same_schema_and_count(actual, expected)
    _same_rows(actual.to_pylist(), expected.to_pylist())
