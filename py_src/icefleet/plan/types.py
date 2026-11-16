"""DuckDB types through a parquet exchange (PLAN §4.4, Phase 1 "As built").

Some DuckDB types do not come back from parquet as the same Arrow type, measured on
duckdb 1.5.6 (commands.md, 2026-11-15): HUGEINT/UHUGEINT become DOUBLE, second- and
millisecond timestamps become microseconds, fixed-size arrays become lists, and ENUM,
BIT, VARINT and UNION change shape. `py_test/unit/test_result_types.py` pins the list.

Phase 2 (kickoff answer 5) moves the ones with a lossless stand-in through the exchange
as that stand-in and casts them back on the other side:

    HUGEINT, UHUGEINT          -> DECIMAL(38,0)   a value past 38 digits fails the cast
                                                  loudly instead of losing digits
    TIMESTAMP_S, TIMESTAMP_MS  -> TIMESTAMP       exact: microseconds hold both
    T[n] (fixed-size array)    -> T[]             exact

Every other unsafe type has no stand-in, and a plan whose exchange would carry one runs
single-node, as in Phase 1.
"""

from __future__ import annotations

import re

from sqlglot import exp

__all__ = ["cast_to", "data_type", "parquet_safe_type", "parquet_unsafe_type"]

#: A struct field *named* like one of these trips the check too -- a false positive that
#: costs speed, never an answer.
_PARQUET_UNSAFE = re.compile(
    r"\b(HUGEINT|UHUGEINT|TIMESTAMP_S|TIMESTAMP_MS|BIT|BITSTRING|VARINT|BIGNUM)\b"
    r"|\bENUM\s*\(|\bUNION\s*\(|\[\d+\]",
    re.IGNORECASE,
)
_FIXED_SIZE = re.compile(r"\[\d+\]")
_STAND_INS = {
    "HUGEINT": "DECIMAL(38,0)",
    "UHUGEINT": "DECIMAL(38,0)",
    "TIMESTAMP_S": "TIMESTAMP",
    "TIMESTAMP_MS": "TIMESTAMP",
}


def parquet_unsafe_type(duckdb_type: str) -> bool:
    """True when a column of this DuckDB type would change type through a parquet file."""
    return _PARQUET_UNSAFE.search(duckdb_type) is not None


def parquet_safe_type(duckdb_type: str) -> str | None:
    """The type to carry `duckdb_type` through parquet as, or None when there is none.

    A safe type is its own stand-in. Casting a value to the stand-in and back returns
    the value unchanged, and orders the same way while it is the stand-in.
    """
    if not parquet_unsafe_type(duckdb_type):
        return duckdb_type
    stand_in = _STAND_INS.get(duckdb_type.strip().upper())
    if stand_in is not None:
        return stand_in
    widened = _FIXED_SIZE.sub("[]", duckdb_type)
    if widened != duckdb_type and not parquet_unsafe_type(widened):
        return widened
    return None


def data_type(duckdb_type: str) -> exp.DataType:
    """A DuckDB type as DESCRIBE spells it, as a sqlglot node that renders back to it."""
    return exp.DataType.build(duckdb_type, dialect="duckdb", udt=True)


def cast_to(expression: exp.Expression, duckdb_type: str) -> exp.Cast:
    return exp.Cast(this=expression, to=data_type(duckdb_type))
