"""Hash partitioning (PLAN §4.1): the partition expression, the type a key is hashed as,
and what a pipeline's rows are known to be partitioned by.

Two rows meet in one partition only when their keys hash alike, and DuckDB's `hash()`
is a hash of the *physical* value: `hash((-1)::INTEGER) != hash((-1)::BIGINT)` on
DuckDB 1.5.6 (`py_test/unit/test_partitioning.py`). So every key is cast to one
canonical type before it is hashed:

    one type, as an aggregate key or on both sides of a join
        every integer type except UHUGEINT  -> HUGEINT
        FLOAT                               -> DOUBLE
        anything else                       -> itself
    two types, the sides of `l = r` in a join (kickoff answer 5)
        integers of different widths        -> HUGEINT
        any FLOAT / DOUBLE among numbers    -> DOUBLE
        integers and DECIMALs               -> DECIMAL(38, the larger scale), when it fits
        anything else                       -> not hashable alike: the join runs
                                               single-node (or broadcasts)

The cast is exact or, for DOUBLE, the very conversion DuckDB's own comparison makes, so
keys that compare equal are cast to equal values and hash alike. Every worker runs the
same DuckDB (the handshake enforces it), because `hash()` is not promised stable
across versions.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from sqlglot import exp

from icefleet.plan.types import cast_to

__all__ = ["Hash", "hash_type", "join_hash_type", "partition_expression"]

_SIGNED_DIGITS = {"TINYINT": 3, "SMALLINT": 5, "INTEGER": 10, "BIGINT": 19, "HUGEINT": 39}
_UNSIGNED_DIGITS = {"UTINYINT": 3, "USMALLINT": 5, "UINTEGER": 10, "UBIGINT": 20}
_INTEGER_DIGITS = {**_SIGNED_DIGITS, **_UNSIGNED_DIGITS}
_FLOATS = ("FLOAT", "DOUBLE")
_DECIMAL = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$")

#: Keys of these types are hashed as the canonical type, so equal values meet in one
#: partition whatever their width. UHUGEINT does not fit HUGEINT and is hashed as it is.
_HASH_AS = {**dict.fromkeys(_INTEGER_DIGITS, "HUGEINT"), "FLOAT": "DOUBLE"}


def _normal(duckdb_type: str) -> str:
    return re.sub(r"\s+", " ", duckdb_type.strip()).upper()


def hash_type(duckdb_type: str) -> str:
    """The type one key of `duckdb_type` is hashed as."""
    return _HASH_AS.get(_normal(duckdb_type), duckdb_type)


def join_hash_type(left: str, right: str) -> str | None:
    """The type both sides of `left = right` are hashed as, or None when there is none."""
    a, b = _normal(left), _normal(right)
    if a == b:
        return hash_type(left)
    numbers = []
    for kind in (a, b):
        decimal = _DECIMAL.match(kind)
        if kind in _INTEGER_DIGITS:
            numbers.append(("integer", _INTEGER_DIGITS[kind], 0))
        elif kind in _FLOATS:
            numbers.append(("float", 0, 0))
        elif decimal is not None:
            width, scale = int(decimal.group(1)), int(decimal.group(2))
            numbers.append(("decimal", width - scale, scale))
        else:
            return None  # a string, a date, a struct ...: compared through a cast we do not mirror
    families = {family for family, _, _ in numbers}
    if families == {"integer"}:
        return "HUGEINT" if "UHUGEINT" not in (a, b) else None
    if "float" in families:
        return "DOUBLE"
    digits = max(d for _, d, _ in numbers)
    scale = max(s for _, _, s in numbers)
    if digits + scale > 38:
        return None  # DECIMAL(38, s) could not hold every value of both sides
    return f"DECIMAL(38,{scale})"


@dataclass(frozen=True)
class Hash:
    """Rows partitioned by `hash(CAST(c AS t), ...) % partitions` over output columns.

    `columns` name the columns of the pipeline that carries the partitioning; `types`
    are what each was cast to before hashing. Two pipelines are co-partitioned when
    their `Hash`es pair up column for column with the same types and the same count.
    """

    columns: tuple[str, ...]
    types: tuple[str, ...]
    partitions: int

    def renamed(self, names: dict[str, str]) -> Hash | None:
        """The same partitioning after a projection renamed columns; None if one was dropped."""
        if not all(column in names for column in self.columns):
            return None
        return Hash(tuple(names[c] for c in self.columns), self.types, self.partitions)


def partition_expression(
    keys: Sequence[exp.Expression],
    partitions: int,
    types: Sequence[str] | None = None,
    *,
    hash_as: Sequence[str] | None = None,
) -> exp.Expression:
    """`hash(k0, k1, ...) % n`, each key cast to the type it is hashed as first.

    `types` are the keys' DuckDB types (an aggregate's keys, cast to `hash_type`);
    `hash_as` names the types outright (a join's keys, from `join_hash_type`). `-0.0`
    and `0.0` hash alike and NULL hashes to one value, so equal keys always meet.
    """
    hashed_keys = []
    for index, key in enumerate(keys):
        if hash_as is not None:
            canonical: str | None = hash_as[index]
        elif types is not None:
            canonical = _HASH_AS.get(_normal(types[index]))
        else:
            canonical = None
        hashed_keys.append(cast_to(key.copy(), canonical) if canonical else key.copy())
    hashed = exp.Anonymous(this="hash", expressions=hashed_keys)
    return exp.Mod(this=hashed, expression=exp.Literal.number(partitions))
