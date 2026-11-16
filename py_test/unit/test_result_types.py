"""Pins the claim behind `parquet_unsafe_type`: which DuckDB types survive a result file.

Measured, not assumed (P6). If a DuckDB upgrade changes how a type round-trips through
parquet, this fails and the list in `icefleet.plan.stages` must be revisited -- in either
direction: a type that starts surviving can be distributed, one that stops must not be.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from icefleet.plan.stages import parquet_unsafe_type

VALUES = {
    "BOOLEAN": "true",
    "TINYINT": "1",
    "SMALLINT": "1",
    "INTEGER": "1",
    "BIGINT": "1",
    "UTINYINT": "1",
    "USMALLINT": "1",
    "UINTEGER": "1",
    "UBIGINT": "1",
    "FLOAT": "1.5",
    "DOUBLE": "1.5",
    "DECIMAL(10,2)": "1.25",
    "DECIMAL(38,0)": "1",
    "VARCHAR": "'a'",
    "BLOB": "'\\x01'::BLOB",
    "DATE": "DATE '2026-01-01'",
    "TIME": "TIME '01:02:03'",
    "TIMESTAMP": "TIMESTAMP '2026-01-01 01:02:03'",
    "TIMESTAMP_NS": "TIMESTAMP '2026-01-01'",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ '2026-01-01 01:02:03+00'",
    "TIME WITH TIME ZONE": "TIMETZ '01:02:03+00'",
    "INTERVAL": "INTERVAL 3 DAY",
    "UUID": "'6f1c0a2e-0b4e-4f7e-9d53-3a7c4b2f1e00'",
    "INTEGER[]": "[1, 2]",
    "STRUCT(a INTEGER, b VARCHAR)": "{'a': 1, 'b': 'x'}",
    "MAP(VARCHAR, INTEGER)": "MAP {'k': 1}",
    # The ones the guard exists for:
    "HUGEINT": "1",
    "UHUGEINT": "1",
    "TIMESTAMP_S": "TIMESTAMP '2026-01-01'",
    "TIMESTAMP_MS": "TIMESTAMP '2026-01-01'",
    "INTEGER[2]": "[1, 2]",
    "ENUM('x', 'y')": "'x'",
    "BIT": "'101'",
    "VARINT": "1",
    "UNION(i INTEGER, s VARCHAR)": "1::INTEGER",
}


@pytest.mark.parametrize("duckdb_type", list(VALUES))
def test_guard_matches_what_parquet_actually_does(duckdb_type: str, tmp_path: Path) -> None:
    con = duckdb.connect()
    path = (tmp_path / "t.parquet").as_posix()
    query = f"SELECT ({VALUES[duckdb_type]})::{duckdb_type} AS c"
    described = con.execute(f"DESCRIBE {query}").fetchall()[0][1]
    con.execute(f"COPY ({query}) TO '{path}' (FORMAT parquet)")
    before = con.execute(query).to_arrow_table().schema.field(0).type
    after = (
        con.execute(f"SELECT * FROM read_parquet('{path}')").to_arrow_table().schema.field(0).type
    )
    survives = before == after
    assert parquet_unsafe_type(described) == (not survives), (
        f"{duckdb_type} (DESCRIBE says {described}): arrow {before} -> {after}"
    )
