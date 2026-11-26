"""Writes on the dev catalog, only in `icefleet_it` (guard.py): twin tables of January
2024's trips, one filled by the cluster and one by icetl, then the row-level statements
of `smoke_rowlevel.py` on each. After every step the twins agree day by day, and the
cluster's statement was one distributed snapshot. Both tables are purged afterwards.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any

import pytest
from icetl.sql import DataFrame, Session
from icetl.sql.writer import iceberg_ready

from icefleet.session import ClusterSession
from py_test.integration.conftest import scripts_module
from py_test.integration.guard import it_namespace, safe_drop, safe_identifier

pytestmark = pytest.mark.integration

rowlevel = scripts_module("smoke_rowlevel")


@pytest.fixture(scope="module")
def twins(
    it_cluster: ClusterSession, it_oracle: Session, it_catalog: Any
) -> Iterator[tuple[str, str]]:
    ours = safe_identifier(f"{it_namespace()}.it_trips_cluster")
    theirs = safe_identifier(f"{it_namespace()}.it_trips_icetl")
    for name in (ours, theirs):
        safe_drop(it_catalog, name)
    schema = iceberg_ready(it_cluster._empty_result(rowlevel.trips(it_cluster))).schema
    for name, session in ((ours, it_cluster), (theirs, it_oracle)):
        table = it_catalog.create_table(name, schema=schema)
        with table.update_spec() as spec:
            spec.add_identity("pickup_date")
        rowlevel.trips(session).write.insertInto(name)
    try:
        yield ours, theirs
    finally:
        for name in (ours, theirs):
            safe_drop(it_catalog, name)


def _read(oracle: Session, name: str) -> DataFrame:
    with contextlib.suppress(Exception):
        oracle._invalidate_source(oracle._resolver.parse(name))
    return oracle.table(name)


def test_the_cluster_writes_what_icetl_writes(
    twins: tuple[str, str], it_cluster: ClusterSession, it_oracle: Session
) -> None:
    ours, theirs = twins
    assert it_cluster._last_write is not None
    assert (
        rowlevel.same(
            rowlevel.summary(_read(it_oracle, ours)), rowlevel.summary(_read(it_oracle, theirs))
        )
        is None
    )


@pytest.mark.parametrize("step", range(len(rowlevel.STATEMENTS)))
def test_row_level_statement(
    step: int,
    twins: tuple[str, str],
    it_cluster: ClusterSession,
    it_oracle: Session,
    it_catalog: Any,
) -> None:
    ours, theirs = twins
    label, statement, for_icetl = rowlevel.STATEMENTS[step]
    before = len(it_catalog.load_table(ours).snapshots())
    it_cluster.sql(statement.format(t=ours))
    run = it_cluster._last_write
    it_oracle.sql((for_icetl or statement).format(t=theirs))
    made = it_catalog.load_table(ours).snapshots()[before:]
    assert run is not None and run.mode == "distributed", label
    assert len(made) == 1, f"{label}: {len(made)} snapshots"
    assert (
        rowlevel.same(
            rowlevel.summary(_read(it_oracle, ours)), rowlevel.summary(_read(it_oracle, theirs))
        )
        is None
    ), label
