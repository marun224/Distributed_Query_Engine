"""Reads on the dev catalog: every proof-of-life query, then the 41M-row checks (`slow`).

Each one distributed through the exchange in MinIO, equal to icetl's own answer, and its
exchange prefix gone afterwards. The queries are the scripts' own (`smoke_cluster.py`,
`fleet_check.py`), so a green tier means green scripts and the other way round.
"""

from __future__ import annotations

import pytest
from icetl.sql import Session

from icefleet.exchange.object_store import ObjectStoreExchange
from icefleet.session import ClusterSession
from py_test.integration.conftest import scripts_module

pytestmark = pytest.mark.integration

smoke = scripts_module("smoke_cluster")
fleet = scripts_module("fleet_check")


@pytest.mark.parametrize("name", list(smoke.QUERIES))
def test_proof_of_life_query(
    name: str,
    it_cluster: ClusterSession,
    it_oracle: Session,
    it_exchange: ObjectStoreExchange,
) -> None:
    assert smoke.check(name, *smoke.QUERIES[name], it_cluster, it_oracle, it_exchange)


@pytest.mark.slow
@pytest.mark.parametrize("name", list(fleet.QUERIES))
def test_41m_rows(
    name: str,
    it_cluster: ClusterSession,
    it_oracle: Session,
    it_exchange: ObjectStoreExchange,
) -> None:
    query, stages, expect = fleet.QUERIES[name]
    assert expect in query(it_cluster)._explain_text(verbose=False)
    actual = query(it_cluster).toArrow()
    run = it_cluster._last_run
    assert run is not None and run.mode == "distributed" and run.stages >= stages
    assert smoke.same_rows(actual, query(it_oracle).toArrow()) is None
    assert run.query_id is not None
    assert not it_exchange.exists(it_exchange.query_prefix(run.query_id))
