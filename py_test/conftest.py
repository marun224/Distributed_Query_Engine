"""Shared fixtures: a local Iceberg catalog, an icetl oracle, and `local[N]` clusters.

Everything is offline: the catalog is sqlite, the warehouse and the exchange are temp
directories, and the workers are subprocesses on 127.0.0.1. The `cluster` fixture is
parametrised over `local[2]` and `local[3]` and session-scoped, so every cluster test
runs on both fleets while each fleet starts only once.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from icetl.conf import CatalogSettings, EngineSettings, IcetlConf, IcetlSettings
from icetl.sql import Session

from icefleet.session import ClusterSession
from py_test.fixtures import build_all, local_catalog, warehouse_uri

if TYPE_CHECKING:
    from pyiceberg.catalog.sql import SqlCatalog


@pytest.fixture(scope="session")
def warehouse_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("warehouse")


@pytest.fixture(scope="session")
def catalog(warehouse_root: Path) -> SqlCatalog:
    built = local_catalog(warehouse_root)
    build_all(built)
    return built


@pytest.fixture(scope="session")
def local_settings(warehouse_root: Path) -> IcetlSettings:
    return IcetlSettings(
        catalog=CatalogSettings(
            name="test",
            type="sql",
            uri=f"sqlite:///{(warehouse_root / 'catalog.db').as_posix()}",
            warehouse=warehouse_uri(warehouse_root),
        ),
        default_namespace=("fx",),
    )


def with_spill(settings: IcetlSettings, root: Path) -> IcetlSettings:
    return replace(settings, engine=EngineSettings(temp_directory=str(root / "spill")))


@pytest.fixture
def oracle(local_settings: IcetlSettings, catalog: SqlCatalog, tmp_path: Path) -> Iterator[Session]:
    """Plain icetl over the same catalog: the answer every distributed result must equal."""
    session = Session(settings=with_spill(local_settings, tmp_path), catalog=catalog)
    yield session
    session.stop()


def cluster_conf(workers: int, exchange: Path, **extra: str) -> IcetlConf:
    return IcetlConf(
        {
            "icefleet.master": f"local[{workers}]",
            "icefleet.exchange.location": str(exchange),
            "icefleet.worker.slots": "2",
            "icefleet.local.threads": "2",
            "icefleet.fallback": "single-node",
            **extra,
        }
    )


@pytest.fixture(scope="session", params=[2, 3], ids=["local2", "local3"])
def cluster(
    request: pytest.FixtureRequest,
    local_settings: IcetlSettings,
    catalog: SqlCatalog,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[ClusterSession]:
    """A ClusterSession on `local[2]`, then `local[3]`. Its exchange is a known directory."""
    workers = int(request.param)
    root = tmp_path_factory.mktemp(f"cluster-{workers}")
    session = ClusterSession(
        settings=with_spill(local_settings, root),
        conf=cluster_conf(workers, root / "exchange"),
        catalog=catalog,
    )
    yield session
    session.stop()


@pytest.fixture
def fresh_cluster(
    local_settings: IcetlSettings, catalog: SqlCatalog, tmp_path: Path
) -> Iterator[ClusterSession]:
    """A `local[2]` session of the test's own, for tests that break or reconfigure it."""
    session = ClusterSession(
        settings=with_spill(local_settings, tmp_path),
        conf=cluster_conf(2, tmp_path / "exchange"),
        catalog=catalog,
    )
    yield session
    session.stop()
