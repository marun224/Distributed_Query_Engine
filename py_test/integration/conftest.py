"""Fixtures for the integration tier (PLAN §9, decision 22): the dev REST catalog, MinIO,
the real 41M-row table -- the opposite of the offline suite's sqlite and temp dirs.

    .\\scripts\\dev.ps1 uv run pytest -m "integration and not slow"   bounded, ~minutes
    .\\scripts\\dev.ps1 uv run pytest -m integration                  plus the 41M-row checks
    .\\scripts\\k8s.ps1 run integration.py                            the same, on an Argo fleet

Configuration comes from the environment / `.env` (icetl's resolver), as the smoke
scripts read it. The fleet is `ICEFLEET_IT_MASTER`, else `ICEFLEET_MASTER` when it names
running workers (static:// or k8s://, which is how the Argo run points here), else
`local[3]`. The exchange is a fresh prefix under `s3://icefleet-shuffle/it-*`. The
protected tables are witnessed before and after the session (guard.py).
"""

from __future__ import annotations

import importlib
import os
import sys
import urllib.request
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from icetl.conf import IcetlConf, IcetlSettings, resolve_settings
from icetl.sql import Session

from icefleet.exchange.object_store import ObjectStoreExchange
from icefleet.session import ClusterSession
from py_test.integration.guard import Witness, ensure_namespace

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def scripts_module(name: str) -> Any:
    """A module from `scripts/` (the smoke and fleet checks hold the shared queries).

    Inside the test image the scripts sit beside the tests, under `ICEFLEET_SCRIPTS`."""
    location = os.environ.get("ICEFLEET_SCRIPTS", str(SCRIPTS))
    if location not in sys.path:
        sys.path.insert(0, location)
    return importlib.import_module(name)


def _master() -> str:
    explicit = os.environ.get("ICEFLEET_IT_MASTER")
    if explicit:
        return explicit
    ambient = os.environ.get("ICEFLEET_MASTER", "")
    return ambient if ambient.startswith(("static://", "k8s://")) else "local[3]"


@pytest.fixture(scope="session")
def it_settings() -> IcetlSettings:
    settings = resolve_settings(IcetlConf())
    uri = settings.catalog.uri or ""
    try:
        urllib.request.urlopen(f"{uri.rstrip('/')}/v1/config", timeout=5).read()
    except Exception as exc:
        pytest.skip(f"the dev catalog at {uri} is not reachable: {exc}")
    return settings


@pytest.fixture(scope="session")
def it_exchange(it_settings: IcetlSettings) -> ObjectStoreExchange:
    exchange = ObjectStoreExchange(
        f"s3://icefleet-shuffle/it-{uuid.uuid4().hex[:8]}", it_settings.s3
    )
    exchange.ensure_bucket()
    return exchange


@pytest.fixture(scope="session")
def it_oracle(it_settings: IcetlSettings) -> Iterator[Session]:
    session = Session(settings=it_settings)
    yield session
    session.stop()


@pytest.fixture(scope="session", autouse=True)
def witness(it_oracle: Session) -> Iterator[None]:
    catalog = it_oracle._registry.get(None)
    watched = Witness.capture(catalog)
    yield
    watched.verify()


@pytest.fixture(scope="session")
def it_cluster(
    it_settings: IcetlSettings, it_exchange: ObjectStoreExchange
) -> Iterator[ClusterSession]:
    expected = int(os.environ.get("ICEFLEET_EXPECT_WORKERS", "0") or 0)
    if expected:  # an Argo fleet: wait until DNS lists every daemon worker
        scripts_module("fleet_check").wait_for_workers(_master(), expected)
    session = ClusterSession(
        settings=it_settings,
        conf=IcetlConf(
            {"icefleet.master": _master(), "icefleet.exchange.location": it_exchange.location}
        ),
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def it_catalog(it_oracle: Session) -> Any:
    catalog = it_oracle._registry.get(None)
    ensure_namespace(catalog)
    return catalog
