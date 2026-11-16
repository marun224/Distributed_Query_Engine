from __future__ import annotations

import os
import socket
from typing import Any

import pytest

from icefleet.cluster.discovery import parse_master, resolve_endpoints
from icefleet.errors import ClusterConfigurationError, ClusterError


def test_local() -> None:
    assert parse_master("local[3]").workers == 3
    assert parse_master("local").workers == 1
    assert parse_master("local[*]").workers == (os.cpu_count() or 1)
    assert parse_master(" local[2] ").kind == "local"


def test_static() -> None:
    master = parse_master("static://w1:8815, w2:8816")
    assert master.kind == "static"
    assert resolve_endpoints(master) == ["w1:8815", "w2:8816"]


def test_k8s_resolves_every_ready_pod_once() -> None:
    def resolver(host: str, port: int, **_: Any) -> list[tuple[Any, ...]]:
        assert (host, port) == ("icefleet-workers.etl.svc", 8815)
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.7", port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.5", port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.7", port)),
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("fd00::1", port, 0, 0)),
        ]

    master = parse_master("k8s://icefleet-workers.etl.svc:8815")
    assert resolve_endpoints(master, resolver=resolver) == [
        "10.0.0.5:8815",
        "10.0.0.7:8815",
        "[fd00::1]:8815",
    ]


def test_k8s_with_no_ready_pods_is_an_error() -> None:
    master = parse_master("k8s://svc:8815")
    with pytest.raises(ClusterError, match="no ready worker pods"):
        resolve_endpoints(master, resolver=lambda *a, **k: [])


@pytest.mark.parametrize(
    "text", ["", "yarn", "local[0]", "local[x]", "static://", "static://w1", "k8s://svc:99999"]
)
def test_unrecognised_masters_are_refused(text: str) -> None:
    with pytest.raises(ClusterConfigurationError):
        parse_master(text)
