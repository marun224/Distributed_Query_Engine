"""`icefleet.master` -> where the workers are (PLAN §3.5).

local[N]                          N worker subprocesses on 127.0.0.1 (local[*] = one per core)
static://host:port,host:port      workers that are already running
k8s://service.ns.svc:port         a headless Service: DNS returns one A record per
                                  *ready* pod, so a lookup is the discovery -- no
                                  Kubernetes client, no RBAC
"""

from __future__ import annotations

import os
import re
import socket
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from icefleet.errors import ClusterConfigurationError, ClusterError

__all__ = ["Master", "parse_master", "resolve_endpoints"]

_LOCAL = re.compile(r"^local(?:\[(\d+|\*)\])?$")
_HOST_PORT = re.compile(r"^\[?([^\]\s]+?)\]?:(\d{1,5})$")

Resolver = Callable[..., list[tuple[Any, ...]]]


@dataclass(frozen=True)
class Master:
    """A parsed `icefleet.master`."""

    kind: Literal["local", "static", "k8s"]
    text: str
    workers: int = 0
    endpoints: tuple[str, ...] = ()
    host: str = ""
    port: int = 0


def _host_port(text: str, master: str) -> tuple[str, int]:
    match = _HOST_PORT.match(text.strip())
    if not match or not 0 < int(match.group(2)) < 65536:
        raise ClusterConfigurationError(f"icefleet.master {master!r}: {text!r} is not host:port.")
    return match.group(1), int(match.group(2))


def parse_master(text: str) -> Master:
    """Parse `icefleet.master`, refusing anything it does not recognise."""
    master = text.strip()
    local = _LOCAL.match(master)
    if local:
        count = local.group(1)
        workers = (os.cpu_count() or 1) if count == "*" else int(count or 1)
        if workers < 1:
            raise ClusterConfigurationError(f"icefleet.master {master!r}: needs at least 1 worker.")
        return Master(kind="local", text=master, workers=workers)
    if master.startswith("static://"):
        parts = [p for p in master[len("static://") :].split(",") if p.strip()]
        if not parts:
            raise ClusterConfigurationError(f"icefleet.master {master!r}: names no workers.")
        endpoints = tuple(f"{h}:{p}" for h, p in (_host_port(part, master) for part in parts))
        return Master(kind="static", text=master, workers=len(endpoints), endpoints=endpoints)
    if master.startswith("k8s://"):
        host, port = _host_port(master[len("k8s://") :], master)
        return Master(kind="k8s", text=master, host=host, port=port)
    raise ClusterConfigurationError(
        f"icefleet.master {master!r}: expected local[N], static://host:port,... or "
        f"k8s://service:port."
    )


def resolve_endpoints(master: Master, *, resolver: Resolver = socket.getaddrinfo) -> list[str]:
    """The `host:port` of every worker a static or k8s master names, right now.

    For `k8s://` this is re-run between stages (PLAN §3.5), so a pod that appeared or
    vanished is noticed at the next stage boundary. `local[N]` has no endpoints until
    its launcher starts them.
    """
    if master.kind == "static":
        return list(master.endpoints)
    if master.kind == "k8s":
        try:
            records = resolver(master.host, master.port, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise ClusterError(f"{master.text}: DNS lookup failed: {exc}") from exc
        addresses = sorted({str(record[4][0]) for record in records})
        if not addresses:
            raise ClusterError(f"{master.text}: DNS returned no ready worker pods.")
        return [f"[{a}]:{master.port}" if ":" in a else f"{a}:{master.port}" for a in addresses]
    raise ClusterError(f"{master.text}: local workers are found by starting them, not by lookup.")
