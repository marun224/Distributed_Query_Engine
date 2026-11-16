"""Finding workers, launching `local[N]`, and talking to them over HTTP."""

from __future__ import annotations

from typing import TYPE_CHECKING

from icefleet.cluster.client import Cluster, WorkerClient, WorkerInfo, handshake
from icefleet.cluster.discovery import Master, parse_master, resolve_endpoints
from icefleet.cluster.launcher import LocalCluster

if TYPE_CHECKING:
    from icefleet.config import FleetSettings

__all__ = [
    "Cluster",
    "LocalCluster",
    "Master",
    "WorkerClient",
    "WorkerInfo",
    "connect",
    "handshake",
    "parse_master",
    "resolve_endpoints",
]


def connect(master: Master, fleet: FleetSettings) -> Cluster:
    """Start (`local[N]`) or find (static / k8s) the workers, and handshake with each."""
    timeout = fleet.task_timeout_seconds
    if master.kind == "local":
        launcher = LocalCluster(
            master.workers,
            slots=fleet.worker_slots,
            threads=fleet.local_threads,
            memory_limit=fleet.local_memory_limit,
            startup_timeout=fleet.worker_startup_timeout_seconds,
        )
        endpoints = launcher.start()
        try:
            workers = [handshake(WorkerClient(e, task_timeout=timeout)) for e in endpoints]
        except BaseException:
            launcher.stop()
            raise
        return Cluster(workers, description=master.text, launcher=launcher)
    endpoints = resolve_endpoints(master)
    workers = [handshake(WorkerClient(e, task_timeout=timeout)) for e in endpoints]
    return Cluster(workers, description=master.text)
