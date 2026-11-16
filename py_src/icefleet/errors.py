"""Errors icefleet raises, each under the icetl error a caller already catches.

A query that fails on the cluster is still a query that failed during execution, so
`ClusterError` is a `QueryExecutionException`: code written against icetl keeps working.
"""

from __future__ import annotations

from dataclasses import dataclass

from icetl.errors import ConfigurationError, QueryExecutionException

__all__ = ["ClusterConfigurationError", "ClusterError", "Uncuttable", "WorkerError"]


class ClusterError(QueryExecutionException):
    """The cluster could not run a query: no workers, a refused handshake, a lost worker."""


class WorkerError(ClusterError):
    """A task failed on a worker. Says which worker and which task."""

    def __init__(self, message: str, *, worker: str | None = None, task: str | None = None) -> None:
        self.detail = message
        self.worker = worker
        self.task = task
        where = ", ".join(
            part
            for part in (f"worker {worker}" if worker else "", f"task {task}" if task else "")
            if part
        )
        super().__init__(f"{message} [{where}]" if where else message)


class ClusterConfigurationError(ConfigurationError):
    """`icefleet.*` settings are missing or contradictory -- a setup fault, not a query one."""


@dataclass(frozen=True)
class Uncuttable:
    """A plan the stage planner cannot distribute, and why.

    Not an exception: the plan runs single-node through icetl (P3), and `explain()`
    prints the reason. Only `icefleet.fallback=error` turns it into one.
    """

    reason: str
