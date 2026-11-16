"""`ThreadPoolScheduler`: tasks to the slots of running workers, over HTTP.

One coordinator thread per busy slot, each holding a blocking `POST /tasks`. Slots are
handed out from a queue interleaved across workers, so a stage spreads evenly. The first
failure cancels the rest: queued tasks never start, running ones are interrupted on their
workers through `POST /cancel`, and the original error is what the caller sees.
"""

from __future__ import annotations

import logging
import queue
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from icefleet.cluster.client import Cluster, WorkerClient
    from icefleet.exchange.base import Exchange
    from icefleet.plan.stages import StagePlan
    from icefleet.plan.tasks import TaskResult, TaskSpec

__all__ = ["ThreadPoolScheduler"]

logger = logging.getLogger(__name__)


class ThreadPoolScheduler:
    """The long-lived-worker backend (models A and B of PLAN §3.5)."""

    def __init__(self, cluster: Cluster, *, target_file_bytes: int) -> None:
        self._cluster = cluster
        self._target_file_bytes = target_file_bytes

    def describe(self) -> str:
        return f"thread pool over {self._cluster.summary()}"

    def run(self, plan: StagePlan, *, query_id: str, exchange: Exchange) -> list[TaskResult]:
        """Every stage in order, each fully before the next (a blocking shuffle): a stage's
        tasks are built from the manifests of the stages before it, never from a glob."""
        results: list[TaskResult] = []
        for stage in plan.stages:
            specs = stage.tasks(
                query_id, exchange, results, target_file_bytes=self._target_file_bytes
            )
            results.extend(self._run_tasks(specs, query_id=query_id))
        return results

    def _run_tasks(self, specs: list[TaskSpec], *, query_id: str) -> list[TaskResult]:
        if not specs:
            return []
        slots: queue.Queue[WorkerClient] = queue.Queue()
        for client in self._cluster.slot_clients():
            slots.put(client)

        def run_one(spec: TaskSpec) -> TaskResult:
            client = slots.get()
            try:
                return client.run_task(spec)
            finally:
                slots.put(client)

        failure: BaseException | None = None
        results: list[TaskResult] = []
        width = min(self._cluster.total_slots, len(specs))
        with ThreadPoolExecutor(max_workers=width, thread_name_prefix="icefleet-task") as pool:
            futures: list[Future[TaskResult]] = [pool.submit(run_one, spec) for spec in specs]
            for future in as_completed(futures):
                if future.cancelled():
                    continue
                error = future.exception()
                if error is None:
                    results.append(future.result())
                elif failure is None:
                    failure = error
                    logger.warning("query %s failed: %s -- cancelling the rest", query_id, error)
                    for other in futures:
                        other.cancel()
                    for worker in self._cluster.workers:
                        worker.client.cancel(query_id)
        if failure is not None:
            raise failure
        return sorted(results, key=lambda r: r.task_index)
