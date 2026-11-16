"""Running a `StagePlan`: a protocol with two backends (decision 27).

`ThreadPoolScheduler` sends tasks to the slots of running workers over HTTP (Phase 1);
`ArgoScheduler` turns the plan into one Argo Workflow with a pod per task (Phase 6b).
Both consume the same `StagePlan` and return the same `TaskResult` manifests.
"""

from icefleet.scheduler.base import Scheduler
from icefleet.scheduler.threadpool import ThreadPoolScheduler

__all__ = ["Scheduler", "ThreadPoolScheduler"]
