"""The `Scheduler` protocol both backends implement."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from icefleet.exchange.base import Exchange
    from icefleet.plan.stages import StagePlan
    from icefleet.plan.tasks import TaskResult

__all__ = ["Scheduler"]


class Scheduler(Protocol):
    """Runs a plan's stages in order, each fully before its dependents (blocking shuffle).

    Returns every stage's task results, stage by stage, each stage's ordered by task
    index. A failure anywhere fails the whole query (decision 13); task retry is Phase 7.
    """

    def run(self, plan: StagePlan, *, query_id: str, exchange: Exchange) -> list[TaskResult]: ...

    def describe(self) -> str: ...
