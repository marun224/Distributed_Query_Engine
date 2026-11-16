"""The `== Distributed Plan ==` section `explain()` prints above icetl's own (PLAN §4.9)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from icetl.exec.scan_planner import format_bytes

from icefleet.errors import Uncuttable

if TYPE_CHECKING:
    from icefleet.plan.stages import StagePlan

__all__ = ["describe_plan"]

HEADER = "== Distributed Plan =="


def describe_plan(
    staged: StagePlan | Uncuttable | None,
    *,
    cluster: str,
    exchange: str,
    note: str | None = None,
) -> str:
    """Render the stage DAG, or the reason the plan runs single-node.

    `staged` is None when icetl answers without running a query at all (the manifest-only
    `count(*)`), and `note` then says so.
    """
    lines = [HEADER]
    if staged is None:
        lines.append(f"mode: single-node -- {note or 'answered without a scan'}")
    elif isinstance(staged, Uncuttable):
        lines.append(f"mode: single-node -- {staged.reason}")
    else:
        lines.append(f"mode: distributed · {cluster}")
        for stage in staged.stages:
            scan = stage.scan
            lines.append(
                f"stage {stage.stage_id}  scan {stage.table}  {len(stage.splits)} splits · "
                f"{scan.describe()}"
            )
            sizes = [split.bytes for split in stage.splits]
            if sizes:
                lines.append(
                    f"         split sizes {format_bytes(min(sizes))} .. {format_bytes(max(sizes))}"
                )
            limit = (
                f" · limit {stage.limit} per task and overall" if stage.limit is not None else ""
            )
            lines.append(f"         -> result{limit}")
        lines.append(f"exchange {exchange}")
    return "\n".join(lines) + "\n\n"
