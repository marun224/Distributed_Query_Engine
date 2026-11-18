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
            indent = " " * 9
            notes = list(stage.notes)
            reads = ", ".join(
                f"stage {i.stage_id}{' (broadcast)' if i.mode == 'all' else ''}"
                for i in stage.inputs
            )
            if stage.scan is not None:
                table = stage.table
                named = (
                    f" as {stage.alias}"
                    if stage.alias and stage.alias != table.rsplit(".", 1)[-1]
                    else ""
                )
                lines.append(
                    f"stage {stage.stage_id}  scan {table}{named}  {len(stage.splits)} splits · "
                    f"{stage.scan.describe()}"
                )
                if reads:
                    notes.insert(0, f"reads {reads}")
                sizes = [split.bytes for split in stage.splits]
                if sizes:
                    lines.append(
                        f"{indent}split sizes {format_bytes(min(sizes))} .. "
                        f"{format_bytes(max(sizes))}"
                    )
            else:
                lines.append(f"stage {stage.stage_id}  {notes.pop(0) if notes else 'reads'}")
                if reads:
                    notes.insert(0, f"reads {reads}")
            lines.extend(f"{indent}{note}" for note in notes)
        note = staged.coordinator_note()
        if note:
            lines.append(f"coordinator  {note}")
        lines.append(f"exchange {exchange}")
    return "\n".join(lines) + "\n\n"
