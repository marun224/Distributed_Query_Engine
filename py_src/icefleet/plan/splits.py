"""Cutting one scan's files into splits (PLAN §4.5).

A file's weight is the bytes of the *selected* columns, from the manifest's per-column
sizes -- the number icetl's `explain()` prints as bytes scanned -- falling back to the
file size. Files are never cut (decision 6).

The split count is the larger of "enough splits that none exceeds the target" and
"enough to give every slot work", capped by `icefleet.split.max` and by the file count.
Files are then dealt largest-first onto the least-loaded split (LPT), which keeps the
heaviest split within 4/3 of the best possible.
"""

from __future__ import annotations

import heapq
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from icetl.paths import engine_path

if TYPE_CHECKING:
    from icetl.exec.scan_planner import ScanPlan

__all__ = ["Split", "SplitFile", "file_weights", "plan_splits"]


@dataclass(frozen=True)
class SplitFile:
    """One data file and its weight for packing."""

    path: str
    bytes: int


@dataclass(frozen=True)
class Split:
    """The files one task reads."""

    files: tuple[SplitFile, ...]

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(f.path for f in self.files)

    @property
    def bytes(self) -> int:
        return sum(f.bytes for f in self.files)


def plan_splits(
    files: Sequence[SplitFile],
    *,
    target_bytes: int,
    min_splits: int,
    max_splits: int,
) -> tuple[Split, ...]:
    """Pack `files` into splits. Every file lands in exactly one split; none is empty."""
    if not files:
        return ()
    total = sum(max(f.bytes, 1) for f in files)
    count = max(math.ceil(total / max(target_bytes, 1)), min_splits, 1)
    count = min(count, max(max_splits, 1), len(files))

    bins: list[list[SplitFile]] = [[] for _ in range(count)]
    heap = [(0, index) for index in range(count)]
    for item in sorted(files, key=lambda f: (-f.bytes, f.path)):
        load, index = heapq.heappop(heap)
        bins[index].append(item)
        heapq.heappush(heap, (load + max(item.bytes, 1), index))

    splits = [Split(tuple(sorted(b, key=lambda f: f.path))) for b in bins if b]
    return tuple(sorted(splits, key=lambda s: s.files[0].path))


def _contained_field_ids(field_type: Any) -> set[int]:
    """Every field-id inside a type, nested parts included.

    Copied from icetl's `exec.scan_planner._contained_field_ids` rather than imported,
    because it is private there (PLAN §4.10); `test_seams.py` checks the original still
    exists, so a change upstream is noticed.
    """
    from pyiceberg.types import ListType, MapType, StructType

    ids: set[int] = set()
    if isinstance(field_type, StructType):
        for nested in field_type.fields:
            ids.add(nested.field_id)
            ids |= _contained_field_ids(nested.field_type)
    elif isinstance(field_type, ListType):
        ids.add(field_type.element_id)
        ids |= _contained_field_ids(field_type.element_type)
    elif isinstance(field_type, MapType):
        ids.add(field_type.key_id)
        ids.add(field_type.value_id)
        ids |= _contained_field_ids(field_type.key_type)
        ids |= _contained_field_ids(field_type.value_type)
    return ids


def _selected_field_ids(schema: Any, columns: Iterable[str]) -> set[int]:
    """The field-ids the selected columns occupy (copy of icetl's private helper)."""
    wanted = {name.lower() for name in columns}
    ids: set[int] = set()
    for nested in schema.fields:
        if nested.name.lower() in wanted:
            ids.add(nested.field_id)
            ids |= _contained_field_ids(nested.field_type)
    return ids


def file_weights(scan: ScanPlan, paths: Sequence[str]) -> list[SplitFile]:
    """Weigh `paths` (one file group of `scan`) by the bytes of its selected columns.

    icetl's `ScanPlan` keeps per-group totals, not per-file sizes, so the files are listed
    again from the manifests -- one extra metadata pass on the coordinator, and no data
    read. A path the listing does not return weighs the group's average.
    TODO(icetl 0.2.0): take the sizes from the scan tasks icetl already holds.
    """
    table = scan.source.resolved.table
    field_ids = _selected_field_ids(table.schema(), scan.columns)
    sizes: dict[str, int] = {}
    for task in table.scan(snapshot_id=scan.source.snapshot_id).plan_files():
        data_file = task.file
        column_sizes = data_file.column_sizes or {}
        selected = sum(size for fid, size in column_sizes.items() if fid in field_ids)
        sizes[engine_path(data_file.file_path)] = selected or int(data_file.file_size_in_bytes)
    average = max(1, sum(sizes.get(p, 0) for p in paths) // max(len(paths), 1))
    return [SplitFile(path=p, bytes=sizes.get(p, average)) for p in paths]
