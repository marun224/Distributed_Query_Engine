"""The worker's memory check at start-up (PLAN §3.5, "Memory is the new failure mode").

On one machine DuckDB over its `memory_limit` spills or raises. In a pod, the process
over the *container's* limit is OOM-killed with no message. So a worker inside a memory
cgroup keeps DuckDB well below that limit:

* no `memory_limit` given: the worker sets one, the cgroup limit minus a headroom of
  20% and at least 1 GiB (the rule the manifests size pods by);
* a `memory_limit` given: refused if it is within 15% of the cgroup limit, because
  Python, Arrow buffers and the HTTP server live outside DuckDB's budget.

Outside a cgroup limit (a laptop, `local[N]`, the dev container) nothing changes.
"""

from __future__ import annotations

from pathlib import Path

from icefleet.config import GiB, MiB, parse_bytes
from icefleet.errors import ClusterConfigurationError

__all__ = [
    "HEADROOM_FRACTION",
    "HEADROOM_MIN_BYTES",
    "REFUSE_WITHIN_FRACTION",
    "cgroup_memory_limit",
    "check_memory_limit",
    "default_memory_limit",
]

#: Headroom left outside DuckDB when the worker picks the limit itself.
HEADROOM_FRACTION = 0.20
HEADROOM_MIN_BYTES = 1 * GiB
#: A given `memory_limit` above (1 - this) of the cgroup limit is refused.
REFUSE_WITHIN_FRACTION = 0.15

#: cgroup v1 reports "no limit" as a page-rounded 2**63 - 1; anything this large is none.
_UNLIMITED = 1 << 60


def cgroup_memory_limit(root: Path = Path("/sys/fs/cgroup")) -> int | None:
    """The memory limit of the cgroup this process runs in, in bytes, or None.

    cgroup v2 (`memory.max`, "max" when unlimited) first, then v1
    (`memory/memory.limit_in_bytes`). Unreadable or absent files mean no limit.
    """
    for candidate in (root / "memory.max", root / "memory" / "memory.limit_in_bytes"):
        try:
            text = candidate.read_text(encoding="ascii").strip()
        except OSError:
            continue
        if text == "max":
            return None
        try:
            value = int(text)
        except ValueError:
            continue
        return None if value <= 0 or value >= _UNLIMITED else value
    return None


def default_memory_limit(cgroup_bytes: int) -> int:
    """DuckDB's budget in a container of `cgroup_bytes`: the limit minus the headroom."""
    headroom = max(int(cgroup_bytes * HEADROOM_FRACTION), HEADROOM_MIN_BYTES)
    budget = cgroup_bytes - headroom
    if budget < 256 * MiB:
        raise ClusterConfigurationError(
            f"The container's memory limit ({_human(cgroup_bytes)}) leaves "
            f"{_human(max(budget, 0))} for DuckDB after the headroom of "
            f"{_human(headroom)} (20%, at least 1 GiB). Give the pod more memory."
        )
    return budget


def check_memory_limit(requested: str | None, cgroup_bytes: int | None) -> str | None:
    """The `memory_limit` the worker's DuckDB should use, or a refusal.

    `requested` is `--memory-limit` (or icetl's `ICETL_DUCKDB_MEMORY_LIMIT`); `None`
    leaves DuckDB's own default when there is no cgroup limit either.
    """
    if cgroup_bytes is None:
        return requested
    if requested is None:
        return f"{default_memory_limit(cgroup_bytes) // MiB}MiB"
    wanted = parse_bytes(requested, key="--memory-limit")
    ceiling = int(cgroup_bytes * (1 - REFUSE_WITHIN_FRACTION))
    if wanted > ceiling:
        raise ClusterConfigurationError(
            f"--memory-limit {requested} is within 15% of the container's memory limit "
            f"({_human(cgroup_bytes)}); a worker over that limit is OOM-killed without a "
            f"message. Use at most {_human(ceiling)}, or leave it unset and the worker "
            f"keeps 20% (at least 1 GiB) for itself."
        )
    return requested


def _human(value: int) -> str:
    return f"{value / GiB:.2f} GiB" if value >= GiB else f"{value // MiB} MiB"
