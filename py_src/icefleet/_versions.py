"""The versions a coordinator and a worker compare in their handshake (PLAN §4.1).

Partition identity depends on DuckDB's `hash()`, which is not promised stable across
versions, and the task format is icefleet's own -- so a worker that differs in either is
refused rather than trusted.
"""

from __future__ import annotations

import platform

#: Bumped whenever TaskSpec / TaskResult or an HTTP route changes incompatibly.
PROTOCOL_VERSION = 1

#: The keys that must be equal on both ends of a handshake.
MUST_MATCH = ("protocol", "icefleet", "duckdb")


def runtime_versions() -> dict[str, str]:
    """This process's versions, as a worker reports them on `GET /ping`."""
    import duckdb
    import icetl
    import pyarrow

    import icefleet

    return {
        "protocol": str(PROTOCOL_VERSION),
        "icefleet": icefleet.__version__,
        "duckdb": str(duckdb.__version__),
        "pyarrow": str(pyarrow.__version__),
        "icetl": str(icetl.__version__),
        "python": platform.python_version(),
    }


def mismatches(local: dict[str, str], remote: dict[str, str]) -> list[str]:
    """`["duckdb 1.5.6 here, 1.4.0 there", ...]` for every key that must match and does not."""
    return [
        f"{key} {local.get(key)} here, {remote.get(key, 'missing')} there"
        for key in MUST_MATCH
        if local.get(key) != remote.get(key)
    ]
