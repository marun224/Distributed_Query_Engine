"""`icefleet.*` settings on top of icetl's configuration.

Resolved the way icetl resolves its own: session conf > process environment > `.env` in
the working directory > default. Every key has an environment spelling, so a worker pod or
a CI job can be configured without code.

    icefleet.master               local[2] | static://h:p,... | k8s://svc:port
    icefleet.exchange.location    a directory or s3://bucket/prefix
    icefleet.exchange.keep        keep a query's stage output (debugging)
    icefleet.fallback             single-node | error
    icefleet.split.targetBytes    bytes of selected columns per split
    icefleet.split.max            the most splits one scan is cut into
    icefleet.shuffle.partitions   hash partitions of a grouped aggregate's shuffle
    icefleet.worker.slots         task slots per local[N] worker
    icefleet.local.threads        DuckDB threads per local[N] worker
    icefleet.local.memoryLimit    DuckDB memory_limit per local[N] worker
    icefleet.result.maxBytes      decision 16's guard on what comes back to the coordinator
    icefleet.result.fileBytes     roll result files at this size
    icefleet.task.timeoutSeconds  how long the coordinator waits for one task (default: forever)
    icefleet.worker.startupTimeoutSeconds

The environment spelling is in `ENV_KEYS`: `icefleet.split.targetBytes` is
`ICEFLEET_SPLIT_TARGET_BYTES`, and so on.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from dotenv import dotenv_values
from icetl.conf import IcetlConf

from icefleet.errors import ClusterConfigurationError

__all__ = ["FleetSettings", "parse_bytes", "resolve_fleet_settings"]

MiB = 1024**2
GiB = 1024**3

#: conf key -> environment variable.
ENV_KEYS: dict[str, str] = {
    "icefleet.master": "ICEFLEET_MASTER",
    "icefleet.exchange.location": "ICEFLEET_EXCHANGE_LOCATION",
    "icefleet.exchange.keep": "ICEFLEET_EXCHANGE_KEEP",
    "icefleet.fallback": "ICEFLEET_FALLBACK",
    "icefleet.split.targetBytes": "ICEFLEET_SPLIT_TARGET_BYTES",
    "icefleet.split.max": "ICEFLEET_SPLIT_MAX",
    "icefleet.shuffle.partitions": "ICEFLEET_SHUFFLE_PARTITIONS",
    "icefleet.worker.slots": "ICEFLEET_WORKER_SLOTS",
    "icefleet.local.threads": "ICEFLEET_LOCAL_THREADS",
    "icefleet.local.memoryLimit": "ICEFLEET_LOCAL_MEMORY_LIMIT",
    "icefleet.result.maxBytes": "ICEFLEET_RESULT_MAX_BYTES",
    "icefleet.result.fileBytes": "ICEFLEET_RESULT_FILE_BYTES",
    "icefleet.task.timeoutSeconds": "ICEFLEET_TASK_TIMEOUT_SECONDS",
    "icefleet.worker.startupTimeoutSeconds": "ICEFLEET_WORKER_STARTUP_TIMEOUT_SECONDS",
}

Fallback = Literal["single-node", "error"]


@dataclass(frozen=True)
class FleetSettings:
    """The resolved `icefleet.*` settings. Defaults are placeholders until Phase 6 measures."""

    master: str = "local[2]"
    exchange_location: str | None = None
    exchange_keep: bool = False
    fallback: Fallback = "single-node"
    split_target_bytes: int = 128 * MiB
    split_max: int = 4096
    shuffle_partitions: int = 64
    worker_slots: int = 2
    local_threads: int | None = None
    local_memory_limit: str | None = None
    result_max_bytes: int = 2 * GiB
    result_file_bytes: int = 256 * MiB
    task_timeout_seconds: float | None = None
    worker_startup_timeout_seconds: float = 60.0


_BYTES = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]i?b?|b)?\s*$", re.IGNORECASE)
_UNITS = {"": 1, "b": 1, "k": 1000, "m": 1000**2, "g": 1000**3, "t": 1000**4}


def parse_bytes(value: str | int, *, key: str = "size") -> int:
    """`"128MiB"` -> 134217728; `"2GB"` -> 2000000000; `4096` -> 4096.

    `KiB/MiB/GiB/TiB` are powers of 1024 and `KB/MB/GB/TB` of 1000, as DuckDB spells them.
    """
    if isinstance(value, int):
        return value
    match = _BYTES.match(value)
    if not match:
        raise ClusterConfigurationError(f"{key}: {value!r} is not a size such as 128MiB or 2GB.")
    number, unit = float(match.group(1)), (match.group(2) or "").lower()
    factor = 1024 ** ("kmgt".index(unit[0]) + 1) if "i" in unit else _UNITS[unit[:1]]
    return int(number * factor)


def _as_bool(value: str, *, key: str) -> bool:
    lowered = value.strip().lower()
    if lowered in ("true", "1", "yes", "on"):
        return True
    if lowered in ("false", "0", "no", "off"):
        return False
    raise ClusterConfigurationError(f"{key}: {value!r} is not true or false.")


def _as_int(value: str, *, key: str, minimum: int = 1) -> int:
    try:
        number = int(value)
    except ValueError:
        raise ClusterConfigurationError(f"{key}: {value!r} is not a whole number.") from None
    if number < minimum:
        raise ClusterConfigurationError(f"{key}: {number} is below the minimum of {minimum}.")
    return number


def _as_float(value: str, *, key: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise ClusterConfigurationError(f"{key}: {value!r} is not a number.") from None
    if number <= 0:
        raise ClusterConfigurationError(f"{key}: {number} must be positive.")
    return number


def resolve_fleet_settings(
    conf: IcetlConf | None = None,
    *,
    env: Mapping[str, str] | None = None,
    dotenv_path: str | Path | None = None,
) -> FleetSettings:
    """Resolve every `icefleet.*` key. `env` and `dotenv_path` are injectable for tests."""
    conf = conf or IcetlConf()
    environ: Mapping[str, str] = os.environ if env is None else env
    candidate = Path(dotenv_path) if dotenv_path is not None else Path.cwd() / ".env"
    dotenv = (
        {k: v for k, v in dotenv_values(candidate).items() if v is not None}
        if candidate.is_file()
        else {}
    )

    def pick(key: str) -> str | None:
        value = conf.get(key)
        if value is not None:
            return value
        env_key = ENV_KEYS[key]
        if env_key in environ:
            return environ[env_key]
        return dotenv.get(env_key)

    defaults = FleetSettings()
    fallback = pick("icefleet.fallback") or defaults.fallback
    if fallback not in ("single-node", "error"):
        raise ClusterConfigurationError(
            f"icefleet.fallback: {fallback!r}; expected 'single-node' or 'error'."
        )

    def size(key: str, default: int) -> int:
        raw = pick(key)
        return default if raw is None else parse_bytes(raw, key=key)

    def whole(key: str, default: int) -> int:
        raw = pick(key)
        return default if raw is None else _as_int(raw, key=key)

    threads = pick("icefleet.local.threads")
    timeout = pick("icefleet.task.timeoutSeconds")
    startup = pick("icefleet.worker.startupTimeoutSeconds")
    keep = pick("icefleet.exchange.keep")
    return FleetSettings(
        master=(pick("icefleet.master") or defaults.master).strip(),
        exchange_location=pick("icefleet.exchange.location") or None,
        exchange_keep=_as_bool(keep, key="icefleet.exchange.keep") if keep is not None else False,
        fallback="error" if fallback == "error" else "single-node",
        split_target_bytes=size("icefleet.split.targetBytes", defaults.split_target_bytes),
        split_max=whole("icefleet.split.max", defaults.split_max),
        shuffle_partitions=whole("icefleet.shuffle.partitions", defaults.shuffle_partitions),
        worker_slots=whole("icefleet.worker.slots", defaults.worker_slots),
        local_threads=_as_int(threads, key="icefleet.local.threads") if threads else None,
        local_memory_limit=pick("icefleet.local.memoryLimit") or None,
        result_max_bytes=size("icefleet.result.maxBytes", defaults.result_max_bytes),
        result_file_bytes=size("icefleet.result.fileBytes", defaults.result_file_bytes),
        task_timeout_seconds=(
            _as_float(timeout, key="icefleet.task.timeoutSeconds") if timeout else None
        ),
        worker_startup_timeout_seconds=(
            _as_float(startup, key="icefleet.worker.startupTimeoutSeconds")
            if startup
            else defaults.worker_startup_timeout_seconds
        ),
    )
