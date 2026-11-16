"""icefleet -- a distributed SQL and DataFrame engine over Apache Iceberg.

Built on icetl: the same sqlglot IR, PyIceberg planning and DuckDB execution, with a query
cut into stages that a fleet of DuckDB workers run at once. The design is
``docs/PLAN_distributed_engine.md``.

    from icefleet import ClusterSession
    session = ClusterSession.builder.config("icefleet.master", "local[4]").getOrCreate()

``ClusterSession`` is imported lazily, so ``import icefleet`` (which every worker does for
its version) does not pay for the session machinery.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["ClusterSession", "__version__"]

__version__ = "0.0.1"

if TYPE_CHECKING:
    from icefleet.session import ClusterSession


def __getattr__(name: str) -> Any:
    if name == "ClusterSession":
        from icefleet.session import ClusterSession

        return ClusterSession
    raise AttributeError(f"module 'icefleet' has no attribute {name!r}")
