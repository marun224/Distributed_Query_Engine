"""icefleet -- a distributed SQL and DataFrame engine over Apache Iceberg.

Built on icetl: the same sqlglot IR, PyIceberg planning and DuckDB execution, with a query
cut into stages that a fleet of DuckDB workers run at once. The design is
``docs/PLAN_distributed_engine.md``; ``ClusterSession`` arrives in Phase 1.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.0.1"
