"""The exchange protocol and the factory that picks an implementation from a location.

Layout, shared by every implementation (PLAN §4.4):

    <location>/<query_id>/stage-<k>/task-<i>/data_<n>.parquet

Workers write there with DuckDB; the coordinator reads exactly the files the tasks'
manifests name -- never a glob, so a failed attempt's leftovers are never read (P5) --
and deletes the query's prefix when it is done, unless `icefleet.exchange.keep` is set.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from icetl.paths import is_object_store

if TYPE_CHECKING:
    from collections.abc import Sequence

    from icetl.conf import S3Settings

__all__ = ["Exchange", "open_exchange"]


class Exchange(ABC):
    """Addresses stage output and removes it. Reading and writing are DuckDB's job."""

    def __init__(self, location: str) -> None:
        self.location = location.rstrip("/")

    def query_prefix(self, query_id: str) -> str:
        return f"{self.location}/{query_id}"

    def task_location(self, query_id: str, stage_id: int, task_index: int) -> str:
        """The directory one task writes its files into."""
        return f"{self.query_prefix(query_id)}/stage-{stage_id}/task-{task_index}"

    @abstractmethod
    def exists(self, path: str) -> bool:
        """True when anything is stored at `path` (a file or a prefix)."""

    @abstractmethod
    def delete_query(self, query_id: str) -> None:
        """Remove everything a query wrote. Idempotent: an absent prefix is not an error."""

    @abstractmethod
    def delete_files(self, paths: Sequence[str]) -> None:
        """Remove these files (as DuckDB reported writing them). An absent one is fine."""

    def describe(self) -> str:
        return self.location


def open_exchange(location: str, s3: S3Settings | None = None) -> Exchange:
    """`s3://...` (and `s3a://`) -> object store; anything else -> a local directory."""
    if is_object_store(location):
        from icefleet.exchange.object_store import ObjectStoreExchange

        return ObjectStoreExchange(location, s3)
    from icefleet.exchange.local import LocalDirExchange

    return LocalDirExchange(location)
