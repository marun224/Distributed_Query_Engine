"""`LocalDirExchange`: the exchange as a directory, for `local[N]` and the offline suite.

The same code as the object store in every way that matters -- the layout and the
manifests -- so what the offline suite proves carries over; only listing and deletion
differ.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from icetl.paths import engine_path

from icefleet.exchange.base import Exchange

__all__ = ["LocalDirExchange"]


class LocalDirExchange(Exchange):
    """Stage output under a local (or shared) directory."""

    def __init__(self, location: str) -> None:
        root = Path(engine_path(location)).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        super().__init__(root.as_posix())

    def exists(self, path: str) -> bool:
        return Path(path).exists()

    def delete_query(self, query_id: str) -> None:
        shutil.rmtree(self.query_prefix(query_id), ignore_errors=True)
