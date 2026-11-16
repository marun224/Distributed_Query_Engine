"""`ObjectStoreExchange`: the exchange in S3 or MinIO.

Workers write with DuckDB's httpfs, using the S3 settings from their own environment
(decision 20); the coordinator lists and deletes with s3fs, using the session's. Both
point at the same bucket because both read the same `ICETL_S3_*` variables.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from icefleet.exchange.base import Exchange

if TYPE_CHECKING:
    from icetl.conf import S3Settings

__all__ = ["ObjectStoreExchange", "s3_filesystem"]


def s3_filesystem(s3: S3Settings | None) -> Any:
    """An `s3fs.S3FileSystem` configured from icetl's S3 settings."""
    import s3fs

    kwargs: dict[str, Any] = {}
    if s3 is not None:
        if s3.access_key_id:
            kwargs["key"] = s3.access_key_id
        if s3.secret_access_key:
            kwargs["secret"] = s3.secret_access_key
        if s3.session_token:
            kwargs["token"] = s3.session_token
        if s3.endpoint:
            kwargs["endpoint_url"] = s3.endpoint
        if s3.region:
            kwargs["client_kwargs"] = {"region_name": s3.region}
        if s3.path_style_access:
            kwargs["config_kwargs"] = {"s3": {"addressing_style": "path"}}
    return s3fs.S3FileSystem(**kwargs)


def _key(path: str) -> str:
    """`s3://bucket/a/b` (or `s3a://`) -> `bucket/a/b`, the form s3fs takes."""
    return path.split("://", 1)[1] if "://" in path else path


class ObjectStoreExchange(Exchange):
    """Stage output under an `s3://bucket/prefix`."""

    def __init__(self, location: str, s3: S3Settings | None = None) -> None:
        if location.startswith(("s3a://", "s3n://")):
            location = "s3://" + location.split("://", 1)[1]
        super().__init__(location)
        self._fs = s3_filesystem(s3)

    @property
    def bucket(self) -> str:
        return _key(self.location).split("/", 1)[0]

    def ensure_bucket(self) -> bool:
        """Create the bucket if it is missing. True when it had to be created."""
        if self._fs.exists(self.bucket):
            return False
        self._fs.mkdir(self.bucket)
        return True

    def exists(self, path: str) -> bool:
        self._fs.invalidate_cache()
        return bool(self._fs.exists(_key(path)))

    def delete_query(self, query_id: str) -> None:
        prefix = _key(self.query_prefix(query_id))
        with contextlib.suppress(FileNotFoundError):  # nothing written, nothing to remove
            self._fs.rm(prefix, recursive=True)
        self._fs.invalidate_cache()
