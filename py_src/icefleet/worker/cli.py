"""``icefleet-worker`` -- the command-line entry point.

Phase 0 ships the argument surface only, so that the CLI, its help text and its packaging
are tested from the first day. The HTTP server behind it is Phase 1 (PLAN §6, decision 26).
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from icefleet import __version__

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["build_parser", "main"]

#: Exit code when the CLI parsed its arguments but has no server to run yet.
EXIT_NOT_IMPLEMENTED = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="icefleet-worker",
        description=(
            "Run one icefleet worker: an HTTP server that executes query stages "
            "on a DuckDB engine. Credentials and S3 settings come from the worker's own "
            ".env / environment, never over the wire."
        ),
    )
    parser.add_argument("--host", default="127.0.0.1", help="interface to bind (default: loopback)")
    parser.add_argument("--port", type=int, default=8815, help="HTTP port (default: 8815)")
    parser.add_argument("--slots", type=int, default=2, help="tasks run concurrently (default: 2)")
    parser.add_argument(
        "--memory-limit",
        default=None,
        help="DuckDB memory_limit, e.g. 12GB (default: DuckDB's own)",
    )
    parser.add_argument(
        "--threads", type=int, default=None, help="DuckDB threads (default: all cores)"
    )
    parser.add_argument("--temp-dir", default=None, help="DuckDB spill directory")
    parser.add_argument(
        "--exchange", default=None, help="exchange location: a directory or s3://bucket/prefix"
    )
    parser.add_argument("--version", action="version", version=f"icefleet-worker {__version__}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and run the worker. Returns the process exit code."""
    args = build_parser().parse_args(argv)
    print(
        f"icefleet-worker {__version__}: parsed --host {args.host} --port {args.port} "
        f"--slots {args.slots}; the HTTP server is Phase 1, nothing to run yet.",
        file=sys.stderr,
    )
    return EXIT_NOT_IMPLEMENTED


if __name__ == "__main__":  # pragma: no cover - exercised through __main__.py
    raise SystemExit(main())
