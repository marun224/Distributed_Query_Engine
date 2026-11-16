"""``icefleet-worker`` -- the command-line entry point.

    icefleet-worker serve [--host --port --slots --memory-limit --threads --temp-dir]
    icefleet-worker run-task <spec-uri>        Phase 6b: run one task and exit

Credentials and S3 settings come from the worker's own environment / `.env`, read by
icetl's settings resolver, never over the wire (decision 20). The exchange location
travels in each TaskSpec, so the worker needs no flag for it.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import threading
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from icefleet import __version__

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["build_parser", "main"]

#: Exit code for a subcommand that exists but is not built yet.
EXIT_NOT_IMPLEMENTED = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="icefleet-worker",
        description=(
            "An icefleet worker: executes query stages on DuckDB. Credentials and S3 "
            "settings come from the worker's own .env / environment, never over the wire."
        ),
    )
    parser.add_argument("--version", action="version", version=f"icefleet-worker {__version__}")
    commands = parser.add_subparsers(dest="command", metavar="{serve,run-task}")

    serve = commands.add_parser("serve", help="run a long-lived worker (HTTP control plane)")
    serve.add_argument("--host", default="127.0.0.1", help="interface to bind (default: loopback)")
    serve.add_argument("--port", type=int, default=8815, help="HTTP port; 0 picks a free one")
    serve.add_argument("--slots", type=int, default=2, help="tasks run concurrently (default: 2)")
    serve.add_argument(
        "--memory-limit", default=None, help="DuckDB memory_limit, e.g. 12GB (default: DuckDB's)"
    )
    serve.add_argument(
        "--threads", type=int, default=None, help="DuckDB threads (default: all cores)"
    )
    serve.add_argument("--temp-dir", default=None, help="DuckDB spill directory")
    serve.add_argument(
        "--port-file",
        default=None,
        help="write the bound port here once listening (how local[N] finds its workers)",
    )
    serve.add_argument("--log-level", default="WARNING", help="Python logging level")

    run_task = commands.add_parser("run-task", help="run one task and exit (Phase 6b)")
    run_task.add_argument("spec_uri", help="where the TaskSpec JSON is stored")
    return parser


def _serve(args: argparse.Namespace) -> int:
    from icetl.conf import EngineSettings, IcetlConf, resolve_settings
    from icetl.exec.engine import DuckDBEngine

    from icefleet.worker.runner import TaskRunner
    from icefleet.worker.server import WorkerServer

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(name)s %(message)s")
    if args.slots < 1:
        print("icefleet-worker: --slots must be at least 1", file=sys.stderr)
        return 2

    settings = resolve_settings(IcetlConf())
    engine_settings = EngineSettings(
        threads=args.threads if args.threads is not None else settings.engine.threads,
        memory_limit=args.memory_limit or settings.engine.memory_limit,
        temp_directory=args.temp_dir or settings.engine.temp_directory,
    )
    engine = DuckDBEngine(replace(settings, engine=engine_settings))
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    server = WorkerServer((args.host, args.port), TaskRunner(engine, worker_id), args.slots)

    if args.port_file:
        # Written atomically, so a launcher polling for it never reads half a number.
        target = Path(args.port_file)
        staging = target.with_suffix(".tmp")
        staging.write_text(str(server.server_address[1]), encoding="utf-8")
        os.replace(staging, target)

    def stop(*_: Any) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    print(
        f"icefleet-worker {__version__} listening on {server.endpoint}, {args.slots} slot(s)",
        file=sys.stderr,
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        engine.close()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and run the subcommand. Returns the process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "serve":
        return _serve(args)
    if args.command == "run-task":
        print(
            "icefleet-worker run-task: running one task per process is Phase 6b "
            "(the Argo pod-per-task backend); use `serve` for now.",
            file=sys.stderr,
        )
        return EXIT_NOT_IMPLEMENTED
    parser.print_help(sys.stderr)
    return EXIT_NOT_IMPLEMENTED


if __name__ == "__main__":  # pragma: no cover - exercised through __main__.py
    raise SystemExit(main())
