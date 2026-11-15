"""Phase 0 smoke tests: the package installs, its pillars import, the CLI answers.

Nothing here tests behaviour -- there is none yet. What it pins is that the environment the
later phases assume is really there: the exact icetl pin, Arrow IPC working, DuckDB
importable, and the worker entry point wired into the wheel.
"""

from __future__ import annotations

import importlib.metadata

import pytest

import icefleet
from icefleet.worker.cli import EXIT_NOT_IMPLEMENTED, build_parser, main


def test_version_is_exposed() -> None:
    assert icefleet.__version__ == "0.0.1"
    assert importlib.metadata.version("icefleet") == icefleet.__version__


def test_icetl_is_pinned_exactly() -> None:
    """PLAN 4.10: private seams are overridden, so the pin is exact until the hooks land."""
    import icetl

    assert icetl.__version__ == "0.1.0"
    assert importlib.metadata.version("icetl") == "0.1.0"


def test_pyarrow_ipc_round_trips() -> None:
    """Arrow IPC is the serialisation between coordinator and workers (decision 26).

    IPC lives in pyarrow's core library, which loads on every machine the project has met
    so far -- unlike Flight, see the test below.
    """
    import pyarrow as pa

    table = pa.table({"k": [1, 2, 3], "v": ["a", "b", None]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    back = pa.ipc.open_stream(sink.getvalue()).read_all()
    assert back.equals(table)


def test_pyarrow_flight_availability_is_visible() -> None:
    """Flight is the Phase 8 fast path, and it is *blocked on the dev machine*.

    Windows Smart App Control (enforce mode) refuses pyarrow's unsigned ``arrow_flight.dll``
    (tracker, environment facts). The test skips rather than fails so the fact shows up in
    every run without turning the gate red; on a machine where it loads, it asserts the API.
    """
    try:
        import pyarrow.flight as flight
    except ImportError as exc:
        pytest.skip(f"pyarrow.flight unavailable here: {exc}")
    assert hasattr(flight, "FlightServerBase")
    assert hasattr(flight, "FlightClient")


def test_engine_libraries_import() -> None:
    import duckdb
    import pyiceberg
    import sqlglot
    from icetl.exec.engine import DuckDBEngine
    from icetl.sql import Session

    assert duckdb.__version__
    assert pyiceberg.__version__
    assert sqlglot.__version__
    assert DuckDBEngine is not None and Session is not None


def test_worker_cli_help(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "icefleet-worker" in out
    flags = (
        "--host",
        "--port",
        "--slots",
        "--memory-limit",
        "--threads",
        "--temp-dir",
        "--exchange",
    )
    for flag in flags:
        assert flag in out


def test_worker_cli_defaults() -> None:
    args = build_parser().parse_args([])
    assert (args.host, args.port, args.slots) == ("127.0.0.1", 8815, 2)
    assert args.memory_limit is None and args.exchange is None


def test_worker_cli_has_no_server_yet(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--port", "9000"]) == EXIT_NOT_IMPLEMENTED
    assert "Phase 1" in capsys.readouterr().err
