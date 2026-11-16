"""icetl's private surface that icefleet overrides or reads (PLAN §4.10).

icetl is pinned exactly because of these. A pin bump that moves one fails here, by
name, rather than somewhere deep in a query.
"""

from __future__ import annotations

import dataclasses
import inspect

import icetl
import pytest
from icetl.exec import engine as engine_module
from icetl.exec import scan_planner, source_sql
from icetl.plan import analysis, builder
from icetl.sql.dataframe import DataFrame
from icetl.sql.session import CompiledPlan, Session


def params(func: object) -> list[str]:
    return list(inspect.signature(func).parameters)  # type: ignore[arg-type]


def test_the_pin() -> None:
    assert icetl.__version__ == "0.1.0"


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("_execute", ["self", "plan", "sources", "output_names"]),
        ("_stream", ["self", "plan", "sources", "output_names", "batch_size"]),
        ("_compile", ["self", "plan", "sources", "output_names"]),
        ("_count_from_metadata", ["self", "plan", "sources", "output_names"]),
        ("_analyze", ["self", "plan", "sources"]),
        ("stop", ["self"]),
    ],
)
def test_session_methods(name: str, expected: list[str]) -> None:
    assert params(getattr(Session, name)) == expected


def test_session_state_and_builder() -> None:
    session = Session.__init__
    assert params(session) == ["self", "settings", "conf", "catalog"]
    source = inspect.getsource(Session.__init__)
    for attribute in ("self._engine", "self._analyzer", "self._udf", "self._stopped", "self._conf"):
        assert attribute in source, attribute
    assert hasattr(Session, "_active") and hasattr(Session, "_active_lock")
    builder = Session.Builder()
    assert hasattr(builder, "_conf") and hasattr(builder, "_catalog")


def test_compiled_plan_fields() -> None:
    names = [f.name for f in dataclasses.fields(CompiledPlan)]
    assert names == ["sql", "parameters", "scans", "optimized"]


def test_scan_plan_and_groups() -> None:
    scan = {f.name for f in dataclasses.fields(scan_planner.ScanPlan)}
    assert {"source", "groups", "columns"} <= scan
    group = {f.name for f in dataclasses.fields(scan_planner.FileGroup)}
    assert {"paths", "total_bytes", "projection"} <= group
    # Copied into icefleet.plan.splits; the originals must still exist to compare against.
    assert callable(scan_planner._selected_field_ids)
    assert callable(scan_planner._contained_field_ids)


def test_dataframe_seam() -> None:
    assert params(getattr(DataFrame._explain_text, "__wrapped__", DataFrame._explain_text)) == [
        "self",
        "verbose",
    ]
    source = inspect.getsource(DataFrame.__init__)
    for attribute in ("self._session", "self._plan", "self._sources"):
        assert attribute in source, attribute


def test_helpers_icefleet_imports() -> None:
    assert params(builder.substitute_sources) == ["expression", "sources", "factory"]
    assert params(builder.source_key) == ["table", "versioned"]
    assert params(builder.collect_source_keys) == ["expression"]
    assert params(source_sql.build_source) == ["plan", "parameters", "register"]
    assert hasattr(analysis.PlanAnalyzer, "connection")
    for method in ("ensure_object_store", "cursor", "arrow", "record_batches", "register"):
        assert hasattr(engine_module.DuckDBEngine, method), method


def test_build_source_still_names_the_file_list_parameter_paths_0() -> None:
    """plan_stages binds `<view>_paths_0`; icetl must still call the one group that."""
    source = inspect.getsource(source_sql.build_source)
    assert 'f"{view}_paths_{index}"' in source
