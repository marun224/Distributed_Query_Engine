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


def test_the_write_seams() -> None:
    """Phase 5 replaces `DataFrameWriter._save` and `_derive`, wraps `DataFrame.write`,
    reads the writer's settings, and calls PyIceberg's private schema check."""
    from icetl.sql import writer
    from pyiceberg.io import pyarrow as iceberg_arrow

    assert params(writer.DataFrameWriter._save) == ["self", "name", "insert_into", "overwrite"]
    assert params(writer.DataFrameWriter._derive) == ["self", "changes"]
    for method in (
        "_merges_schema",
        "_dynamic_partitions",
        "_apply_partitioning",
        "_apply_sort_order",
    ):
        assert hasattr(writer.DataFrameWriter, method), method
    source = inspect.getsource(writer.DataFrameWriter.__init__)
    for attribute in ("_df", "_mode", "_options", "_partition_by", "_sort_by", "_properties"):
        assert f"self.{attribute} =" in source, attribute
    assert params(writer.commit_with_retry) == ["operation"]
    assert params(writer.iceberg_ready) == ["data"]
    write = DataFrame.write
    original = getattr(write, "__wrapped__", write)
    assert isinstance(original, property)
    assert params(iceberg_arrow._check_pyarrow_schema_compatible) == [
        "requested_schema",
        "provided_schema",
        "downcast_ns_timestamp_to_us",
        "format_version",
    ]
    for method in ("_invalidate_source",):
        assert hasattr(Session, method), method


def test_the_row_level_seams() -> None:
    """Phase 5b overrides `Session._row_level` and builds on icetl's row-level planner's
    private helpers; the file replacement uses PyIceberg's private footer reader."""
    from icetl.plan import pushdown
    from icetl.sql import rowlevel
    from pyiceberg import table as iceberg_table
    from pyiceberg.table.update import snapshot

    assert params(Session._row_level) == ["self", "plan"]
    for method in ("_inline_temp_views", "_frame_for", "_empty_frame", "_resolver"):
        assert hasattr(Session, method) or method in inspect.getsource(Session.__init__), method
    expected = {
        "_resolve_target": ["session", "node"],
        "_where": ["plan"],
        "_assignments": ["items", "target"],
        "_whens": ["plan", "target", "source_alias", "source_columns"],
        "_key_pairs": ["terms", "target", "source_alias"],
        "_distinct_values": ["session", "source", "expression"],
        "_clause_projections": ["target", "clauses"],
        "_survives": ["clauses"],
        "_restrict": ["select", "conditions"],
        "_aliased": ["node", "alias"],
        "_columns_of": ["session", "source"],
        "_qualified": ["alias", "column"],
        "_is_true": ["condition"],
        "_all_of": ["terms"],
        "run_delete": ["session", "plan"],
        "run_update": ["session", "plan"],
        "run_merge": ["session", "plan"],
    }
    for name, arguments in expected.items():
        assert params(getattr(rowlevel, name)) == arguments, name
    assert isinstance(rowlevel._ATTEMPTS, int) and isinstance(rowlevel._BACKOFF_SECONDS, float)
    target = {f.name for f in dataclasses.fields(rowlevel._Target)}
    assert {"node", "alias", "columns", "resolver", "snapshot_id"} <= target
    when = {f.name for f in dataclasses.fields(rowlevel._When)}
    assert {"kind", "action", "condition", "assignments", "inserts"} <= when
    assert params(pushdown.scope_predicate) == ["terms", "resolver", "schema", "exact_only"]
    assert hasattr(pushdown.ColumnResolver, "owns_every_column")
    assert params(iceberg_table._parquet_files_to_data_files) == [
        "table_metadata",
        "file_paths",
        "io",
    ]
    for method in ("delete_data_file", "append_data_file"):
        assert hasattr(snapshot._OverwriteFiles, method), method
