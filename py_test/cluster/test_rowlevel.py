"""The row-level suite (Phase 5b): DELETE / UPDATE / MERGE, rewritten file by file on the
workers, read back by icetl equal to what icetl's own planner leaves behind.

Every case runs on twin tables holding the same rows -- the cluster's written by the
cluster (several files, so which files a statement rewrites shows), icetl's by icetl --
and compares what a fresh icetl session reads back. A distributed statement makes one
snapshot (kickoff answer 3), and a file it does not change it does not rewrite
(answer 4).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from icetl.conf import IcetlSettings
from icetl.errors import AnalysisException, UnsupportedFeatureError
from icetl.sql import Session
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import LongType, NestedField, StringType

import icefleet.write.rowlevel as rowlevel
from icefleet.session import ClusterSession
from py_test.conftest import cluster_conf, with_spill
from py_test.oracle import assert_same_rows

NAMESPACE = "r"
SOURCE = "SELECT * FROM fx.many"
#: A source of every 50th row, renamed and with its own qty: 40 rows, keys unique.
EVERY_50TH = "(SELECT id, concat('m', name) AS name, qty FROM fx.many WHERE id % 50 = 0) s"
#: An INSERT naming every column of fx.many, the ones the cluster's statement leaves out
#: as NULLs typed by hand -- which icetl needs (its untyped NULLs fail the cast).
EVERY_COLUMN_TYPED = (
    "(id, name, flag, amount, qty, price, day, ts, tstz, raw) VALUES (s.id, s.name, s.flag, "
    "CAST(NULL AS DOUBLE), CAST(NULL AS INT), CAST(NULL AS DECIMAL(10,2)), "
    "CAST(NULL AS DATE), CAST(NULL AS TIMESTAMP_NTZ), CAST(NULL AS TIMESTAMP), "
    "CAST(NULL AS BINARY))"
)


@pytest.fixture(scope="module")
def cluster(
    local_settings: IcetlSettings, catalog: SqlCatalog, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[ClusterSession]:
    """`local[2]` only: a rewrite is Phase 5's write path, which the write suite already
    runs on both fleets, and twice over would cost the gate its time budget."""
    root = tmp_path_factory.mktemp("rowlevel")
    session = ClusterSession(
        settings=with_spill(local_settings, root),
        conf=cluster_conf(2, root / "exchange"),
        catalog=catalog,
    )
    yield session
    session.stop()


def unique(stem: str) -> str:
    return f"{NAMESPACE}.{stem}_{uuid.uuid4().hex[:8]}"


def read(oracle: Session, name: str) -> pa.Table:
    oracle._invalidate_source(oracle._resolver.parse(name))
    return oracle.table(name).toArrow()


def snapshots(catalog: SqlCatalog, name: str) -> list[Any]:
    return list(catalog.load_table(name).snapshots())


def data_paths(catalog: SqlCatalog, name: str) -> set[str]:
    return {task.file.file_path for task in catalog.load_table(name).scan().plan_files()}


def table_dir(catalog: SqlCatalog, name: str) -> Path:
    return Path(catalog.load_table(name).location().replace("file://", ""))


def twins(
    cluster: ClusterSession,
    oracle: Session,
    stem: str,
    *,
    query: str = SOURCE,
    partition_by: tuple[str, ...] = (),
) -> tuple[str, str]:
    """(the cluster's table, icetl's), the same rows; the cluster's in several files."""
    ours, theirs = unique(stem), unique(f"{stem}_icetl")
    for session, name in ((cluster, ours), (oracle, theirs)):
        writer = session.sql(query).write
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(name)
    return ours, theirs


def both(cluster: ClusterSession, oracle: Session, sql: str, ours: str, theirs: str) -> None:
    cluster.sql(sql.format(t=ours))
    oracle.sql(sql.format(t=theirs))


def distributed(cluster: ClusterSession) -> None:
    run = cluster._last_write
    assert run is not None and run.mode == "distributed", run


def unreferenced(catalog: SqlCatalog, name: str) -> list[Path]:
    """Files under the table's `data/icefleet/` that no snapshot of it references."""
    referenced: set[str] = set()
    table = catalog.load_table(name)
    for snapshot in table.snapshots():
        for task in table.scan(snapshot_id=snapshot.snapshot_id).plan_files():
            referenced.add(task.file.file_path.replace("file://", ""))
    root = table_dir(catalog, name) / "data" / "icefleet"
    return sorted(p for p in root.rglob("*.parquet") if str(p) not in referenced)


# -- equal to icetl, one snapshot ------------------------------------------------------

STATEMENTS = [
    pytest.param("DELETE FROM {t} WHERE id < 100", id="delete-range"),
    pytest.param("DELETE FROM {t} WHERE name LIKE '%7%'", id="delete-inexact"),
    pytest.param("DELETE FROM {t} WHERE qty > 5", id="delete-null-survives"),
    pytest.param("DELETE FROM {t} t WHERE t.amount > 50 AND t.flag", id="delete-aliased"),
    pytest.param("UPDATE {t} SET qty = qty + 1 WHERE id % 3 = 0", id="update"),
    pytest.param("UPDATE {t} SET name = upper(name), qty = 0", id="update-everything"),
    pytest.param("UPDATE {t} SET price = price * 2 WHERE qty > 5", id="update-null-keeps"),
    pytest.param(
        "UPDATE {t} SET name = CAST(qty AS STRING), qty = length(name) WHERE id < 50",
        id="update-sees-old-values",
    ),
    pytest.param(
        "MERGE INTO {t} t USING " + EVERY_50TH + " ON t.id = s.id "
        "WHEN MATCHED AND s.qty > 3 THEN DELETE WHEN MATCHED THEN UPDATE SET qty = s.qty * 10",
        id="merge-update-delete",
    ),
    pytest.param(
        "MERGE INTO {t} t USING " + EVERY_50TH + " ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET name = s.name "
        "WHEN NOT MATCHED BY SOURCE AND t.id < 500 THEN DELETE",
        id="merge-by-source",
    ),
    pytest.param(
        "MERGE INTO {t} t USING (SELECT id, name, amount, qty, price, flag, day, ts, tstz, raw "
        "FROM fx.many WHERE id % 97 = 0) s ON t.id = s.id + 1 "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        id="merge-set-star-insert-star",
    ),
    pytest.param(
        "MERGE INTO {t} t USING " + EVERY_50TH + " ON t.id = s.id AND t.flag "
        "WHEN MATCHED THEN UPDATE SET qty = -1",
        id="merge-target-only-on",
    ),
    pytest.param(
        "MERGE INTO {t} t USING (SELECT id, name FROM {t} WHERE id < 10) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET name = concat(s.name, '!')",
        id="merge-from-itself",
    ),
]


@pytest.mark.parametrize("sql", STATEMENTS)
def test_the_statement_leaves_what_icetl_leaves(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog, sql: str
) -> None:
    ours, theirs = twins(cluster, oracle, "same")
    before = len(snapshots(catalog, ours))
    both(cluster, oracle, sql, ours, theirs)
    distributed(cluster)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    added = snapshots(catalog, ours)[before:]
    assert [s.summary.operation.value for s in added] == ["overwrite"]
    assert unreferenced(catalog, ours) == []


def test_only_the_files_holding_a_match_are_rewritten(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    """Iceberg's statistics cannot rule a file out by `id % 2000`; the probe can."""
    ours, theirs = twins(cluster, oracle, "narrow")
    files = data_paths(catalog, ours)
    assert len(files) > 1
    both(cluster, oracle, "DELETE FROM {t} WHERE id % 2000 = 5", ours, theirs)
    distributed(cluster)
    summary = snapshots(catalog, ours)[-1].summary
    assert summary is not None and summary["deleted-data-files"] == "1"
    assert len(files - data_paths(catalog, ours)) == 1  # the others are untouched
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


def test_a_merge_rewrites_only_the_files_its_keys_are_in(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    """The source's key values narrow the probe, exactly translatable or not (a date)."""
    ours, theirs = twins(cluster, oracle, "mnarrow")
    both(
        cluster,
        oracle,
        "MERGE INTO {t} t USING (SELECT id, day, 'merged' AS name FROM fx.many "
        "WHERE id % 2000 = 7) s ON t.id = s.id AND t.day = s.day "
        "WHEN MATCHED THEN UPDATE SET name = s.name",
        ours,
        theirs,
    )
    distributed(cluster)
    summary = snapshots(catalog, ours)[-1].summary
    assert summary is not None and summary["deleted-data-files"] == "1"
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


def test_a_statement_matching_nothing_commits_nothing(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, _ = twins(cluster, oracle, "nothing")
    before = len(snapshots(catalog, ours))
    cluster.sql(f"DELETE FROM {ours} WHERE id < 0")
    cluster.sql(f"UPDATE {ours} SET qty = 0 WHERE name = 'nobody'")
    assert len(snapshots(catalog, ours)) == before
    assert cluster._last_write is not None and "no data file" in (cluster._last_write.reason or "")


# -- partitioned tables --------------------------------------------------------------------


def test_a_delete_by_partition_is_answered_from_metadata(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = twins(cluster, oracle, "bypart", partition_by=("flag",))
    kept = {p for p in data_paths(catalog, ours) if "flag=true" not in p}
    both(cluster, oracle, "DELETE FROM {t} WHERE flag = true", ours, theirs)
    run = cluster._last_write
    assert run is not None and run.mode == "metadata", run
    assert data_paths(catalog, ours) <= kept  # no file was rewritten
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


def test_a_delete_without_a_condition_empties_the_table_from_metadata(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = twins(cluster, oracle, "all")
    both(cluster, oracle, "DELETE FROM {t}", ours, theirs)
    assert cluster._last_write is not None and cluster._last_write.mode == "metadata"
    assert read(oracle, ours).num_rows == 0 == read(oracle, theirs).num_rows


def test_a_rewrite_within_partitions_writes_a_file_per_value_and_task(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = twins(cluster, oracle, "inpart", partition_by=("flag",))
    both(cluster, oracle, "UPDATE {t} SET qty = 7 WHERE id % 5 = 0", ours, theirs)
    distributed(cluster)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


def test_an_update_of_the_partition_column_moves_rows_between_partitions(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = twins(cluster, oracle, "moved", partition_by=("flag",))
    both(cluster, oracle, "UPDATE {t} SET flag = NOT flag WHERE id < 500", ours, theirs)
    distributed(cluster)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    # Rows that changed partition were shuffled to it: every file holds one flag value.
    for path in data_paths(catalog, ours):
        flags = pq.read_table(path.replace("file://", ""), columns=["flag"]).column("flag")
        assert len(set(flags.to_pylist())) == 1, path


def test_a_merge_inserting_into_a_partitioned_table(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = twins(cluster, oracle, "mpart", partition_by=("flag",))
    sql = (
        "MERGE INTO {t} t USING (SELECT id + 3000 AS id, name, flag FROM fx.many WHERE id < 30) s "
        "ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, name, flag) VALUES (s.id, s.name, s.flag)"
    )
    cluster.sql(sql.format(t=ours))
    oracle.sql(
        sql.replace("(id, name, flag) VALUES (s.id, s.name, s.flag)", EVERY_COLUMN_TYPED).format(
            t=theirs
        )
    )
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


# -- MERGE ---------------------------------------------------------------------------------


def test_an_insert_only_merge_is_an_append(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = twins(cluster, oracle, "append")
    files = data_paths(catalog, ours)
    before = len(snapshots(catalog, ours))
    both(
        cluster,
        oracle,
        "MERGE INTO {t} t USING (SELECT * FROM fx.many WHERE id % 400 = 0) s ON t.id = s.id - 1 "
        "WHEN NOT MATCHED THEN INSERT *",
        ours,
        theirs,
    )
    distributed(cluster)
    assert [s.summary.operation.value for s in snapshots(catalog, ours)[before:]] == ["append"]
    assert files <= data_paths(catalog, ours)  # nothing rewritten
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


def test_a_merge_inserting_some_columns_types_the_rest_where_icetl_fails(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    """icetl writes the columns an INSERT does not name as untyped NULLs, which its cast
    to a timestamp or decimal column refuses; the cluster types them (kickoff answer 7).
    The oracle runs the same statement with every NULL typed by hand."""
    ours, theirs = twins(cluster, oracle, "typed")
    short = (
        "MERGE INTO {t} t USING " + EVERY_50TH + " ON t.id = s.id + 1 "
        "WHEN MATCHED THEN UPDATE SET name = s.name "
        "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
    )
    with pytest.raises(AnalysisException, match="do not fit its schema"):
        oracle.sql(short.format(t=theirs))
    cluster.sql(short.format(t=ours))
    distributed(cluster)
    oracle.sql(
        short.replace("(id, name) VALUES (s.id, s.name)", EVERY_COLUMN_TYPED)
        .replace("s.name, s.flag", "s.name, CAST(NULL AS BOOLEAN)")
        .format(t=theirs)
    )
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


def test_a_merge_matching_one_target_row_twice_is_refused(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, _ = twins(cluster, oracle, "twice")
    before = len(snapshots(catalog, ours))
    with pytest.raises(AnalysisException, match="matched more than one row"):
        cluster.sql(
            f"MERGE INTO {ours} t USING (SELECT id % 10 AS id FROM fx.many) s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET qty = 0"
        )
    assert len(snapshots(catalog, ours)) == before


def test_a_merge_into_an_empty_table_inserts(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = twins(cluster, oracle, "empty", query=f"{SOURCE} WHERE id < 0")
    both(
        cluster,
        oracle,
        "MERGE INTO {t} t USING (SELECT * FROM fx.many WHERE id < 20) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        ours,
        theirs,
    )
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    assert read(oracle, ours).num_rows == 20


# -- concurrency and failure ---------------------------------------------------------------


def test_a_commit_in_between_makes_the_statement_plan_again(
    cluster: ClusterSession,
    oracle: Session,
    catalog: SqlCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ours, _ = twins(cluster, oracle, "raced")
    real = rowlevel.replace_files
    calls: list[int] = []

    def racing(table: Any, remove: Any, paths: Any) -> None:
        calls.append(len(paths))
        if len(calls) == 1:  # another writer commits first
            oracle.sql(f"INSERT INTO {ours} SELECT * FROM fx.many WHERE id = 7")
        real(table, remove, paths)

    monkeypatch.setattr(rowlevel, "replace_files", racing)
    cluster.sql(f"UPDATE {ours} SET qty = -1 WHERE id = 7")
    assert len(calls) == 2  # refused once, planned again
    rows = read(oracle, ours).filter(pc.field("id") == 7)
    assert rows.column("qty").to_pylist() == [-1, -1]  # the late row was updated too
    assert unreferenced(catalog, ours) == []  # the refused attempt's files are gone


def test_a_failed_rewrite_leaves_the_table_as_it_was(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, _ = twins(cluster, oracle, "failed")
    before = len(snapshots(catalog, ours))
    rows = read(oracle, ours)
    with pytest.raises(Exception, match="boom"):
        cluster.sql(f"UPDATE {ours} SET qty = CASE WHEN id = 1999 THEN error('boom') ELSE qty END")
    assert len(snapshots(catalog, ours)) == before
    assert_same_rows(read(oracle, ours), rows)
    assert unreferenced(catalog, ours) == []


# -- what runs on icetl's planner -----------------------------------------------------------


def test_a_bucket_partitioned_target_runs_on_icetls_planner(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
    )
    spec = PartitionSpec(PartitionField(1, 1000, BucketTransform(4), "id_bucket"))
    ours, theirs = unique("bucket"), unique("bucket_icetl")
    rows = pa.table({"id": pa.array(range(20), pa.int64()), "name": [f"n{i}" for i in range(20)]})
    for name in (ours, theirs):
        catalog.create_namespace_if_not_exists(NAMESPACE)
        catalog.create_table(name, schema, partition_spec=spec).append(rows)
    both(cluster, oracle, "UPDATE {t} SET name = 'x' WHERE id < 5", ours, theirs)
    run = cluster._last_write
    assert run is not None and run.mode == "single-node" and "bucket" in (run.reason or "")
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


def test_a_delete_with_an_in_subquery_runs_on_icetls_planner(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    """`NOT COALESCE(x IN (SELECT ...), FALSE)` keeps its subquery inside an expression,
    which no stage holds (Phase 3 makes only a top-level IN a semi join)."""
    ours, theirs = twins(cluster, oracle, "insub")
    both(
        cluster,
        oracle,
        "DELETE FROM {t} WHERE id IN (SELECT id FROM fx.many WHERE qty = 3)",
        ours,
        theirs,
    )
    run = cluster._last_write
    assert run is not None and run.mode == "single-node", run
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


def test_fallback_error_refuses_a_statement_icetl_would_plan(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, _ = twins(cluster, oracle, "refused")
    before = len(snapshots(catalog, ours))
    cluster.conf.set("icefleet.fallback", "error")
    try:
        with pytest.raises(UnsupportedFeatureError, match="Distributing this write"):
            cluster.sql(f"DELETE FROM {ours} WHERE id IN (SELECT id FROM fx.many WHERE qty = 3)")
    finally:
        cluster.conf.set("icefleet.fallback", "single-node")
    assert len(snapshots(catalog, ours)) == before


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM {t} USING fx.many m WHERE {t}.id = m.id",
        "UPDATE {t} SET qty = 0 FROM fx.many m WHERE {t}.id = m.id",
    ],
    ids=["delete-using", "update-from"],
)
def test_what_icetl_refuses_is_refused(cluster: ClusterSession, oracle: Session, sql: str) -> None:
    ours, theirs = twins(cluster, oracle, "refusal")
    with pytest.raises(UnsupportedFeatureError) as theirs_error:
        oracle.sql(sql.format(t=theirs))
    with pytest.raises(UnsupportedFeatureError) as ours_error:
        cluster.sql(sql.format(t=ours))
    assert str(ours_error.value).split(":")[0] == str(theirs_error.value).split(":")[0]
