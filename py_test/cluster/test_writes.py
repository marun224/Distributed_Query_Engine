"""The write suite (Phase 5): what the workers write, icetl reads back as icetl would have
written it.

Every case writes twice -- through the cluster, and through plain icetl into a twin
table -- and compares what a fresh icetl session reads back, and the snapshots each
write made. The cluster's writes must be distributed (asserted), and its data files the
workers' own (one per partition value for a partitioned table).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from icetl.errors import AnalysisException, TableAlreadyExistsException, UnsupportedFeatureError
from icetl.sql import DataFrame, Session
from icetl.sql import functions as F
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    YearTransform,
)
from pyiceberg.types import LongType, NestedField, StringType, TimestampType, TimestamptzType

from icefleet.session import ClusterSession
from py_test.oracle import assert_same_rows

Query = Callable[[Session], DataFrame]
NAMESPACE = "w"


def unique(stem: str) -> str:
    return f"{NAMESPACE}.{stem}_{uuid.uuid4().hex[:8]}"


def read(oracle: Session, name: str) -> pa.Table:
    """A table as a fresh icetl read sees it: the session's cached snapshot dropped first."""
    oracle._invalidate_source(oracle._resolver.parse(name))
    return oracle.table(name).toArrow()


def operations(catalog: SqlCatalog, name: str) -> list[str]:
    return [s.summary.operation.value for s in catalog.load_table(name).snapshots() if s.summary]


def data_files(catalog: SqlCatalog, name: str) -> list[Any]:
    return [task.file for task in catalog.load_table(name).scan().plan_files()]


def distributed(cluster: ClusterSession) -> None:
    run = cluster._last_write
    assert run is not None and run.mode == "distributed", run


def single_node(cluster: ClusterSession, why: str) -> None:
    run = cluster._last_write
    assert run is not None and run.mode == "single-node" and why in (run.reason or ""), run


def many(s: Session) -> DataFrame:
    return s.table("fx.many").select("id", "name", "amount", "qty", "price", "flag", "day", "ts")


# -- creating, appending, overwriting --------------------------------------------------


def test_save_as_table_creates_and_fills(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = unique("created"), unique("created_icetl")
    many(cluster).write.saveAsTable(ours)
    distributed(cluster)
    many(oracle).write.saveAsTable(theirs)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    assert catalog.load_table(ours).schema() == catalog.load_table(theirs).schema()
    assert operations(catalog, ours) == operations(catalog, theirs) == ["append"]


def test_a_partitioned_table_gets_a_file_per_partition_value(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = unique("by_flag"), unique("by_flag_icetl")
    many(cluster).write.partitionBy("flag").saveAsTable(ours)
    distributed(cluster)
    many(oracle).write.partitionBy("flag").saveAsTable(theirs)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    files = data_files(catalog, ours)
    # true, false and NULL: one file each, however many tasks read the input
    assert sorted(str(f.partition) for f in files) == sorted(
        str(f.partition) for f in data_files(catalog, theirs)
    )
    assert len(files) == 3


def test_append_and_overwrite_make_icetl_s_snapshots(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = unique("modes"), unique("modes_icetl")
    for session, name in ((cluster, ours), (oracle, theirs)):
        many(session).filter(F.col("id") < 500).write.saveAsTable(name)
        many(session).filter(F.col("id") >= 1500).write.mode("append").saveAsTable(name)
    distributed(cluster)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    for session, name in ((cluster, ours), (oracle, theirs)):
        many(session).filter(F.col("qty") > 30).write.mode("overwrite").saveAsTable(name)
    distributed(cluster)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    assert operations(catalog, ours) == operations(catalog, theirs)
    assert operations(catalog, ours)[-2:] == ["delete", "append"]


def test_dynamic_partition_overwrite_replaces_only_what_it_writes(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = unique("dynamic"), unique("dynamic_icetl")
    for session, name in ((cluster, ours), (oracle, theirs)):
        many(session).write.partitionBy("name").saveAsTable(name)
        (
            many(session)
            .filter(F.col("name").isin("name-01", "name-02") | F.col("name").isNull())
            .filter(F.col("id") < 300)
            .write.mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .saveAsTable(name)
        )
    distributed(cluster)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    assert operations(catalog, ours) == operations(catalog, theirs)


def test_insert_into_matches_by_position(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = unique("positional"), unique("positional_icetl")
    for session, name in ((cluster, ours), (oracle, theirs)):
        session.table("fx.plain").limit(0).write.saveAsTable(name)
        session.table("fx.many").select(
            F.col("id"), F.col("name").alias("x"), F.col("amount").alias("y")
        ).write.insertInto(name)
    distributed(cluster)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


def test_sql_insert_and_create_table_as(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = unique("sql"), unique("sql_icetl")
    for session, name in ((cluster, ours), (oracle, theirs)):
        session.sql(f"CREATE TABLE {name} AS SELECT id, name, qty FROM fx.many WHERE qty > 0")
        session.sql(f"INSERT INTO {name} SELECT id, name, qty FROM fx.many WHERE qty < -40")
    distributed(cluster)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    for session, name in ((cluster, ours), (oracle, theirs)):
        session.sql(f"INSERT OVERWRITE {name} SELECT id, name, qty FROM fx.many WHERE id < 10")
    distributed(cluster)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    assert operations(catalog, ours) == operations(catalog, theirs)


@pytest.mark.parametrize(
    "query",
    [
        pytest.param(lambda s: s.table("fx.nested"), id="nested types"),
        pytest.param(
            lambda s: s.sql(
                "SELECT a.id, b.vendor, a.amount FROM fx.many a JOIN fx.plain b ON a.id = b.id"
            ),
            id="a join",
        ),
        pytest.param(
            lambda s: s.table("fx.many").groupBy("name").agg(F.sum("qty").alias("s")),
            id="an aggregate",
        ),
        pytest.param(lambda s: s.table("fx.many").orderBy("id").limit(17), id="a limit"),
        pytest.param(
            lambda s: s.table("fx.many").select("id").union(s.table("fx.plain").select("id")),
            id="a union",
        ),
    ],
)
def test_any_distributed_query_can_be_written(
    query: Query, cluster: ClusterSession, oracle: Session
) -> None:
    ours, theirs = unique("query"), unique("query_icetl")
    query(cluster).write.saveAsTable(ours)
    distributed(cluster)
    query(oracle).write.saveAsTable(theirs)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))


# -- partition transforms ------------------------------------------------------------


def _timed_table(catalog: SqlCatalog, name: str, transform: Any, tz: bool) -> None:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "label", StringType(), required=False),
        NestedField(3, "at", TimestamptzType() if tz else TimestampType(), required=False),
    )
    spec = PartitionSpec(PartitionField(3, 1000, transform, "at_part"))
    catalog.create_table(name, schema=schema, partition_spec=spec)


@pytest.mark.parametrize(
    ("transform", "tz"),
    [
        (DayTransform(), False),
        (DayTransform(), True),
        (MonthTransform(), True),
        (YearTransform(), False),
        (HourTransform(), True),
        (IdentityTransform(), False),
    ],
    ids=["day", "day of a timestamptz", "month", "year", "hour", "identity of a timestamp"],
)
def test_time_partitions_match_icetl_s(
    transform: Any, tz: bool, cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours, theirs = unique("timed"), unique("timed_icetl")
    for name in (ours, theirs):
        _timed_table(catalog, name, transform, tz)

    def rows(s: Session) -> DataFrame:
        return s.sql(
            "SELECT id, name AS label, "
            + ("CAST(tstz AS TIMESTAMPTZ)" if tz else "ts")
            + " AS at FROM fx.many WHERE id % 7 = 0"
        )

    rows(cluster).write.insertInto(ours)
    distributed(cluster)
    rows(oracle).write.insertInto(theirs)
    assert_same_rows(read(oracle, ours), read(oracle, theirs))
    assert sorted(str(f.partition) for f in data_files(catalog, ours)) == sorted(
        {str(f.partition) for f in data_files(catalog, theirs)}
    )


# -- what icetl's writer still does ------------------------------------------------------


def test_a_bucket_partition_goes_to_icetl_s_writer(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    ours = unique("bucketed")
    catalog.create_table(
        ours,
        schema=Schema(NestedField(1, "id", LongType(), required=False)),
        partition_spec=PartitionSpec(PartitionField(1, 1000, BucketTransform(4), "id_bucket")),
    )
    cluster.table("fx.many").select("id").write.insertInto(ours)
    single_node(cluster, "bucket")
    assert read(oracle, ours).num_rows == 2000


def test_merge_schema_goes_to_icetl_s_writer(cluster: ClusterSession, oracle: Session) -> None:
    name = unique("merged")
    cluster.table("fx.plain").select("id").write.saveAsTable(name)
    cluster.table("fx.plain").write.mode("append").option("mergeSchema", "true").saveAsTable(name)
    single_node(cluster, "mergeSchema")
    assert read(oracle, name).column_names == ["id", "vendor", "amount"]


def test_fallback_error_refuses_a_single_node_write(fresh_cluster: ClusterSession) -> None:
    fresh_cluster.conf.set("icefleet.fallback", "error")
    name = unique("refused")
    fresh_cluster.table("fx.plain").write.saveAsTable(name)
    with pytest.raises(UnsupportedFeatureError, match="mergeSchema"):
        fresh_cluster.table("fx.plain").write.mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(name)


# -- the rules icetl's writer keeps, kept ------------------------------------------------


def test_the_modes_on_an_existing_table(cluster: ClusterSession, oracle: Session) -> None:
    name = unique("existing")
    cluster.table("fx.plain").write.saveAsTable(name)
    with pytest.raises(TableAlreadyExistsException):
        cluster.table("fx.plain").write.saveAsTable(name)
    cluster.table("fx.plain").write.mode("ignore").saveAsTable(name)
    assert read(oracle, name).num_rows == 5
    with pytest.raises(AnalysisException, match="does not exist"):
        cluster.table("fx.plain").write.insertInto(unique("missing"))


def test_a_frame_that_does_not_fit_is_refused_as_icetl_refuses_it(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    name = unique("strict")
    cluster.table("fx.plain").write.saveAsTable(name)
    before = operations(catalog, name)
    with pytest.raises(AnalysisException):
        cluster.table("fx.plain").select(
            F.col("vendor").alias("id"), "vendor", "amount"
        ).write.mode("append").saveAsTable(name)
    with pytest.raises(AnalysisException, match="position"):
        cluster.table("fx.plain").select("id").write.insertInto(name)
    assert operations(catalog, name) == before


# -- failures leave nothing behind ---------------------------------------------------------


#: A task fails on the last row: a bad CAST would be NULL under the reference's semantics,
#: so DuckDB's error() makes the failure.
FAILING = "CASE WHEN id = 1999 THEN error('boom') ELSE 1 END"


def test_a_failed_save_as_table_leaves_no_table_and_no_files(
    cluster: ClusterSession, catalog: SqlCatalog, warehouse_root: Path
) -> None:
    name = unique("failed")
    query = cluster.sql(f"SELECT id, {FAILING} AS v FROM fx.many")
    with pytest.raises(Exception, match="boom"):
        query.write.saveAsTable(name)
    assert not catalog.table_exists(name)
    leftovers = [p for p in warehouse_root.rglob("*.parquet") if name.split(".")[1] in str(p)]
    assert leftovers == []


def test_a_failed_append_leaves_the_table_as_it_was(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    name = unique("kept")
    cluster.table("fx.plain").write.saveAsTable(name)
    before = operations(catalog, name)
    table_dir = Path(catalog.load_table(name).location().replace("file://", ""))
    files_before = sorted(table_dir.rglob("*.parquet"))
    bad = cluster.sql(
        f"SELECT CAST({FAILING} AS BIGINT) AS id, 'v' AS vendor, CAST(1 AS DOUBLE) AS amount "
        "FROM fx.many"
    )
    with pytest.raises(Exception, match="boom"):
        bad.write.mode("append").saveAsTable(name)
    assert operations(catalog, name) == before
    assert sorted(table_dir.rglob("*.parquet")) == files_before
    assert read(oracle, name).num_rows == 5


def test_icetl_reads_renamed_columns_of_files_icefleet_wrote(
    cluster: ClusterSession, oracle: Session, catalog: SqlCatalog
) -> None:
    """The files carry Iceberg field ids, so a rename is a metadata change only."""
    name = unique("renamed")
    many(cluster).write.saveAsTable(name)
    with catalog.load_table(name).update_schema() as update:
        update.rename_column("amount", "total")
    back = read(oracle, name)
    assert "total" in back.column_names and back.column("total").null_count < back.num_rows
    assert sorted(back.column("total").to_pylist(), key=str) == sorted(
        many(oracle).toArrow().column("amount").to_pylist(), key=str
    )
