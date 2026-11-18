"""Build the fixture tables the suite runs against.

Adapted from icetl's `tests/fixtures/generator.py` (same owner): `plain`, `partitioned`,
`wide`, `nested`, `renamed` and `mor` keep their shapes, so the cases icetl found risky
are covered here too. Added for distribution:

    many     8 files and ten column types: something to cut into splits, and a check that
             every type crosses the result exchange unchanged
    empty    a table with no data files: nothing to distribute
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import (
    POSITIONAL_DELETE_SCHEMA,
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Snapshot
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.table.update import AddSnapshotUpdate, AssertRefSnapshotId, SetSnapshotRefUpdate
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
)

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

__all__ = ["FIXTURE_NAMES", "FixtureTable", "build_all", "local_catalog", "warehouse_uri"]

NAMESPACE = "fx"
WIDE_COLUMN_COUNT = 200
MANY_FILES = 8
MANY_ROWS_PER_FILE = 250

# PyIceberg's snapshot models take hyphenated aliases; going through Any keeps the
# builder readable (icetl's generator explains this in full).
_Snapshot: Any = Snapshot
_AddSnapshot: Any = AddSnapshotUpdate
_SetSnapshotRef: Any = SetSnapshotRefUpdate
_AssertRefSnapshotId: Any = AssertRefSnapshotId


class _DeleteManifestWriter(ManifestWriterV2):
    """A manifest writer that marks its content as DELETES."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES


@dataclass(frozen=True)
class FixtureTable:
    identifier: str
    table: Table
    rows: int
    note: str = ""


def warehouse_uri(root: Path) -> str:
    """`file://` + POSIX path: the spelling PyIceberg accepts on every platform."""
    return f"file://{root.as_posix()}"


def local_catalog(root: Path, name: str = "test") -> SqlCatalog:
    root.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        name,
        **{
            "uri": f"sqlite:///{(root / 'catalog.db').as_posix()}",
            "warehouse": warehouse_uri(root),
        },
    )
    if (NAMESPACE,) not in catalog.list_namespaces():
        catalog.create_namespace(NAMESPACE)
    return catalog


def build_plain(catalog: Catalog) -> FixtureTable:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "vendor", StringType(), required=False),
        NestedField(3, "amount", DoubleType(), required=False),
    )
    table = catalog.create_table(f"{NAMESPACE}.plain", schema=schema)
    table.append(
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5], pa.int64()),
                "vendor": pa.array(["a", "b", "a", "c", None], pa.string()),
                "amount": pa.array([10.0, 20.5, 30.25, None, 50.0], pa.float64()),
            },
            schema=schema.as_arrow(),
        )
    )
    return FixtureTable(f"{NAMESPACE}.plain", table, rows=5)


def build_partitioned(catalog: Catalog) -> FixtureTable:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "as_at_date", StringType(), required=False),
        NestedField(3, "amount", DoubleType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="as_at_date")
    )
    table = catalog.create_table(f"{NAMESPACE}.partitioned", schema=schema, partition_spec=spec)
    for index, day in enumerate(["2026-08-15", "2026-08-16", "2026-08-17"]):
        table.append(
            pa.table(
                {
                    "id": pa.array(range(index * 4, (index + 1) * 4), pa.int64()),
                    "as_at_date": pa.array([day] * 4, pa.string()),
                    "amount": pa.array([float(index * 10 + i) for i in range(4)], pa.float64()),
                },
                schema=schema.as_arrow(),
            )
        )
    return FixtureTable(f"{NAMESPACE}.partitioned", table, rows=12)


def build_wide(catalog: Catalog) -> FixtureTable:
    fields = [NestedField(1, "id", LongType(), required=False)]
    fields += [
        NestedField(i, f"col_{i - 1:03d}", DoubleType(), required=False)
        for i in range(2, WIDE_COLUMN_COUNT + 1)
    ]
    schema = Schema(*fields)
    table = catalog.create_table(f"{NAMESPACE}.wide", schema=schema)
    rows = 500
    columns: dict[str, pa.Array] = {"id": pa.array(range(rows), pa.int64())}
    for i in range(2, WIDE_COLUMN_COUNT + 1):
        columns[f"col_{i - 1:03d}"] = pa.array([float(i * r) for r in range(rows)], pa.float64())
    table.append(pa.table(columns, schema=schema.as_arrow()))
    return FixtureTable(f"{NAMESPACE}.wide", table, rows=rows)


def build_nested(catalog: Catalog) -> FixtureTable:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(
            2,
            "person",
            StructType(
                NestedField(5, "name", StringType(), required=False),
                NestedField(6, "age", LongType(), required=False),
            ),
            required=False,
        ),
        NestedField(
            3, "tags", ListType(element_id=7, element=StringType(), element_required=False),
            required=False,
        ),
        NestedField(
            4,
            "scores",
            MapType(key_id=8, key_type=StringType(), value_id=9, value_type=LongType(),
                    value_required=False),
            required=False,
        ),
    )  # fmt: skip
    table = catalog.create_table(f"{NAMESPACE}.nested", schema=schema)
    table.append(
        pa.table(
            {
                "id": [1, 2],
                "person": [{"name": "ada", "age": 36}, {"name": None, "age": None}],
                "tags": [["x", "y"], []],
                "scores": [[("a", 1)], [("b", 2), ("c", 3)]],
            },
            schema=schema.as_arrow(),
        )
    )
    return FixtureTable(f"{NAMESPACE}.nested", table, rows=2)


def build_renamed(catalog: Catalog) -> FixtureTable:
    """A column renamed between two appends: two file groups, field-id aliasing."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "old_name", StringType(), required=False),
    )
    table = catalog.create_table(f"{NAMESPACE}.renamed", schema=schema)
    table.append(
        pa.table(
            {"id": pa.array([1, 2], pa.int64()), "old_name": pa.array(["before-a", "before-b"])},
            schema=schema.as_arrow(),
        )
    )
    with table.update_schema() as update:
        update.rename_column("old_name", "new_name")
    table.append(
        pa.table(
            {"id": pa.array([3, 4], pa.int64()), "new_name": pa.array(["after-c", "after-d"])},
            schema=table.schema().as_arrow(),
        )
    )
    return FixtureTable(f"{NAMESPACE}.renamed", table, rows=4)


def build_mor(catalog: Catalog) -> FixtureTable:
    """Merge-on-read positional deletes: icetl refuses the scan, and so must icefleet."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "vendor", StringType(), required=False),
        NestedField(3, "amount", DoubleType(), required=False),
    )
    identifier = f"{NAMESPACE}.mor"
    table = catalog.create_table(identifier, schema=schema)
    table.append(
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
                "vendor": pa.array(["a", "b", "c", "d", "e", "f"]),
                "amount": pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], pa.float64()),
            },
            schema=schema.as_arrow(),
        )
    )
    table = catalog.load_table(identifier)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    metadata = cast(TableMetadataV2, table.metadata)
    data_path = next(iter(table.scan().plan_files())).file.file_path
    new_snapshot_id = snapshot.snapshot_id + 1

    deletes = pa.table(
        {"file_path": pa.array([data_path, data_path]), "pos": pa.array([1, 3], pa.int64())},
        schema=POSITIONAL_DELETE_SCHEMA.as_arrow(),
    )
    delete_path = f"{table.location()}/data/positional-deletes-{uuid.uuid4()}.parquet"
    with table.io.new_output(delete_path).create(overwrite=True) as handle:
        pq.write_table(deletes, handle)
    delete_size = len(table.io.new_input(delete_path))

    manifest_path = f"{table.location()}/metadata/{uuid.uuid4()}-deletes.avro"
    with _DeleteManifestWriter(
        spec=table.spec(),
        schema=table.schema(),
        output_file=table.io.new_output(manifest_path),
        snapshot_id=new_snapshot_id,
        avro_compression="null",
    ) as writer:
        writer.add_entry(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=new_snapshot_id,
                data_file=DataFile.from_args(
                    content=DataFileContent.POSITION_DELETES,
                    file_path=delete_path,
                    file_format=FileFormat.PARQUET,
                    partition=Record(),
                    record_count=deletes.num_rows,
                    file_size_in_bytes=delete_size,
                    spec_id=metadata.default_spec_id,
                ),
            )
        )
    delete_manifest = writer.to_manifest_file()

    manifest_list_path = f"{table.location()}/metadata/snap-{new_snapshot_id}-deletes.avro"
    with write_manifest_list(
        format_version=metadata.format_version,
        output_file=table.io.new_output(manifest_list_path),
        snapshot_id=new_snapshot_id,
        parent_snapshot_id=snapshot.snapshot_id,
        sequence_number=metadata.last_sequence_number + 1,
        avro_compression="null",
    ) as manifest_list:
        manifest_list.add_manifests([*snapshot.manifests(table.io), delete_manifest])

    catalog.commit_table(
        table,
        (_AssertRefSnapshotId(snapshot_id=snapshot.snapshot_id, ref="main"),),
        (
            _AddSnapshot(
                snapshot=_Snapshot(
                    snapshot_id=new_snapshot_id,
                    parent_snapshot_id=snapshot.snapshot_id,
                    sequence_number=metadata.last_sequence_number + 1,
                    timestamp_ms=snapshot.timestamp_ms + 1,
                    manifest_list=manifest_list_path,
                    summary=Summary(operation=Operation.OVERWRITE, **{"total-data-files": "1"}),
                    schema_id=table.schema().schema_id,
                )
            ),
            _SetSnapshotRef(ref_name="main", type="branch", snapshot_id=new_snapshot_id),
        ),
    )
    return FixtureTable(identifier, catalog.load_table(identifier), rows=4)


def _many_rows(file_index: int) -> dict[str, list[Any]]:
    """Deterministic rows for one file of `many`, with a NULL in every column somewhere."""
    base = file_index * MANY_ROWS_PER_FILE
    start = datetime(2026, 1, 1, 8, 30)
    rows: dict[str, list[Any]] = {k: [] for k in MANY_COLUMNS}
    for i in range(MANY_ROWS_PER_FILE):
        n = base + i
        missing = n % 37 == 0
        rows["id"].append(n)
        rows["name"].append(None if missing else f"name-{n % 53:02d}")
        rows["amount"].append(None if missing else round(n * 1.25 - 300.5, 2))
        rows["qty"].append(None if missing else (n * 7) % 101 - 50)
        rows["price"].append(None if missing else Decimal(f"{(n * 13) % 10000}.{n % 100:02d}"))
        rows["flag"].append(None if missing else n % 3 == 0)
        rows["day"].append(None if missing else date(2026, 1, 1) + timedelta(days=n % 400))
        rows["ts"].append(None if missing else start + timedelta(minutes=17 * n))
        rows["tstz"].append(
            None if missing else (start + timedelta(seconds=611 * n)).replace(tzinfo=UTC)
        )
        rows["raw"].append(None if missing else bytes([n % 256, (n * 3) % 256]))
    return rows


MANY_COLUMNS = ("id", "name", "amount", "qty", "price", "flag", "day", "ts", "tstz", "raw")


def build_many(catalog: Catalog) -> FixtureTable:
    """Eight appends -> eight files, so a scan has something to split."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "amount", DoubleType(), required=False),
        NestedField(4, "qty", IntegerType(), required=False),
        NestedField(5, "price", DecimalType(10, 2), required=False),
        NestedField(6, "flag", BooleanType(), required=False),
        NestedField(7, "day", DateType(), required=False),
        NestedField(8, "ts", TimestampType(), required=False),
        NestedField(9, "tstz", TimestamptzType(), required=False),
        NestedField(10, "raw", BinaryType(), required=False),
    )
    table = catalog.create_table(f"{NAMESPACE}.many", schema=schema)
    for index in range(MANY_FILES):
        table.append(pa.table(_many_rows(index), schema=schema.as_arrow()))
    return FixtureTable(f"{NAMESPACE}.many", table, rows=MANY_FILES * MANY_ROWS_PER_FILE)


def build_empty(catalog: Catalog) -> FixtureTable:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
    )
    table = catalog.create_table(f"{NAMESPACE}.empty", schema=schema)
    return FixtureTable(f"{NAMESPACE}.empty", table, rows=0)


def _two_files(
    catalog: Catalog, name: str, schema: Schema, rows: list[dict[str, Any]]
) -> FixtureTable:
    """`rows` appended in two halves -- two files, so a window over them distributes."""
    table = catalog.create_table(f"{NAMESPACE}.{name}", schema=schema)
    half = len(rows) // 2
    for chunk in (rows[:half], rows[half:]):
        table.append(pa.Table.from_pylist(chunk, schema=schema.as_arrow()))
    return FixtureTable(f"{NAMESPACE}.{name}", table, rows=len(rows))


def build_ties(catalog: Catalog) -> FixtureTable:
    """icetl's `ties` frame (tests/fixture/test_window.py): x ties at 20, so RANGE and
    ROWS frames disagree."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "x", DoubleType(), required=False),
    )
    rows: list[dict[str, Any]] = [
        {"id": 1, "x": 10.0},
        {"id": 2, "x": 20.0},
        {"id": 3, "x": 20.0},
        {"id": 4, "x": 40.0},
    ]
    return _two_files(catalog, "ties", schema, rows)


def build_gaps(catalog: Catalog) -> FixtureTable:
    """icetl's `gaps` frame: NULLs in the value column, for ignoreNulls and defaults."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "v", StringType(), required=False),
    )
    rows: list[dict[str, Any]] = [
        {"id": 1, "v": None},
        {"id": 2, "v": "b"},
        {"id": 3, "v": None},
        {"id": 4, "v": "d"},
    ]
    return _two_files(catalog, "gaps", schema, rows)


def build_groups(catalog: Catalog) -> FixtureTable:
    """icetl's per-partition frames, in one table: g a/a/b with x 1/2/1, y 1.0/2.0/5.0."""
    schema = Schema(
        NestedField(1, "g", StringType(), required=False),
        NestedField(2, "x", LongType(), required=False),
        NestedField(3, "y", DoubleType(), required=False),
    )
    rows: list[dict[str, Any]] = [
        {"g": "a", "x": 1, "y": 1.0},
        {"g": "a", "x": 2, "y": 2.0},
        {"g": "b", "x": 1, "y": 5.0},
    ]
    return _two_files(catalog, "groups", schema, rows)


BUILDERS = {
    "plain": build_plain,
    "partitioned": build_partitioned,
    "wide": build_wide,
    "nested": build_nested,
    "renamed": build_renamed,
    "mor": build_mor,
    "many": build_many,
    "empty": build_empty,
    "ties": build_ties,
    "gaps": build_gaps,
    "groups": build_groups,
}
FIXTURE_NAMES = tuple(BUILDERS)


def build_all(catalog: Catalog) -> dict[str, FixtureTable]:
    return {name: builder(catalog) for name, builder in BUILDERS.items()}
