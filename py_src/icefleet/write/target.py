"""What a distributed write needs to know about the table it writes into (PLAN §4.6).

Every worker writes its rows with one DuckDB statement,

    COPY (SELECT CAST(c AS <the table's type>) AS c, ..., <partition values> FROM ...)
    TO '<table>/data/icefleet/<query>/stage-<k>/task-<i>'
    (FORMAT parquet, FIELD_IDS {...}, PARTITION_BY (...) | FILE_SIZE_BYTES n, RETURN_STATS true)

and the coordinator hands the files to PyIceberg's `add_files`, which reads each footer for
the statistics and the partition value. So this module turns a table into three things:

- **the columns**, each cast to the DuckDB spelling of its Iceberg type, read from the
  frame by name (`saveAsTable`) or by position (`insertInto`); a table column the frame
  lacks is written NULL, as PyIceberg's own append would;
- **the field ids**, from the table's *own* schema -- PyIceberg renumbers nested fields
  when it creates a table, so nothing else is to be trusted -- in DuckDB's FIELD_IDS form,
  `{c: 1, s: {__duckdb_field_id: 2, a: 5}, l: {__duckdb_field_id: 3, element: 6}}`;
- **a grouping value per partition field**, which only decides which rows share a file:
  `hash(c)` for an identity field, the Iceberg ordinal (years / months / days / hours
  since 1970, in UTC) for a time transform. `add_files` computes the real partition value
  from each file and refuses a file holding two, so a wrong grouping fails loudly rather
  than landing rows in the wrong partition.

A table this cannot write -- a `bucket` / `truncate` partition field, a column type DuckDB
does not write the way Iceberg reads it (`time`, `uuid`, `fixed`, nanosecond timestamps)
-- gets a reason instead, and icetl's own writer runs (kickoff answer 2).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from icetl.paths import engine_path
from sqlglot import exp

if TYPE_CHECKING:
    from pyiceberg.table import Table

__all__ = ["WriteTarget", "duckdb_type", "field_ids_of", "iceberg_path", "write_target"]

#: Iceberg's own default for `write.target-file-size-bytes`.
DEFAULT_TARGET_FILE_BYTES = 512 * 1024**2
PARTITION_PREFIX = "__icefleet_p"


def _quote(name: str) -> str:
    return exp.to_identifier(name, quoted=True).sql(dialect="duckdb")


@dataclass(frozen=True)
class WriteTarget:
    """A table, as the stages that write into it need it."""

    columns: tuple[tuple[str, str], ...]  # (table column, DuckDB type), in table order
    sources: tuple[str | None, ...]  # the frame column each is read from; None: NULL
    field_ids: str  # DuckDB's FIELD_IDS struct literal
    partitions: tuple[str, ...]  # SQL over the table columns: one grouping value per field
    identity: tuple[str, ...]  # the table columns of the identity partition fields
    transformed: bool  # some partition field is not identity (no dynamic overwrite)
    location: str  # where the tasks write: <table location>/data/icefleet, as DuckDB spells it
    target_file_bytes: int
    #: Shuffle a partitioned table's rows by their partition values before writing. A
    #: copy-on-write rewrite whose rows cannot change partition turns it off (Phase 5b):
    #: each task then reads whole files of one partition and writes a file per value.
    shuffle: bool = True

    @property
    def hidden(self) -> tuple[str, ...]:
        """The grouping columns' names: computed, partitioned on, never written."""
        return tuple(f"{PARTITION_PREFIX}{i}" for i in range(len(self.partitions)))


def duckdb_type(field_type: Any) -> str | None:
    """The DuckDB type an Iceberg type is written as, or None when there is none that
    PyIceberg reads back as the same type."""
    from pyiceberg import types as t

    simple = {
        t.BooleanType: "BOOLEAN",
        t.IntegerType: "INTEGER",
        t.LongType: "BIGINT",
        t.FloatType: "FLOAT",
        t.DoubleType: "DOUBLE",
        t.DateType: "DATE",
        t.TimestampType: "TIMESTAMP",
        t.TimestamptzType: "TIMESTAMPTZ",
        t.StringType: "VARCHAR",
        t.BinaryType: "BLOB",
    }
    for kind, name in simple.items():
        if type(field_type) is kind:
            return name
    if isinstance(field_type, t.DecimalType):
        return f"DECIMAL({field_type.precision},{field_type.scale})"
    if isinstance(field_type, t.StructType):
        members = []
        for nested in field_type.fields:
            inner = duckdb_type(nested.field_type)
            if inner is None:
                return None
            members.append(f"{_quote(nested.name)} {inner}")
        return f"STRUCT({', '.join(members)})"
    if isinstance(field_type, t.ListType):
        element = duckdb_type(field_type.element_type)
        return None if element is None else f"{element}[]"
    if isinstance(field_type, t.MapType):
        key, value = duckdb_type(field_type.key_type), duckdb_type(field_type.value_type)
        return None if key is None or value is None else f"MAP({key}, {value})"
    return None


def _ids(field_type: Any, field_id: int) -> str:
    from pyiceberg import types as t

    if isinstance(field_type, t.StructType):
        inner = ", ".join(
            f"{_quote(f.name)}: {_ids(f.field_type, f.field_id)}" for f in field_type.fields
        )
        return f"{{__duckdb_field_id: {field_id}, {inner}}}"
    if isinstance(field_type, t.ListType):
        element = _ids(field_type.element_type, field_type.element_id)
        return f"{{__duckdb_field_id: {field_id}, element: {element}}}"
    if isinstance(field_type, t.MapType):
        key = _ids(field_type.key_type, field_type.key_id)
        value = _ids(field_type.value_type, field_type.value_id)
        return f"{{__duckdb_field_id: {field_id}, key: {key}, value: {value}}}"
    return str(field_id)


def field_ids_of(schema: Any) -> str:
    """DuckDB's FIELD_IDS for every column of an Iceberg schema, nested fields included."""
    return (
        "{"
        + ", ".join(f"{_quote(f.name)}: {_ids(f.field_type, f.field_id)}" for f in schema.fields)
        + "}"
    )


def _partition_value(transform: Any, column: str, field_type: Any) -> str | None:
    """SQL over the table's column grouping rows exactly as `transform` does, or None."""
    from pyiceberg import transforms as tr
    from pyiceberg import types as t

    name = _quote(column)
    if isinstance(transform, tr.IdentityTransform):
        return f"hash({name})"
    if isinstance(transform, tr.VoidTransform):
        return "0"
    if not isinstance(
        transform, (tr.YearTransform, tr.MonthTransform, tr.DayTransform, tr.HourTransform)
    ):
        return None
    if isinstance(field_type, t.TimestamptzType):
        value = f"timezone('UTC', {name})"  # Iceberg's transforms of a timestamptz are in UTC
    elif isinstance(field_type, (t.TimestampType, t.DateType)):
        value = name
    else:
        return None
    if isinstance(transform, tr.YearTransform):
        return f"(year({value}) - 1970)"
    if isinstance(transform, tr.MonthTransform):
        return f"((year({value}) - 1970) * 12 + month({value}) - 1)"
    if isinstance(transform, tr.DayTransform):
        return f"datediff('day', DATE '1970-01-01', CAST({value} AS DATE))"
    if isinstance(field_type, t.DateType):
        return None
    return (
        f"datediff('hour', TIMESTAMP '1970-01-01', date_trunc('hour', CAST({value} AS TIMESTAMP)))"
    )


def write_target(
    table: Table, frame_columns: Sequence[str], *, positional: bool
) -> WriteTarget | str:
    """The write target for `table`, or the reason the distributed writer cannot write it.

    `frame_columns` are the result's columns; by position they are renamed onto the
    table's, which is what `insertInto` means. (That they fit -- the count, the names,
    the types -- is PyIceberg's own check, run by the caller before this.)
    """
    schema = table.schema()
    columns: list[tuple[str, str]] = []
    for field in schema.fields:
        kind = duckdb_type(field.field_type)
        if kind is None:
            return (
                f"a column the distributed writer does not write ({field.name} {field.field_type})"
            )
        columns.append((field.name, kind))

    names = [f.name for f in schema.fields]
    if positional:
        sources: list[str | None] = list(frame_columns)
    else:
        lowered = {c.lower(): c for c in frame_columns}
        sources = [lowered.get(name.lower()) for name in names]

    partitions: list[str] = []
    identity: list[str] = []
    transformed = False
    by_id = {f.field_id: f for f in schema.fields}
    for spec_field in table.spec().fields:
        source = by_id.get(spec_field.source_id)
        if source is None:
            return f"a partition field over a nested column ({spec_field.name})"
        value = _partition_value(spec_field.transform, source.name, source.field_type)
        if value is None:
            return (
                f"a {spec_field.transform} partition field ({spec_field.name}), "
                "which icetl's writer handles"
            )
        partitions.append(value)
        from pyiceberg.transforms import IdentityTransform

        if isinstance(spec_field.transform, IdentityTransform):
            identity.append(source.name)
        else:
            transformed = True

    target_bytes = int(
        table.properties.get("write.target-file-size-bytes", DEFAULT_TARGET_FILE_BYTES)
    )
    return WriteTarget(
        columns=tuple(columns),
        sources=tuple(sources),
        field_ids=field_ids_of(schema),
        partitions=tuple(partitions),
        identity=tuple(identity),
        transformed=transformed,
        location=engine_path(f"{table.location().rstrip('/')}/data/icefleet"),
        target_file_bytes=target_bytes,
    )


def iceberg_path(written: str, table_location: str) -> str:
    """A path DuckDB reported writing, as the table's FileIO spells it (`file://` for a
    local table, unchanged for an object store)."""
    if "://" in written or not table_location.startswith("file:"):
        return written
    return "file://" + written
