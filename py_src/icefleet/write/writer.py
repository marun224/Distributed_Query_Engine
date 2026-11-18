"""`df.write` on a `ClusterSession`: the workers write the data files, the coordinator
commits them once (PLAN §4.6, Phase 5).

`ClusterDataFrameWriter` is icetl's `DataFrameWriter` with one method replaced, `_save`,
which every write goes through -- `saveAsTable`, `insertInto`, SQL `INSERT INTO` /
`INSERT OVERWRITE` and `CREATE TABLE ... AS` alike. It keeps icetl's rules (the modes, the
errors, `insertInto` by position, `partitionBy` / `sortBy` / table properties on a table it
creates, PyIceberg's own schema check) and changes only where the rows go: instead of one
Arrow table materialised on the coordinator and appended, every final task writes its
share as Iceberg data files and the coordinator adds them in one transaction.

What it cannot distribute it hands to icetl's writer unchanged -- `mergeSchema`, a query
the planner cannot cut, a partition transform DuckDB cannot compute (`bucket`,
`truncate`), a column type it does not write, a dynamic overwrite of a table with a
non-identity partition field -- and says why in the log (`icefleet.fallback=error`
refuses instead, as for reads).

On failure nothing is left behind: the files of a write that did not commit are deleted,
and a table `saveAsTable` created for it is dropped again (kickoff answer 5).
"""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING, Any

from icetl.errors import AnalysisException, QueryExecutionException, UnsupportedFeatureError
from icetl.sql.writer import DataFrameWriter, iceberg_ready

from icefleet.errors import Uncuttable
from icefleet.write.commit import commit_files
from icefleet.write.target import WriteTarget, iceberg_path, write_target

if TYPE_CHECKING:
    from icetl.catalog.resolver import TableRef
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

    from icefleet.session import ClusterSession

__all__ = ["ClusterDataFrameWriter"]

logger = logging.getLogger(__name__)


class _SingleNode(Exception):
    """This write goes to icetl's own writer, for `reason`."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class ClusterDataFrameWriter(DataFrameWriter):
    """icetl's writer, writing from the workers."""

    def _derive(self, **changes: Any) -> ClusterDataFrameWriter:
        settings: dict[str, Any] = {
            "mode": self._mode,
            "options": self._options,
            "partition_by": self._partition_by,
            "sort_by": self._sort_by,
            "properties": self._properties,
        }
        settings.update(changes)
        return ClusterDataFrameWriter(self._df, **settings)

    @property
    def _session(self) -> ClusterSession:
        return self._df._session  # type: ignore[return-value]

    # -- the one override ----------------------------------------------------------

    def _save(self, name: str, *, insert_into: bool, overwrite: bool) -> None:
        try:
            self._save_distributed(name, insert_into=insert_into, overwrite=overwrite)
        except _SingleNode as single:
            self._session._write_single_node(single.reason)
            super()._save(name, insert_into=insert_into, overwrite=overwrite)

    def _save_distributed(self, name: str, *, insert_into: bool, overwrite: bool) -> None:
        if not isinstance(name, str):
            raise _SingleNode("not a table name")  # icetl raises the right error
        if self._merges_schema():
            raise _SingleNode("mergeSchema widens the table first")
        session = self._session
        reason = session._write_reason(self._df)
        if reason is not None:
            raise _SingleNode(reason)

        ref = session._resolver.parse(name)
        if not ref.namespace:
            raise _SingleNode("a table name without a namespace")  # icetl's error
        catalog = session._registry.get(ref.catalog)
        if not catalog.table_exists(ref.identifier):
            if insert_into:
                raise _SingleNode("insertInto a table that does not exist")  # icetl's error
            self._create_and_fill_distributed(catalog, ref, name)
            return
        if not insert_into and self._mode in ("error", "ignore"):
            raise _SingleNode(f"mode {self._mode!r} on an existing table")  # icetl decides

        table = catalog.load_table(ref.identifier)
        mode = "append"
        if overwrite:
            mode = "dynamic" if self._dynamic_partitions() else "overwrite"
        if mode == "dynamic" and not table.spec().fields:
            raise _SingleNode("a dynamic overwrite of an unpartitioned table")  # icetl's error
        target = self._target(table, name, positional=insert_into)
        if mode == "dynamic" and target.transformed:
            raise _SingleNode("a dynamic overwrite of a table with a non-identity partition field")
        self._write(catalog, ref, table, target, mode)
        session._invalidate_source(ref)

    # -- creating a table ------------------------------------------------------------

    def _create_and_fill_distributed(self, catalog: Catalog, ref: TableRef, name: str) -> None:
        """icetl's `_create_and_fill`, with the schema from a zero-row run of the plan
        (the same Arrow types DuckDB would hand icetl) and the rows from the workers."""
        session = self._session
        empty = iceberg_ready(session._empty_result(self._df))
        with contextlib.suppress(Exception):
            catalog.create_namespace_if_not_exists(ref.namespace)
        try:
            table = catalog.create_table(
                ref.identifier, schema=empty.schema, properties=self._properties
            )
        except Exception as exc:
            raise QueryExecutionException(f"Could not create table {name!r}: {exc}") from exc
        try:
            if self._partition_by:
                self._apply_partitioning(table, name)
            if self._sort_by:
                self._apply_sort_order(table, name)
            table = catalog.load_table(ref.identifier)
            target = write_target(table, list(empty.schema.names), positional=False)
            if isinstance(target, str):
                # icetl's writer creates it its own way: undo ours, hand it over.
                catalog.drop_table(ref.identifier)
                raise _SingleNode(target)
            self._write(catalog, ref, table, target, "append")
        except _SingleNode:
            raise
        except BaseException:
            with contextlib.suppress(Exception):
                catalog.drop_table(ref.identifier)  # it did not exist before this write
            raise
        session._invalidate_source(ref)

    # -- the write -------------------------------------------------------------------

    def _target(self, table: Table, name: str, *, positional: bool) -> WriteTarget:
        """The table as a write target, after PyIceberg's own check that the frame fits
        it -- so exactly what icetl's append accepts is accepted, and nothing more."""
        from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible

        provided = iceberg_ready(self._session._empty_result(self._df))
        expected = [field.name for field in table.schema().fields]
        if positional:
            if len(provided.schema.names) != len(expected):
                raise _SingleNode("a column count that does not match")  # icetl's error
            provided = provided.rename_columns(expected)
        try:
            _check_pyarrow_schema_compatible(
                table.schema(),
                provided.schema,
                downcast_ns_timestamp_to_us=True,
                format_version=table.format_version,
            )
        except ValueError as exc:
            raise AnalysisException(str(exc)) from exc
        except UnicodeEncodeError as exc:  # pragma: no cover - a cp1252 console (icetl's note)
            raise AnalysisException(
                "The data does not match the table's schema (PyIceberg's report could not "
                f"be printed on this console: {exc})."
            ) from exc
        target = write_target(table, list(self._df.columns), positional=positional)
        if isinstance(target, str):
            raise _SingleNode(target)
        return target

    def _write(
        self, catalog: Catalog, ref: TableRef, table: Table, target: WriteTarget, mode: str
    ) -> None:
        session = self._session
        written = session._write_files(self._df, target)
        if isinstance(written, Uncuttable):
            raise _SingleNode(written.reason)
        files, discard = written
        try:
            paths = [iceberg_path(f.path, table.location()) for f in files]
            partitions: list[dict[str, Any]] = []
            if mode == "dynamic":
                partitions = session._partitions_written(files, target.identity)
            commit_files(
                lambda: catalog.load_table(ref.identifier), paths, mode=mode, partitions=partitions
            )
        except BaseException:
            discard()
            raise
        logger.info(
            "wrote %d file(s), %d row(s) into %s (%s)",
            len(files),
            sum(f.rows for f in files),
            ".".join(ref.identifier),
            mode,
        )


def refuse(reason: str) -> UnsupportedFeatureError:
    """`icefleet.fallback=error`'s answer to a write that would run single-node."""
    return UnsupportedFeatureError(
        f"Distributing this write ({reason})",
        hint=(
            "icefleet.fallback=error refuses to write it from one machine; set "
            "icefleet.fallback=single-node to let icetl's writer do it"
        ),
    )
