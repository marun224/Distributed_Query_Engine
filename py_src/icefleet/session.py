"""`ClusterSession` -- icetl's `Session`, executed by a fleet of DuckDB workers.

Everything a user touches is inherited: `DataFrame`, `Column`, `functions`, `sql()`,
the catalog. Only execution changes (PLAN §3.2):

    icetl compiles the plan unchanged (conformance, binding, optimizer, pushdown, scans)
    -> plan_stages() cuts it, or says why it cannot (Uncuttable)
    -> cuttable:   the scheduler runs the stages on the workers, each fully before the
                   next; the coordinator's own DuckDB reads the final stage's files,
                   casting stand-in types back and applying ORDER BY / LIMIT; the query's
                   exchange prefix is deleted
    -> uncuttable: icetl runs it single-node, on this process (P3), unless
                   icefleet.fallback=error

Writes (Phase 5) go through `df.write`, which on a ClusterSession is a
`ClusterDataFrameWriter` (`write/writer.py`): the workers write the data files, this
process commits them once. `DELETE` / `UPDATE` / `MERGE` (Phase 5b) go through
`_row_level` to `write/rowlevel.py`: the workers rewrite the files holding a row the
statement changes, this process swaps them in one snapshot.

The seams are icetl's private `_execute`, `_stream`, `_compile`, `_row_level`,
`DataFrame._explain_text`, `DataFrame.write` and `DataFrameWriter._save` / `_derive`, and
icetl's row-level helpers, which is why icetl is pinned exactly (PLAN §4.10) and
`py_test/test_seams.py` checks every one of them.
"""

from __future__ import annotations

import logging
import re
import shutil
import tempfile
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from icetl.conf import resolve_settings
from icetl.errors import UnsupportedFeatureError
from icetl.plan.builder import substitute_sources
from icetl.sql.dataframe import DataFrame
from icetl.sql.session import Session
from sqlglot import exp

from icefleet.cluster import Cluster, connect, parse_master
from icefleet.config import FleetSettings, resolve_fleet_settings
from icefleet.errors import ClusterConfigurationError, ClusterError, Uncuttable
from icefleet.exchange import Exchange, open_exchange
from icefleet.plan.describe import describe_plan
from icefleet.plan.stages import StagePlan, plan_stages
from icefleet.plan.types import cast_to
from icefleet.scheduler import ThreadPoolScheduler

if TYPE_CHECKING:
    import pyarrow as pa
    from icetl.conf import IcetlConf, IcetlSettings
    from icetl.plan.builder import ScanSource
    from pyiceberg.catalog import Catalog

    from icefleet.plan.tasks import OutputFile
    from icefleet.write.target import WriteTarget

__all__ = ["ClusterSession"]

logger = logging.getLogger(__name__)

_RESULT_PARAMETER = "icefleet_result_files"
_FILE_NUMBER = re.compile(r"(\d+)(?=\.parquet$)")


def _file_order(path: str) -> tuple[int, str]:
    """`data_10.parquet` after `data_9.parquet`: a task's files in the order written."""
    match = _FILE_NUMBER.search(path)
    return (int(match.group(1)) if match else -1, path)


@dataclass(frozen=True)
class QueryRun:
    """What the last action did. Private: tests read it to catch a plan that quietly
    stopped distributing, which the answer alone would never show."""

    mode: str  # "distributed" | "single-node" | "metadata"
    reason: str | None = None
    query_id: str | None = None
    stages: int = 0
    tasks: int = 0  # across every stage
    stage_tasks: tuple[int, ...] = ()  # per stage, in order
    files: int = 0  # the final stage's result files
    rows: int = 0


class ClusterSession(Session):
    """An icetl `Session` whose queries run on a fleet of DuckDB workers."""

    class Builder(Session.Builder):
        """`ClusterSession.builder.config("icefleet.master", "local[4]").getOrCreate()`."""

        def getOrCreate(self) -> ClusterSession:
            with Session._active_lock:
                active = Session._active
                if isinstance(active, ClusterSession) and not active._stopped:
                    active._conf.setAll(dict(self._conf.getAll()))
                    return active
                session = ClusterSession(
                    settings=resolve_settings(self._conf), conf=self._conf, catalog=self._catalog
                )
                Session._active = session
                return session

        def create(self) -> ClusterSession:
            session = ClusterSession(
                settings=resolve_settings(self._conf), conf=self._conf, catalog=self._catalog
            )
            with Session._active_lock:
                Session._active = session
            return session

    builder = Builder()

    def __init__(
        self,
        settings: IcetlSettings | None = None,
        conf: IcetlConf | None = None,
        *,
        catalog: Catalog | None = None,
    ) -> None:
        super().__init__(settings, conf, catalog=catalog)
        self._fleet_lock = threading.RLock()
        self._cluster: Cluster | None = None
        self._exchange: Exchange | None = None
        self._owned_exchange_dir: str | None = None
        self._last_run: QueryRun | None = None
        # How the last write ran: icetl's own writer reads its frame with `toArrow()`,
        # which sets `_last_run` to that read, so a write keeps its own record.
        self._last_write: QueryRun | None = None
        self._aggregate_names: frozenset[str] | None = None
        # Scan key -> the only data files it may read, while a row-level statement
        # plans its rewrite (Phase 5b, `_pinning`).
        self._pinned: dict[str, frozenset[str]] = {}

    def __repr__(self) -> str:
        name = self._conf.get("icetl.appName", "icetl")
        return f"<ClusterSession app={name!r} master={self._fleet().master!r}>"

    # -- the fleet ---------------------------------------------------------

    def _fleet(self) -> FleetSettings:
        """Re-resolved on every call: `session.conf.set("icefleet.fallback", ...)` applies
        at once. The master and the exchange are fixed at first use, until `stop()`."""
        return resolve_fleet_settings(self._conf)

    def _ensure_cluster(self) -> Cluster:
        with self._fleet_lock:
            if self._cluster is None:
                fleet = self._fleet()
                self._cluster = connect(parse_master(fleet.master), fleet)
            return self._cluster

    def _cluster_shape(self) -> tuple[str, int]:
        """(description, total slots), without launching `local[N]` just to plan a query."""
        if self._cluster is not None:
            return self._cluster.summary(), self._cluster.total_slots
        fleet = self._fleet()
        master = parse_master(fleet.master)
        if master.kind == "local":
            slots = master.workers * fleet.worker_slots
            return (
                f"{master.text} · {master.workers} workers x {fleet.worker_slots} slots",
                slots,
            )
        cluster = self._ensure_cluster()
        return cluster.summary(), cluster.total_slots

    def _ensure_exchange(self) -> Exchange:
        with self._fleet_lock:
            if self._exchange is None:
                fleet = self._fleet()
                location = fleet.exchange_location
                if location is None:
                    if parse_master(fleet.master).kind != "local":
                        raise ClusterConfigurationError(
                            "icefleet.exchange.location must be set when the workers are not "
                            "local: an s3://bucket/prefix (or a directory every worker shares)."
                        )
                    location = tempfile.mkdtemp(prefix="icefleet-exchange-")
                    self._owned_exchange_dir = location
                self._exchange = open_exchange(location, self._settings.s3)
            return self._exchange

    def _describe_exchange(self) -> str:
        if self._exchange is not None:
            return self._exchange.describe()
        return self._fleet().exchange_location or "a temporary directory, created at first use"

    # -- planning ----------------------------------------------------------

    def _describer(
        self, sources: Mapping[str, ScanSource]
    ) -> Callable[[exp.Expression], list[tuple[str, str]]]:
        """Types any SELECT over `sources` with DuckDB, against icetl's zero-row views."""

        def describe(tree: exp.Expression) -> list[tuple[str, str]]:
            bound = substitute_sources(
                tree, sources, lambda source: exp.to_identifier(source.view, quoted=True)
            )
            sql = f"DESCRIBE SELECT * FROM ({bound.sql(dialect='duckdb')}) AS icefleet_analysis"
            rows = self._analyzer.connection.execute(sql).fetchall()
            return [(str(row[0]), str(row[1])) for row in rows]

        return describe

    def _duckdb_aggregates(self) -> frozenset[str]:
        """Every aggregate function name DuckDB knows, so none is taken for row-wise work."""
        if self._aggregate_names is None:
            rows = self._analyzer.connection.execute(
                "SELECT DISTINCT lower(function_name) FROM duckdb_functions() "
                "WHERE function_type = 'aggregate'"
            ).fetchall()
            self._aggregate_names = frozenset(str(row[0]) for row in rows)
        return self._aggregate_names

    def _stage_plan(
        self,
        plan: exp.Expression,
        sources: Mapping[str, ScanSource],
        output_names: Sequence[str],
        *,
        write: WriteTarget | None = None,
        probe: str | None = None,
    ) -> StagePlan | Uncuttable:
        compiled = self._compile(plan, sources, output_names)
        _, slots = self._cluster_shape()
        udfs = list(self._udf.registered) if self._udf is not None else []
        self._analyze(plan, sources)  # registers the zero-row views; raises like icetl would
        return plan_stages(
            compiled,
            sources,
            output_names=list(output_names),
            fleet=self._fleet(),
            min_splits=slots,
            describe=self._describer(sources),
            register=self._engine.register,
            udf_names=udfs,
            aggregate_names=self._duckdb_aggregates(),
            write=write,
            restrict=self._pinned,
            probe=probe,
        )

    def _describe_distributed(
        self,
        plan: exp.Expression,
        sources: Mapping[str, ScanSource],
        output_names: Sequence[str],
    ) -> str:
        """The `== Distributed Plan ==` section of `explain()`."""
        if self._count_from_metadata(plan, sources, output_names) is not None:
            return describe_plan(
                None,
                cluster="",
                exchange="",
                note="an unfiltered count(*), answered from Iceberg's manifests",
            )
        staged = self._stage_plan(plan, sources, output_names)
        cluster, _ = self._cluster_shape()
        return describe_plan(staged, cluster=cluster, exchange=self._describe_exchange())

    def _single_node(self, staged: Uncuttable) -> None:
        if self._fleet().fallback == "error":
            raise UnsupportedFeatureError(
                f"Distributing this plan ({staged.reason})",
                hint=(
                    "icefleet.fallback=error refuses to run it on one machine; set "
                    "icefleet.fallback=single-node to let icetl run it on the coordinator"
                ),
            )
        logger.info("single-node: %s", staged.reason)
        self._last_run = QueryRun(mode="single-node", reason=staged.reason)

    # -- execution: icetl's seams ---------------------------------------------

    def _execute(
        self,
        plan: exp.Expression,
        sources: Mapping[str, ScanSource],
        output_names: Sequence[str],
    ) -> pa.Table:
        if self._stopped:
            raise UnsupportedFeatureError("Using a Session after stop()")
        counted = self._count_from_metadata(plan, sources, output_names)
        if counted is not None:
            self._last_run = QueryRun(mode="metadata")
            return counted
        staged = self._stage_plan(plan, sources, output_names)
        if isinstance(staged, Uncuttable):
            self._single_node(staged)
            return super()._execute(plan, sources, output_names)
        with self._distributed(staged) as files:
            sql, parameters = self._result_query(files, staged)
            return self._engine.arrow(sql, parameters)

    def _stream(
        self,
        plan: exp.Expression,
        sources: Mapping[str, ScanSource],
        output_names: Sequence[str],
        *,
        batch_size: int,
    ) -> Iterator[pa.RecordBatch]:
        if self._stopped:
            raise UnsupportedFeatureError("Using a Session after stop()")
        counted = self._count_from_metadata(plan, sources, output_names)
        if counted is not None:
            self._last_run = QueryRun(mode="metadata")
            yield from counted.to_batches()
            return
        staged = self._stage_plan(plan, sources, output_names)
        if isinstance(staged, Uncuttable):
            self._single_node(staged)
            yield from super()._stream(plan, sources, output_names, batch_size=batch_size)
            return
        with self._distributed(staged) as files:
            sql, parameters = self._result_query(files, staged)
            yield from self._engine.record_batches(sql, parameters, batch_size=batch_size)

    @contextmanager
    def _distributed(self, staged: StagePlan) -> Iterator[list[OutputFile]]:
        """Run `staged` on the fleet; yield the final stage's files; always clean up."""
        fleet = self._fleet()
        cluster = self._ensure_cluster()
        exchange = self._ensure_exchange()
        query_id = f"q-{uuid.uuid4().hex[:12]}"
        scheduler = ThreadPoolScheduler(
            cluster, target_file_bytes=fleet.result_file_bytes, fetch=self._fetch
        )
        try:
            results = scheduler.run(staged, query_id=query_id, exchange=exchange)
            # The final stages' files, stage by stage, task by task, each task's in the
            # order written: for a range sort, that order is the answer's.
            files = [
                f
                for stage_id in staged.finals
                for r in sorted(
                    (r for r in results if r.stage_id == stage_id), key=lambda r: r.task_index
                )
                for f in sorted(r.data, key=lambda f: _file_order(f.path))
            ]
            if not files:
                raise ClusterError(f"query {query_id}: the final stage reported no output files")
            size = sum(f.bytes for f in files)
            if size > fleet.result_max_bytes:
                raise ClusterError(
                    f"query {query_id}: the result is {size} bytes of parquet, over "
                    f"icefleet.result.maxBytes ({fleet.result_max_bytes}). Write it to a table "
                    f"instead (Phase 5), or raise the limit if the coordinator can hold it."
                )
            self._last_run = QueryRun(
                mode="distributed",
                query_id=query_id,
                stages=len(staged.stages),
                tasks=len(results),
                stage_tasks=tuple(
                    sum(1 for r in results if r.stage_id == stage.stage_id)
                    for stage in staged.stages
                ),
                files=len(files),
                rows=sum(f.rows for f in files),
            )
            yield files
        finally:
            if not fleet.exchange_keep:
                try:
                    exchange.delete_query(query_id)
                except Exception as exc:  # never mask the query's own outcome
                    logger.warning("could not remove the exchange prefix of %s: %s", query_id, exc)

    def _result_query(
        self, files: Sequence[OutputFile], staged: StagePlan
    ) -> tuple[str, dict[str, Any]]:
        """The coordinator's read of the final stage: exactly the manifest's files.

        Stand-in columns are cast back to their own type, hidden order columns dropped,
        and the ORDER BY / LIMIT / OFFSET lifted off the plan applied over the
        concatenation (kickoff answer 2). Without an ORDER BY, file order is kept (DuckDB
        preserves it) -- which, after a range sort, is the order.
        """
        paths = [f.path for f in files]
        self._engine.ensure_object_store(paths)
        columns: list[exp.Expression] = []
        for name in staged.columns:
            column = exp.column(name, quoted=True)
            kind = staged.restore.get(name)
            columns.append(
                exp.Alias(this=cast_to(column, kind), alias=exp.to_identifier(name, quoted=True))
                if kind
                else column
            )
        listing = ", ".join(c.sql(dialect="duckdb") for c in columns)
        sql = (
            f"SELECT {listing} FROM read_parquet(${_RESULT_PARAMETER}, "
            f"union_by_name => false, hive_partitioning => false)"
        )
        if staged.order:
            sql += " ORDER BY " + ", ".join(staged.order)
        if staged.limit is not None:
            sql += f" LIMIT {int(staged.limit)}"
        if staged.offset:
            sql += f" OFFSET {int(staged.offset)}"
        return sql, {_RESULT_PARAMETER: paths}

    def _fetch(self, sql: str, parameters: Mapping[str, Any]) -> list[tuple[Any, ...]]:
        """Run `sql` on the coordinator's own DuckDB (a range sort's samples)."""
        paths = [p for value in parameters.values() if isinstance(value, list) for p in value]
        self._engine.ensure_object_store(paths)
        table = self._engine.arrow(sql, dict(parameters))
        return [tuple(row.values()) for row in table.to_pylist()]

    # -- writes (Phase 5) -------------------------------------------------------

    def _write_reason(self, df: DataFrame) -> str | None:
        """Why the query behind a write cannot be distributed, or None when it can."""
        staged = self._stage_plan(df._plan, df._sources, df.columns)
        return staged.reason if isinstance(staged, Uncuttable) else None

    def _empty_result(self, df: DataFrame) -> pa.Table:
        """The result with no rows, as this session's own engine hands it out: the Arrow
        types icetl's writer would have seen from `toArrow()`, without running the query."""
        compiled = self._compile(df._plan, df._sources, df.columns)
        parameters = dict(compiled.parameters)
        paths = [p for v in parameters.values() if isinstance(v, list) for p in v]
        self._engine.ensure_object_store([str(p) for p in paths])
        return self._engine.arrow(f"SELECT * FROM ({compiled.sql}) LIMIT 0", parameters)

    def _write_single_node(self, reason: str) -> None:
        """A write goes to icetl's writer: say so, or refuse (`icefleet.fallback=error`)."""
        from icefleet.write.writer import refuse

        if self._fleet().fallback == "error":
            raise refuse(reason)
        logger.info("single-node write: %s", reason)
        self._last_write = QueryRun(mode="single-node", reason=reason)

    def _write_files(
        self, df: DataFrame, target: WriteTarget
    ) -> tuple[list[OutputFile], Callable[[], None]] | Uncuttable:
        """Run the stages that write `df` into `target`; their data files, and how to
        delete them again if the commit does not happen. A failed run deletes its own."""
        staged = self._stage_plan(df._plan, df._sources, df.columns, write=target)
        if isinstance(staged, Uncuttable):
            return staged
        fleet = self._fleet()
        cluster = self._ensure_cluster()
        exchange = self._ensure_exchange()
        written = open_exchange(target.location, self._settings.s3)
        query_id = f"q-{uuid.uuid4().hex[:12]}"
        scheduler = ThreadPoolScheduler(
            cluster, target_file_bytes=fleet.result_file_bytes, fetch=self._fetch
        )

        def discard() -> None:
            try:
                written.delete_query(query_id)
            except Exception as exc:  # never mask the write's own outcome
                logger.warning("could not remove the files of failed write %s: %s", query_id, exc)

        try:
            results = scheduler.run(staged, query_id=query_id, exchange=exchange)
        except BaseException:
            discard()
            raise
        finally:
            if not fleet.exchange_keep:
                try:
                    exchange.delete_query(query_id)
                except Exception as exc:
                    logger.warning("could not remove the exchange prefix of %s: %s", query_id, exc)
        reported = [f for r in results if r.stage_id in staged.finals for f in r.data]
        files = [f for f in reported if f.rows > 0]
        empty = [f.path for f in reported if f.rows == 0]
        if empty:
            # An unpartitioned task with no rows writes one empty file: never committed,
            # so removed now rather than left in the table's folder.
            try:
                written.delete_files(empty)
            except Exception as exc:  # never mask the write's own outcome
                logger.warning("could not remove %d empty data file(s): %s", len(empty), exc)
        self._last_write = self._last_run = QueryRun(
            mode="distributed",
            query_id=query_id,
            stages=len(staged.stages),
            tasks=len(results),
            stage_tasks=tuple(
                sum(1 for r in results if r.stage_id == stage.stage_id) for stage in staged.stages
            ),
            files=len(files),
            rows=sum(f.rows for f in files),
        )
        return files, discard

    def _partitions_written(
        self, files: Sequence[OutputFile], columns: Sequence[str]
    ) -> list[dict[str, Any]]:
        """The distinct identity-partition values of the written files (a dynamic
        overwrite replaces exactly these partitions)."""
        if not files or not columns:
            return []
        listing = ", ".join(
            exp.to_identifier(c, quoted=True).sql(dialect="duckdb") for c in columns
        )
        paths = [f.path for f in files]
        sql = (
            f"SELECT DISTINCT {listing} FROM read_parquet(${_RESULT_PARAMETER}, "
            f"union_by_name => false, hive_partitioning => false)"
        )
        rows = self._fetch(sql, {_RESULT_PARAMETER: paths})
        return [dict(zip(columns, row, strict=True)) for row in rows]

    # -- row-level operations (Phase 5b) ------------------------------------------

    def _row_level(self, plan: exp.Delete | exp.Update | exp.Merge) -> DataFrame:
        """`DELETE` / `UPDATE` / `MERGE`: copy-on-write, the files rewritten by the workers
        (`write/rowlevel.py`); icetl's own planner for what cannot be distributed."""
        from icefleet.write.rowlevel import run_row_level

        run_row_level(self, self._inline_temp_views(plan))
        return self._empty_frame()

    @contextmanager
    def _pinning(self, files: Mapping[str, frozenset[str]]) -> Iterator[None]:
        """While open, every plan reads only `files` of those scans (by source key)."""
        previous = dict(self._pinned)
        self._pinned.update(files)
        try:
            yield
        finally:
            self._pinned = previous

    def _matching_files(self, query: exp.Expression, key: str) -> frozenset[str] | None:
        """The data files of scan `key` holding a row `query` (a filter over it) keeps, as
        the scan spells them; None when that cannot be found out distributed."""
        df = self._frame_for(query)
        compiled = self._compile(df._plan, df._sources, df.columns)
        scan = next((s for s in compiled.scans if s.source.key == key), None)
        if scan is not None and scan.is_empty:
            return frozenset()  # Iceberg's own pruning already says: no file can hold one
        staged = self._stage_plan(df._plan, df._sources, df.columns, probe=key)
        if isinstance(staged, Uncuttable):
            logger.info("no file probe (%s): every candidate file is rewritten", staged.reason)
            return None
        with self._distributed(staged) as files:
            column = exp.to_identifier(staged.columns[0], quoted=True).sql(dialect="duckdb")
            rows = self._fetch(
                f"SELECT DISTINCT {column} FROM read_parquet(${_RESULT_PARAMETER}, "
                f"union_by_name => false, hive_partitioning => false)",
                {_RESULT_PARAMETER: [f.path for f in files]},
            )
        return frozenset(str(row[0]) for row in rows if row[0] is not None)

    # -- lifecycle ---------------------------------------------------------

    def stop(self) -> None:
        """Stop the workers this session started, remove its exchange, close icetl."""
        with self._fleet_lock:
            cluster, self._cluster = self._cluster, None
            owned, self._owned_exchange_dir = self._owned_exchange_dir, None
            self._exchange = None
        if cluster is not None:
            cluster.close()
        if owned is not None:
            shutil.rmtree(owned, ignore_errors=True)
        super().stop()


# -- explain(): the one DataFrame seam ------------------------------------------
#
# icetl builds explain() text inside DataFrame, and derives every new frame as a plain
# `DataFrame`, so a subclass would never be reached. The wrapper below prepends the
# distributed plan for frames whose session is a ClusterSession and leaves every other
# frame's explain() byte-for-byte unchanged. `__wrapped__` keeps a re-import idempotent.

_ORIGINAL_EXPLAIN_TEXT = getattr(DataFrame._explain_text, "__wrapped__", DataFrame._explain_text)


def _explain_text(self: DataFrame, *, verbose: bool) -> str:
    text: str = _ORIGINAL_EXPLAIN_TEXT(self, verbose=verbose)
    session = self._session
    if isinstance(session, ClusterSession):
        return session._describe_distributed(self._plan, self._sources, self.columns) + text
    return text


_explain_text.__wrapped__ = _ORIGINAL_EXPLAIN_TEXT  # type: ignore[attr-defined]
setattr(DataFrame, "_explain_text", _explain_text)  # noqa: B010 - a method, deliberately


# -- df.write: the write seam (Phase 5) --------------------------------------------
#
# icetl's `DataFrame.write` property hard-codes `DataFrameWriter(self)`; frames of a
# ClusterSession get the cluster writer instead, every other frame icetl's own.

_ORIGINAL_WRITE = getattr(DataFrame.write, "__wrapped__", DataFrame.write)


def _write(self: DataFrame) -> Any:
    if isinstance(self._session, ClusterSession):
        from icefleet.write.writer import ClusterDataFrameWriter

        return ClusterDataFrameWriter(self)
    return _ORIGINAL_WRITE.fget(self)  # type: ignore[union-attr]


class _WriteProperty(property):
    """A property that remembers the one it replaced (a plain one cannot), so a
    re-import wraps icetl's original, not this."""

    __wrapped__: Any


_cluster_write = _WriteProperty(_write, doc=_ORIGINAL_WRITE.__doc__)
_cluster_write.__wrapped__ = _ORIGINAL_WRITE
setattr(DataFrame, "write", _cluster_write)  # noqa: B010 - a property, deliberately
