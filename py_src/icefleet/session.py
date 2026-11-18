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

The seams are icetl's private `_execute`, `_stream`, `_compile` and
`DataFrame._explain_text`, which is why icetl is pinned exactly (PLAN §4.10) and
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
        self._aggregate_names: frozenset[str] | None = None

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
