#!/usr/bin/env python3
"""mf_entry.py — MetricFlow IPC entry point for the Nuitka sidecar.

Compiled by Nuitka into a standalone binary for use as a general sidecar
as a subprocess. Communicates via NDJSON over stdin/stdout (mf-ipc v1).

Protocol v1:
  ready:    {"status":"ready","metricflow_version":"X","python_version":"X","protocol_version":1}
  explain:  {"id":"...","method":"explain","v":1,"params":{
                "manifest_path":"...","metric_names":[...],"group_by_names":[...],
                "where_constraints":null,"order_by_names":null,"limit":null,"sql_engine":"DUCKDB"
            }} → {"id":"...","ok":true,"sql":"..."}
  ping:     {"id":"...","method":"ping","v":1} → {"id":"...","ok":true}
  shutdown: {"id":"...","method":"shutdown","v":1} → {"id":"...","ok":true}
  error:    {"id":"...","ok":false,"error":{"type":"ExceptionClass","message":"..."}}

manifest_path may be a YAML directory (dev/testing) or a manifest.json file (production).
sql_engine must be a SqlEngine enum name: DUCKDB, BIGQUERY, DATABRICKS, POSTGRES, REDSHIFT,
SNOWFLAKE, or TRINO.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import logging
import os
import signal
import sys
import traceback as _traceback
import types
from pathlib import Path
from typing import IO, Literal

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.test_helpers.manifest_helpers import (
    mf_load_manifest_from_json_file,
    mf_load_manifest_from_yaml_directory,
)
from mf_ipc_protocol import (
    ErrorDetail,
    ErrorResponse,
    ExplainParams,
    ExplainResponse,
    OkResponse,
    ReadyMessage,
    RequestEnvelope,
    RequestId,
    StartupErrorMessage,
)
from pydantic import BaseModel, ValidationError

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.big_query import BigQuerySqlPlanRenderer
from metricflow.sql.render.databricks import DatabricksSqlPlanRenderer
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer
from metricflow.sql.render.postgres import PostgresSQLSqlPlanRenderer
from metricflow.sql.render.redshift import RedshiftSqlPlanRenderer
from metricflow.sql.render.snowflake import SnowflakeSqlPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer
from metricflow.sql.render.trino import TrinoSqlPlanRenderer
from metricflow_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest

try:
    _MF_VERSION = importlib.metadata.version("metricflow")
except Exception:
    _MF_VERSION = "unknown"

_ipc: IO[str] = sys.stdout  # replaced by main() before first write
_debug: bool = False
_cached: tuple[str, float, SqlEngine, MetricFlowEngine] | None = None


class _SqlClientStub:
    """Minimal SqlClient satisfying the Protocol for explain() only.

    explain() compiles SQL but never executes it. MetricFlowEngine needs
    sql_engine_type (for feature-gating) and sql_plan_renderer (for dialect SQL).
    All execution methods raise to make accidental execution obvious.
    """

    def __init__(self, engine: SqlEngine) -> None:
        self._engine = engine
        self._renderer = _renderer_for(engine)

    @property
    def sql_engine_type(self) -> SqlEngine:
        return self._engine

    @property
    def sql_plan_renderer(self) -> SqlPlanRenderer:
        return self._renderer

    def query(
        self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()
    ) -> MetricFlowDataTable:
        raise NotImplementedError("IPC stub does not execute SQL")

    def execute(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()) -> None:
        raise NotImplementedError("IPC stub does not execute SQL")

    def dry_run(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()) -> None:
        raise NotImplementedError("IPC stub does not execute SQL")

    def close(self) -> None:
        pass

    def render_bind_parameter_key(self, k: str) -> str:
        return f":{k}"


def _renderer_for(engine: SqlEngine) -> SqlPlanRenderer:
    match engine:
        case SqlEngine.DUCKDB:
            return DuckDbSqlPlanRenderer()
        case SqlEngine.BIGQUERY:
            return BigQuerySqlPlanRenderer()
        case SqlEngine.DATABRICKS:
            return DatabricksSqlPlanRenderer()
        case SqlEngine.POSTGRES:
            return PostgresSQLSqlPlanRenderer()
        case SqlEngine.REDSHIFT:
            return RedshiftSqlPlanRenderer()
        case SqlEngine.SNOWFLAKE:
            return SnowflakeSqlPlanRenderer()
        case SqlEngine.TRINO:
            return TrinoSqlPlanRenderer()
        case _:
            raise ValueError(f"No renderer for engine: {engine!r}")


def _load_manifest(path: str) -> PydanticSemanticManifest:
    p = Path(path)
    if p.is_dir():
        return mf_load_manifest_from_yaml_directory(p, {"source_schema": "production"})
    return mf_load_manifest_from_json_file(p)


def _get_engine(manifest_path: str, sql_engine: SqlEngine) -> MetricFlowEngine:
    global _cached
    mtime = os.path.getmtime(manifest_path)
    if _cached and _cached[:3] == (manifest_path, mtime, sql_engine):
        return _cached[3]
    manifest = _load_manifest(manifest_path)
    lookup = SemanticManifestLookup(manifest)
    engine = MetricFlowEngine(lookup, _SqlClientStub(sql_engine))  # type: ignore[arg-type]
    _cached = (manifest_path, mtime, sql_engine, engine)
    return engine


def _write(msg: BaseModel) -> None:
    try:
        _ipc.write(msg.model_dump_json() + "\n")
        _ipc.flush()
    except BrokenPipeError:
        logging.warning("IPC pipe broken; exiting")
        sys.exit(0)


def _err(req_id: RequestId, exc: Exception) -> ErrorResponse:
    error = ErrorDetail(type=type(exc).__name__, message=str(exc))
    if _debug:
        error.traceback = _traceback.format_exc()
    return ErrorResponse(id=req_id, error=error)


def _handle_explain(req_id: RequestId, raw_params: dict) -> ExplainResponse | ErrorResponse:
    try:
        params = ExplainParams.model_validate(raw_params)
        sql_engine = SqlEngine[params.sql_engine]
        engine = _get_engine(params.manifest_path, sql_engine)
        request = MetricFlowQueryRequest.create(
            metric_names=params.metric_names,
            group_by_names=params.group_by_names,
            where_constraints=params.where_constraints,
            order_by_names=params.order_by_names,
            limit=params.limit,
        )
        result = engine.explain(request)
        return ExplainResponse(id=req_id, sql=result.sql_statement.sql)
    except Exception as e:
        return _err(req_id, e)


def _dispatch(req: object) -> ExplainResponse | OkResponse | ErrorResponse:
    try:
        envelope = RequestEnvelope.model_validate(req)
    except ValidationError as e:
        fallback_id = req.get("id") if isinstance(req, dict) else None
        return _err(fallback_id, e)

    if envelope.v != 1:
        return ErrorResponse(
            id=envelope.id,
            error=ErrorDetail(type="ProtocolVersionError", message=f"Expected v=1, got v={envelope.v!r}"),
        )
    if envelope.method == "ping":
        return OkResponse(id=envelope.id)
    if envelope.method == "shutdown":
        return OkResponse(id=envelope.id)
    if envelope.method == "explain":
        return _handle_explain(envelope.id, envelope.params or {})
    return ErrorResponse(
        id=envelope.id,
        error=ErrorDetail(
            type="UnknownMethod",
            message=f"Unknown method: {envelope.method!r}. Supported: explain, ping, shutdown",
        ),
    )


def main(argv: list[str]) -> Literal[0, 1]:  # noqa: D103
    global _ipc, _debug

    parser = argparse.ArgumentParser(description="MetricFlow IPC entry point (mf-ipc v1)")
    parser.add_argument("--manifest-path", help="Pre-load manifest before sending the ready message")
    parser.add_argument("--sql-engine", default="DUCKDB", help="SQL engine for pre-warming (default: DUCKDB)")
    parser.add_argument("--debug", action="store_true", help="Verbose logging and tracebacks in error responses")
    parser.add_argument("--version", action="store_true", help="Print version and exit")
    args = parser.parse_args(argv)

    if args.version:
        sys.stdout.write(f"mf_entry {_MF_VERSION} (mf-ipc v1)\n")
        return 0

    _debug = args.debug

    # Protect the IPC channel: save real stdout, redirect print()/logging to stderr.
    # Any library that calls print() will write to stderr rather than corrupting the
    # NDJSON framing on the pipe the caller is reading.
    _ipc = sys.stdout
    sys.stdout = sys.stderr

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if _debug else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    def _exit_clean(signum: int, frame: types.FrameType | None) -> None:
        _ipc.flush()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _exit_clean)
    signal.signal(signal.SIGINT, _exit_clean)

    if args.manifest_path:
        try:
            _get_engine(args.manifest_path, SqlEngine[args.sql_engine])
        except Exception as e:
            _write(StartupErrorMessage(type=type(e).__name__, message=str(e)))
            return 1

    _write(
        ReadyMessage(
            metricflow_version=_MF_VERSION,
            python_version=sys.version.split()[0],
        )
    )

    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except json.JSONDecodeError as e:
                _write(ErrorResponse(id=None, error=ErrorDetail(type="JSONDecodeError", message=str(e))))
                continue
            resp = _dispatch(req)
            _write(resp)
            if isinstance(req, dict) and req.get("method") == "shutdown":
                break
    except Exception:
        logging.exception("Uncaught error in IPC loop")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
