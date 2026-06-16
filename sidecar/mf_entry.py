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
from typing import IO

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.test_helpers.manifest_helpers import (
    mf_load_manifest_from_json_file,
    mf_load_manifest_from_yaml_directory,
)

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


def _write(obj: dict) -> None:
    try:
        _ipc.write(json.dumps(obj) + "\n")
        _ipc.flush()
    except BrokenPipeError:
        logging.warning("IPC pipe broken; exiting")
        sys.exit(0)


def _err(req_id: str | int | None, exc: Exception) -> dict:
    error: dict[str, str] = {"type": type(exc).__name__, "message": str(exc)}
    if _debug:
        error["traceback"] = _traceback.format_exc()
    return {"id": req_id, "ok": False, "error": error}


def _handle_explain(req_id: str | int | None, params: dict) -> dict:
    try:
        manifest_path = params["manifest_path"]
        sql_engine = SqlEngine[params.get("sql_engine", "DUCKDB")]
        engine = _get_engine(manifest_path, sql_engine)
        request = MetricFlowQueryRequest.create(
            metric_names=params.get("metric_names"),
            group_by_names=params.get("group_by_names"),
            where_constraints=params.get("where_constraints"),
            order_by_names=params.get("order_by_names"),
            limit=params.get("limit"),
        )
        result = engine.explain(request)
        return {"id": req_id, "ok": True, "sql": result.sql_statement.sql}
    except Exception as e:
        return _err(req_id, e)


def _dispatch(req: dict) -> dict:
    req_id = req.get("id")
    method = req.get("method")
    v = req.get("v", 1)
    if v != 1:
        return {
            "id": req_id,
            "ok": False,
            "error": {"type": "ProtocolVersionError", "message": f"Expected v=1, got v={v!r}"},
        }
    if method == "ping":
        return {"id": req_id, "ok": True}
    if method == "shutdown":
        return {"id": req_id, "ok": True}
    if method == "explain":
        return _handle_explain(req_id, req.get("params") or {})
    return {
        "id": req_id,
        "ok": False,
        "error": {
            "type": "UnknownMethod",
            "message": f"Unknown method: {method!r}. Supported: explain, ping, shutdown",
        },
    }


def main(argv: list[str]) -> int:  # noqa: D103
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
            _ipc.write(json.dumps({"status": "error", "type": type(e).__name__, "message": str(e)}) + "\n")
            _ipc.flush()
            return 1

    _write(
        {
            "status": "ready",
            "metricflow_version": _MF_VERSION,
            "python_version": sys.version.split()[0],
            "protocol_version": 1,
        }
    )

    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except json.JSONDecodeError as e:
                _write({"id": None, "ok": False, "error": {"type": "JSONDecodeError", "message": str(e)}})
                continue
            resp = _dispatch(req)
            _write(resp)
            if req.get("method") == "shutdown":
                break
    except Exception:
        logging.exception("Uncaught error in IPC loop")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
