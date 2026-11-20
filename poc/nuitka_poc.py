#!/usr/bin/env python3
"""Nuitka compilation PoC for MetricFlow.

Loads a semantic manifest from a YAML directory, compiles one metric query to SQL,
and prints the result to stdout. Intentionally minimal: no IPC, no execution, no
database connection. The stub SqlClient provides only the DuckDB renderer so
explain() can generate dialect-specific SQL without opening a real connection.

Usage (run via hatch to get the full dev dependency set):
  hatch run dev-env:python poc/nuitka_poc.py --manifest-dir <path> [--metric-name bookings] [--group-by metric_time]

Validation workflow:
  python poc/nuitka_poc.py --manifest-dir $MANIFEST > poc/reference.sql
  poc/nuitka_poc.dist/nuitka_poc.bin --manifest-dir $MANIFEST > poc/nuitka.sql
  diff poc/reference.sql poc/nuitka.sql   # must be empty
"""
from __future__ import annotations

import argparse
import pathlib
import sys

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.test_helpers.manifest_helpers import mf_load_manifest_from_yaml_directory

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer


class _StubSqlClient:
    """Minimal SqlClient that satisfies the Protocol for explain() only.

    explain() compiles SQL but does not execute it. The only properties
    MetricFlowEngine needs from SqlClient at compile-time are sql_engine_type
    (for feature-gating unsupported granularities) and sql_plan_renderer
    (for dialect-specific SQL generation). All execution methods raise to
    make accidental execution obvious.
    """

    @property
    def sql_engine_type(self) -> SqlEngine:
        return SqlEngine.DUCKDB

    @property
    def sql_plan_renderer(self) -> SqlPlanRenderer:
        return DuckDbSqlPlanRenderer()

    def query(
        self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()
    ) -> MetricFlowDataTable:
        raise NotImplementedError("PoC stub does not execute SQL")

    def execute(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()) -> None:
        raise NotImplementedError("PoC stub does not execute SQL")

    def dry_run(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()) -> None:
        raise NotImplementedError("PoC stub does not execute SQL")

    def close(self) -> None:
        pass

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        return f":{bind_parameter_key}"


def main() -> int:
    """Compile a metric to SQL and print it to stdout."""
    parser = argparse.ArgumentParser(description="MetricFlow Nuitka PoC — compiles a metric to SQL and prints it")
    parser.add_argument(
        "--manifest-dir", required=True, type=pathlib.Path, help="Path to a semantic manifest YAML directory"
    )
    parser.add_argument("--metric-name", default="bookings", help="Metric to compile (default: bookings)")
    parser.add_argument("--group-by", default="metric_time", help="Group-by dimension (default: metric_time)")
    args = parser.parse_args()

    if not args.manifest_dir.is_dir():
        print(f"error: --manifest-dir {args.manifest_dir!r} is not a directory", file=sys.stderr)
        return 1

    manifest = mf_load_manifest_from_yaml_directory(
        yaml_file_directory=args.manifest_dir,
        template_mapping={"source_schema": "poc_schema"},
    )
    lookup = SemanticManifestLookup(manifest)
    engine = MetricFlowEngine(
        semantic_manifest_lookup=lookup,
        sql_client=_StubSqlClient(),  # type: ignore[arg-type]
    )

    result = engine.explain(
        MetricFlowQueryRequest.create(
            metric_names=[args.metric_name],
            group_by_names=[args.group_by],
        )
    )
    print(result.sql_statement.sql)
    return 0


if __name__ == "__main__":
    sys.exit(main())
