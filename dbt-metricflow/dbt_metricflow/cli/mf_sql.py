from __future__ import annotations

import datetime as dt
import logging
import sys
import textwrap
import time
from pathlib import Path
from typing import Dict, Optional, Sequence

import click
from dateutil.parser import parse as parse_datetime
from metricflow_semantics.errors.error_classes import SqlBindParametersNotSupportedError
from metricflow_semantics.model.dbt_manifest_parser import parse_manifest_from_dbt_generated_manifest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowExplainResult, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer
from metricflow_semantic_interfaces.protocols.semantic_manifest import SemanticManifest

logger = logging.getLogger(__name__)


class _DialectOnlySqlClient:
    """SqlClient-like object that only provides a SQL dialect for rendering."""

    def __init__(self, sql_engine_type: SqlEngine, sql_plan_renderer: SqlPlanRenderer) -> None:
        self._sql_engine_type = sql_engine_type
        self._sql_plan_renderer = sql_plan_renderer

    @classmethod
    def from_dialect_name(cls, dialect_name: str) -> _DialectOnlySqlClient:
        normalized = dialect_name.strip().lower()
        aliases: Dict[str, SqlEngine] = {
            "bigquery": SqlEngine.BIGQUERY,
            "big_query": SqlEngine.BIGQUERY,
            "duckdb": SqlEngine.DUCKDB,
            "postgres": SqlEngine.POSTGRES,
            "postgresql": SqlEngine.POSTGRES,
            "redshift": SqlEngine.REDSHIFT,
            "snowflake": SqlEngine.SNOWFLAKE,
        }
        if normalized not in aliases:
            raise click.BadParameter(
                f"Unsupported dialect {dialect_name!r}. Expected one of: {', '.join(sorted(aliases.keys()))}."
            )
        return cls.from_sql_engine(aliases[normalized])

    @classmethod
    def from_sql_engine(cls, sql_engine: SqlEngine) -> _DialectOnlySqlClient:
        if sql_engine is SqlEngine.BIGQUERY:
            from metricflow.sql.render.big_query import BigQuerySqlPlanRenderer

            renderer: SqlPlanRenderer = BigQuerySqlPlanRenderer()
        elif sql_engine is SqlEngine.DUCKDB:
            renderer = DuckDbSqlPlanRenderer()
        elif sql_engine is SqlEngine.POSTGRES:
            from metricflow.sql.render.postgres import PostgresSQLSqlPlanRenderer

            renderer = PostgresSQLSqlPlanRenderer()
        elif sql_engine is SqlEngine.REDSHIFT:
            from metricflow.sql.render.redshift import RedshiftSqlPlanRenderer

            renderer = RedshiftSqlPlanRenderer()
        elif sql_engine is SqlEngine.SNOWFLAKE:
            from metricflow.sql.render.snowflake import SnowflakeSqlPlanRenderer

            renderer = SnowflakeSqlPlanRenderer()
        else:
            raise click.BadParameter(f"Unsupported dialect: {sql_engine}")
        return cls(sql_engine_type=sql_engine, sql_plan_renderer=renderer)

    @property
    def sql_engine_type(self) -> SqlEngine:
        return self._sql_engine_type

    @property
    def sql_plan_renderer(self) -> SqlPlanRenderer:
        return self._sql_plan_renderer

    def query(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> MetricFlowDataTable:
        """Reject query execution; this client is only intended for rendering SQL."""
        raise SqlBindParametersNotSupportedError(
            "_DialectOnlySqlClient cannot execute queries; use it only with MetricFlowEngine.explain()."
        )

    def execute(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        """Reject statement execution; this client is only intended for rendering SQL."""
        raise SqlBindParametersNotSupportedError(
            "_DialectOnlySqlClient cannot execute statements; use it only with MetricFlowEngine.explain()."
        )

    def dry_run(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        """Reject dry runs; this client is only intended for rendering SQL."""
        raise SqlBindParametersNotSupportedError(
            "_DialectOnlySqlClient cannot dry run statements; use it only with MetricFlowEngine.explain()."
        )

    def close(self) -> None:
        """No-op close method to satisfy the SqlClient protocol."""
        return None

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        """Reject bind parameter rendering; this client has no warehouse execution context."""
        raise SqlBindParametersNotSupportedError(
            "_DialectOnlySqlClient cannot render bind parameters; use it only with MetricFlowEngine.explain()."
        )


def _parse_optional_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    if value is None:
        return None
    return parse_datetime(value)


def _parse_csv(value: Optional[str]) -> Optional[Sequence[str]]:
    if value is None:
        return None
    if value == "":
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _load_semantic_manifest_from_source(manifest_source: str) -> SemanticManifest:
    source = manifest_source.strip()
    if source == "-":
        raw_contents = sys.stdin.read()
        if not raw_contents.strip():
            raise click.BadParameter("stdin is empty; pass a semantic manifest JSON string or file.")
    else:
        manifest_path = Path(source)
        if not manifest_path.exists():
            raise click.BadParameter(f"Semantic manifest not found: {manifest_path}")
        raw_contents = manifest_path.read_text()
    return parse_manifest_from_dbt_generated_manifest(manifest_json_string=raw_contents)


def _build_fast_path_engine(manifest_source: str, dialect: str) -> MetricFlowEngine:
    logging.getLogger("metricflow_semantic_interfaces.transformations.fix_proxy_metrics").setLevel(logging.ERROR)
    semantic_manifest = _load_semantic_manifest_from_source(manifest_source)
    semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
    sql_client = _DialectOnlySqlClient.from_dialect_name(dialect)
    return MetricFlowEngine(semantic_manifest_lookup=semantic_manifest_lookup, sql_client=sql_client)


@click.command()
@click.option(
    "--semantic-manifest",
    required=True,
    type=str,
    help="Path to semantic_manifest.json or '-' to read from stdin.",
)
@click.option(
    "--dialect",
    required=True,
    type=str,
    help="SQL dialect (redshift, snowflake, bigquery, postgres, duckdb).",
)
@click.option(
    "--metrics",
    type=str,
    default="",
    help="Comma-separated metrics (e.g., bookings,messages).",
)
@click.option(
    "--group-by",
    type=str,
    default="",
    help="Comma-separated group bys (e.g., ds,org).",
)
@click.option(
    "--where",
    type=str,
    multiple=True,
    default=(),
    help=("SQL-like where statement string. Repeat for multiple conditions (ANDed together). Example: \"{{ Dimension('order_id__revenue') }} > 100\""),
)
@click.option(
    "--start-time",
    type=str,
    default=None,
    help="Optional iso8601 timestamp to constrain the start time (inclusive).",
)
@click.option(
    "--end-time",
    type=str,
    default=None,
    help="Optional iso8601 timestamp to constrain the end time (inclusive).",
)
@click.option(
    "--order",
    type=str,
    default="",
    help="Comma-separated order bys (prefix with '-' for DESC).",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limit the number of rows returned.",
)
@click.option(
    "--show-dataflow-plan",
    is_flag=True,
    default=False,
    help="Display the dataflow plan in the explain output.",
)
@click.option(
    "--show-sql-descriptions",
    is_flag=True,
    default=False,
    help="Show inline descriptions of nodes in displayed SQL.",
)
def cli(  # noqa: D103
    semantic_manifest: str,
    dialect: str,
    metrics: str,
    group_by: str,
    where: Sequence[str],
    start_time: Optional[str],
    end_time: Optional[str],
    order: str,
    limit: Optional[int],
    show_dataflow_plan: bool,
    show_sql_descriptions: bool,
) -> None:
    start_time_seconds = time.perf_counter()
    click.echo("mf-sql: query start")
    mf_engine = _build_fast_path_engine(manifest_source=semantic_manifest, dialect=dialect)

    mf_request = MetricFlowQueryRequest.create(
        metric_names=_parse_csv(metrics),
        group_by_names=_parse_csv(group_by),
        limit=limit,
        time_constraint_start=_parse_optional_datetime(start_time),
        time_constraint_end=_parse_optional_datetime(end_time),
        where_constraints=list(where) if where else None,
        order_by_names=_parse_csv(order),
    )

    explain_result: MetricFlowExplainResult = mf_engine.explain(mf_request=mf_request)
    sql = (
        explain_result.sql_statement.without_descriptions.sql
        if not show_sql_descriptions
        else explain_result.sql_statement.sql
    )
    if show_dataflow_plan:
        import jinja2

        click.echo(
            textwrap.indent(
                jinja2.Template(
                    textwrap.dedent(
                        """\
                        Metric Dataflow Plan:
                            {{ plan_text | indent(4) }}
                        """
                    ),
                    undefined=jinja2.StrictUndefined,
                ).render(plan_text=explain_result.dataflow_plan.structure_text()),
                prefix="-- ",
            )
        )
        click.echo("")

    click.echo(sql)
    elapsed_seconds = time.perf_counter() - start_time_seconds
    click.echo(f"mf-sql: query end ({elapsed_seconds:.4f}s)")


if __name__ == "__main__":
    cli()
