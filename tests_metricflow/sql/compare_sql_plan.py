from __future__ import annotations

import logging
import os
from typing import Optional

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_plan_snapshot_text_equal,
    assert_snapshot_text_equal,
    make_schema_replacement_function,
    snapshot_path_prefix,
)

from metricflow.protocols.sql_client import SqlClient, SqlEngine
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer
from metricflow.sql.sql_plan import SqlPlan, SqlPlanNode
from tests_metricflow.fixtures.setup_fixtures import check_sql_engine_snapshot_marker
from tests_metricflow.snapshot_utils import (
    _EXCLUDE_TABLE_ALIAS_REGEX,
    SQL_ENGINE_HEADER_NAME,
)

logger = logging.getLogger(__name__)


def assert_default_rendered_sql_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    plan_id: str,
    sql_plan_node: SqlPlanNode,
    expectation_description: Optional[str] = None,
) -> None:
    """Helper function to render a select statement and compare with the one saved as a file."""
    sql_query_plan = SqlPlan(render_node=sql_plan_node, plan_id=DagId.from_str(plan_id))
    rendered_sql = DefaultSqlPlanRenderer().render_sql_plan(sql_query_plan).sql

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=sql_query_plan,
        plan_snapshot_text=rendered_sql,
        plan_snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
        expectation_description=expectation_description,
    )


def assert_rendered_sql_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    plan_id: str,
    sql_plan_node: SqlPlanNode,
    sql_client: SqlClient,
    expectation_description: Optional[str] = None,
) -> None:
    """Helper function to render a select statement and compare with the one saved as a file."""
    sql_query_plan = SqlPlan(render_node=sql_plan_node, plan_id=DagId.from_str(plan_id))

    assert_rendered_sql_from_plan_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_query_plan=sql_query_plan,
        sql_client=sql_client,
        expectation_description=expectation_description,
    )


def assert_rendered_sql_from_plan_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_query_plan: SqlPlan,
    sql_client: SqlClient,
    expectation_description: Optional[str] = None,
) -> None:
    """Similar to assert_rendered_sql_equal, but takes in a SQL query plan."""
    check_sql_engine_snapshot_marker(request)

    rendered_sql = sql_client.sql_plan_renderer.render_sql_plan(sql_query_plan).sql
    sql_engine = sql_client.sql_engine_type
    if sql_engine is SqlEngine.DUCKDB:
        snapshot_text = rendered_sql
    else:
        snapshot_text_if_different = _rendered_sql_if_different_from_duckdb_snapshot(
            request=request,
            mf_test_configuration=mf_test_configuration,
            sql_query_plan=sql_query_plan,
            rendered_sql=rendered_sql,
        )
        snapshot_text = "Matches DuckDB snapshot." if snapshot_text_if_different is None else snapshot_text_if_different

    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        group_id=sql_query_plan.__class__.__name__,
        snapshot_id=sql_query_plan.dag_id.id_str,
        snapshot_text=snapshot_text,
        snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
        exclude_line_regex=_EXCLUDE_TABLE_ALIAS_REGEX,
        additional_sub_directories_for_snapshots=(sql_engine.value,),
        additional_header_fields={SQL_ENGINE_HEADER_NAME: sql_engine.value},
        expectation_description=expectation_description,
    )


def _rendered_sql_if_different_from_duckdb_snapshot(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_query_plan: SqlPlan,
    rendered_sql: str,
) -> Optional[str]:
    """Return rendered SQL if it differs from the DuckDB snapshot."""
    duckdb_snapshot_file = _sql_snapshot_file_path(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_query_plan=sql_query_plan,
        sql_engine=SqlEngine.DUCKDB,
    )
    if not os.path.exists(duckdb_snapshot_file):
        raise FileNotFoundError(
            f"Could not find DuckDB snapshot file at path {duckdb_snapshot_file}. "
            "Generate the DuckDB snapshot first using --overwrite-snapshots with DuckDB."
        )

    with open(duckdb_snapshot_file, "r") as duckdb_snapshot:
        duckdb_snapshot_text = duckdb_snapshot.read()

    schema_replacement_function = make_schema_replacement_function(
        system_schema=mf_test_configuration.mf_system_schema,
        source_schema=mf_test_configuration.mf_source_schema,
    )
    duckdb_snapshot_body = _remove_snapshot_header(duckdb_snapshot_text).rstrip("\n")
    rendered_sql_for_comparison = schema_replacement_function(rendered_sql)

    if rendered_sql_for_comparison == duckdb_snapshot_body:
        return None

    return rendered_sql


def _sql_snapshot_file_path(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_query_plan: SqlPlan,
    sql_engine: SqlEngine,
) -> str:
    """Return the snapshot file path for a rendered SQL plan."""
    return str(
        snapshot_path_prefix(
            request=request,
            snapshot_configuration=mf_test_configuration,
            snapshot_group=sql_query_plan.__class__.__name__,
            snapshot_id=sql_query_plan.dag_id.id_str,
            additional_sub_directories=(sql_engine.value,),
        ).with_suffix(".sql")
    )


def _remove_snapshot_header(snapshot_text: str) -> str:
    """Remove the generated snapshot header if there is one."""
    lines = snapshot_text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.rstrip("\n") == "---":
            return "".join(lines[index + 1 :])
    return snapshot_text


def _normalize_rendered_sql_for_duckdb_comparison(snapshot_text: str) -> str:
    """Normalize rendered SQL snapshot text before comparing it to DuckDB."""
    lines = snapshot_text.split("\n")
    return "\n".join(line for line in lines if "_src" not in line)


def assert_sql_plan_text_equal(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_query_plan: SqlPlan,
    expectation_description: Optional[str] = None,
) -> None:
    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=sql_query_plan,
        plan_snapshot_text=sql_query_plan.structure_text(),
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
        expectation_description=expectation_description,
    )
