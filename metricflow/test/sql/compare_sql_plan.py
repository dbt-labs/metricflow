from __future__ import annotations

from _pytest.fixtures import FixtureRequest

from metricflow.dag.mf_dag import DagId
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_plan import SqlQueryPlan, SqlQueryPlanNode
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestConfiguration, check_sql_engine_snapshot_marker
from metricflow.test.snapshot_utils import (
    assert_plan_snapshot_text_equal,
    make_schema_replacement_function,
)


def assert_default_rendered_sql_equal(  # noqa: D
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    plan_id: str,
    sql_plan_node: SqlQueryPlanNode,
) -> None:
    """Helper function to render a select statement and compare with the one saved as a file."""
    sql_query_plan = SqlQueryPlan(render_node=sql_plan_node, plan_id=DagId.from_str(plan_id))

    rendered_sql = DefaultSqlQueryPlanRenderer().render_sql_query_plan(sql_query_plan).sql

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan=sql_query_plan,
        plan_snapshot_text=rendered_sql,
        plan_snapshot_file_extension=".sql",
    )


def assert_rendered_sql_equal(  # noqa: D
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    plan_id: str,
    sql_plan_node: SqlQueryPlanNode,
    sql_client: SqlClient,
) -> None:
    """Helper function to render a select statement and compare with the one saved as a file."""
    sql_query_plan = SqlQueryPlan(render_node=sql_plan_node, plan_id=DagId.from_str(plan_id))

    assert_rendered_sql_from_plan_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_query_plan=sql_query_plan,
        sql_client=sql_client,
    )


def assert_rendered_sql_from_plan_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_query_plan: SqlQueryPlan,
    sql_client: SqlClient,
) -> None:
    """Similar to assert_rendered_sql_equal, but takes in a SQL query plan."""
    check_sql_engine_snapshot_marker(request)

    rendered_sql = sql_client.sql_query_plan_renderer.render_sql_query_plan(sql_query_plan).sql

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan=sql_query_plan,
        plan_snapshot_text=rendered_sql,
        plan_snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,) if sql_client else (),
    )


def assert_sql_plan_text_equal(  # noqa: D
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_query_plan: SqlQueryPlan,
) -> None:
    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan=sql_query_plan,
        plan_snapshot_text=sql_query_plan.structure_text(),
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
    )
