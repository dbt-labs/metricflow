from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_plan_snapshot_text_equal,
    assert_snapshot_text_equal,
    make_schema_replacement_function,
)

from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer
from metricflow.sql.sql_plan import SqlPlan, SqlPlanNode
from tests_metricflow.fixtures.setup_fixtures import check_sql_engine_snapshot_marker
from tests_metricflow.snapshot_utils import (
    _EXCLUDE_TABLE_ALIAS_REGEX,
    SQL_ENGINE_HEADER_NAME,
)


def assert_default_rendered_sql_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    plan_id: str,
    sql_plan_node: SqlPlanNode,
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
    )


def assert_rendered_sql_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    plan_id: str,
    sql_plan_node: SqlPlanNode,
    sql_client: SqlClient,
) -> None:
    """Helper function to render a select statement and compare with the one saved as a file."""
    sql_query_plan = SqlPlan(render_node=sql_plan_node, plan_id=DagId.from_str(plan_id))

    assert_rendered_sql_from_plan_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_query_plan=sql_query_plan,
        sql_client=sql_client,
    )


def assert_rendered_sql_from_plan_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_query_plan: SqlPlan,
    sql_client: SqlClient,
) -> None:
    """Similar to assert_rendered_sql_equal, but takes in a SQL query plan."""
    check_sql_engine_snapshot_marker(request)

    rendered_sql = sql_client.sql_plan_renderer.render_sql_plan(sql_query_plan).sql

    sql_engine = sql_client.sql_engine_type
    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        group_id=sql_query_plan.__class__.__name__,
        snapshot_id=sql_query_plan.dag_id.id_str,
        snapshot_text=rendered_sql,
        snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
        exclude_line_regex=_EXCLUDE_TABLE_ALIAS_REGEX,
        additional_sub_directories_for_snapshots=(sql_engine.value,),
        additional_header_fields={SQL_ENGINE_HEADER_NAME: sql_engine.value},
    )


def assert_sql_plan_text_equal(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_query_plan: SqlPlan,
) -> None:
    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=sql_query_plan,
        plan_snapshot_text=sql_query_plan.structure_text(),
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
    )
