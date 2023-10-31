"""Centralizes useful functions for comparing two different rendered queries."""
from __future__ import annotations

from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.dataflow_plan import BaseOutput
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.sql.compare_sql_plan import assert_rendered_sql_from_plan_equal


def convert_and_check(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    node: BaseOutput,
) -> None:
    """Renders an engine-specific query output from a BaseOutput DataFlowPlan node.

    TODO: refine interface once file move operations are complete.
    """
    # Run dataflow -> sql conversion without optimizers
    sql_query_plan = dataflow_to_sql_converter.convert_to_sql_query_plan(
        sql_engine_type=sql_client.sql_engine_type,
        sql_query_plan_id="plan0",
        dataflow_plan_node=node,
        optimization_level=SqlQueryOptimizationLevel.O0,
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=sql_query_plan,
    )

    assert_rendered_sql_from_plan_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_query_plan=sql_query_plan,
        sql_client=sql_client,
    )

    # Run dataflow -> sql conversion with optimizers
    sql_query_plan = dataflow_to_sql_converter.convert_to_sql_query_plan(
        sql_engine_type=sql_client.sql_engine_type,
        sql_query_plan_id="plan0_optimized",
        dataflow_plan_node=node,
        optimization_level=SqlQueryOptimizationLevel.O4,
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=sql_query_plan,
    )

    assert_rendered_sql_from_plan_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_query_plan=sql_query_plan,
        sql_client=sql_client,
    )
