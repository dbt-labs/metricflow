"""Centralizes useful functions for comparing two different rendered queries."""

from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.optimizer.dataflow_optimizer_factory import DataflowPlanOptimization
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from tests_metricflow.dataflow_plan_to_svg import display_graph_if_requested
from tests_metricflow.sql.compare_sql_plan import assert_rendered_sql_from_plan_equal


def render_and_check(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_spec: MetricFlowQuerySpec,
) -> None:
    """Renders an engine-specific query output from a given query, in both basic and optimized forms."""
    # Build and convert dataflow plan without optimizers
    is_distinct_values_plan = not query_spec.metric_specs
    if is_distinct_values_plan:
        base_plan = dataflow_plan_builder.build_plan_for_distinct_values(query_spec=query_spec)
    else:
        base_plan = dataflow_plan_builder.build_plan(query_spec)
    conversion_result = dataflow_to_sql_converter.convert_to_sql_plan(
        sql_engine_type=sql_client.sql_engine_type,
        dataflow_plan_node=base_plan.sink_node,
        optimization_level=SqlOptimizationLevel.O0,
        sql_query_plan_id=DagId.from_str("plan0"),
    )
    sql_query_plan = conversion_result.sql_plan
    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=sql_query_plan,
    )

    assert_rendered_sql_from_plan_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_query_plan=sql_query_plan,
        sql_client=sql_client,
    )

    # Run dataflow -> sql conversion with all optimizers
    if is_distinct_values_plan:
        optimized_plan = dataflow_plan_builder.build_plan_for_distinct_values(
            query_spec, optimizations=DataflowPlanOptimization.enabled_optimizations()
        )
    else:
        optimized_plan = dataflow_plan_builder.build_plan(
            query_spec, optimizations=DataflowPlanOptimization.enabled_optimizations()
        )
    conversion_result = dataflow_to_sql_converter.convert_to_sql_plan(
        sql_engine_type=sql_client.sql_engine_type,
        dataflow_plan_node=optimized_plan.sink_node,
        sql_query_plan_id=DagId.from_str("plan0_optimized"),
    )
    sql_query_plan = conversion_result.sql_plan
    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=sql_query_plan,
    )

    assert_rendered_sql_from_plan_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_query_plan=sql_query_plan,
        sql_client=sql_client,
    )
