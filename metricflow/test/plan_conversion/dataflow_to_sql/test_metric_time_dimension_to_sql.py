from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import TimeDimensionReference

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import MetricTimeDimensionTransformNode
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.specs import MetricFlowQuerySpec, MetricSpec
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.plan_conversion.test_dataflow_to_sql_plan import convert_and_check
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY


def test_metric_time_dimension_transform_node_using_primary_time(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a PlotTimeDimensionTransform node using the primary time dimension to SQL."""
    source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    metric_time_dimension_transform_node = MetricTimeDimensionTransformNode(
        parent_node=source_node, aggregation_time_dimension_reference=TimeDimensionReference(element_name="ds")
    )
    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=metric_time_dimension_transform_node,
    )


def test_metric_time_dimension_transform_node_using_non_primary_time(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a PlotTimeDimensionTransform node using a non-primary time dimension to SQL."""
    source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    metric_time_dimension_transform_node = MetricTimeDimensionTransformNode(
        parent_node=source_node,
        aggregation_time_dimension_reference=TimeDimensionReference(element_name="paid_at"),
    )
    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=metric_time_dimension_transform_node,
    )


def test_simple_query_with_metric_time_dimension(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests building a query that uses measures defined from 2 different time dimensions."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings"),
                MetricSpec(element_name="booking_payments"),
            ),
            dimension_specs=(),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_node.parent_node,
    )
