from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import PlotTimeDimensionTransformNode
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.dataset.dataset import DataSet
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs import TimeDimensionReference, MetricFlowQuerySpec, MetricSpec, TimeDimensionSpec
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.plan_conversion.test_dataflow_to_sql_plan import convert_and_check
from metricflow.time.time_granularity import TimeGranularity


def test_plot_time_dimension_transform_node_using_primary_time(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter[DataSourceDataSet],
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a PlotTimeDimensionTransform node using the primary time dimension to SQL."""
    source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    plot_time_dimension_transform_node = PlotTimeDimensionTransformNode(
        parent_node=source_node, aggregation_time_dimension_reference=TimeDimensionReference(element_name="ds")
    )
    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=plot_time_dimension_transform_node,
    )


def test_plot_time_dimension_transform_node_using_non_primary_time(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter[DataSourceDataSet],
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a PlotTimeDimensionTransform node using a non-primary time dimension to SQL."""
    source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    plot_time_dimension_transform_node = PlotTimeDimensionTransformNode(
        parent_node=source_node,
        aggregation_time_dimension_reference=TimeDimensionReference(element_name="booking_paid_at"),
    )
    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=plot_time_dimension_transform_node,
    )


def test_simple_query_with_plot_time_dimension(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter[DataSourceDataSet],
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
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name=DataSet.plot_time_dimension_name(),
                    identifier_links=(),
                    time_granularity=TimeGranularity.DAY,
                ),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_node.parent_node,
    )
