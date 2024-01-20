from __future__ import annotations

from typing import List

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.references import EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import (
    AggregateMeasuresNode,
    BaseOutput,
    CombineAggregatedOutputsNode,
    ComputeMetricsNode,
    ConstrainTimeRangeNode,
    DataflowPlan,
    FilterElementsNode,
    JoinDescription,
    JoinToBaseOutputNode,
    JoinToTimeSpineNode,
    MetricTimeDimensionTransformNode,
    OrderByLimitNode,
    SemiAdditiveJoinNode,
    WhereConstraintNode,
    WriteToResultDataframeNode,
)
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    InstanceSpecSet,
    LinkableSpecSet,
    LinklessEntitySpec,
    MeasureSpec,
    MetricFlowQuerySpec,
    MetricInputMeasureSpec,
    MetricSpec,
    NonAdditiveDimensionSpec,
    OrderBySpec,
    TimeDimensionSpec,
    WhereFilterSpec,
)
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_plan import SqlJoinType
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_plan_snapshot_text_equal
from metricflow.test.sql.compare_sql_plan import assert_rendered_sql_from_plan_equal, assert_sql_plan_text_equal
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY


def convert_and_check(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    node: BaseOutput,
) -> None:
    """Convert the dataflow plan to SQL and compare with snapshots."""
    # Generate plans w/o optimizers
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

    assert_sql_plan_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_query_plan=sql_query_plan,
    )

    assert_rendered_sql_from_plan_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_query_plan=sql_query_plan,
        sql_client=sql_client,
    )

    # Generate plans with optimizers
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


@pytest.mark.sql_engine_snapshot
def test_source_node(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a single source node."""
    source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=source_node,
    )


@pytest.mark.sql_engine_snapshot
def test_filter_node(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf pass filter node."""
    measure_spec = MeasureSpec(
        element_name="bookings",
    )
    source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filter_node = FilterElementsNode(
        parent_node=source_node, include_specs=InstanceSpecSet(measure_specs=(measure_spec,))
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=filter_node,
    )


@pytest.mark.sql_engine_snapshot
def test_filter_with_where_constraint_node(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf pass filter node."""
    measure_spec = MeasureSpec(
        element_name="bookings",
    )
    source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]

    ds_spec = TimeDimensionSpec(element_name="ds", entity_links=(), time_granularity=TimeGranularity.DAY)
    filter_node = FilterElementsNode(
        parent_node=source_node,
        include_specs=InstanceSpecSet(measure_specs=(measure_spec,), time_dimension_specs=(ds_spec,)),
    )  # need to include ds_spec because where constraint operates on ds
    where_constraint_node = WhereConstraintNode(
        parent_node=filter_node,
        where_constraint=WhereFilterSpec(
            where_sql="booking__ds__day = '2020-01-01'",
            bind_parameters=SqlBindParameters(),
            linkable_spec_set=LinkableSpecSet(
                time_dimension_specs=(
                    TimeDimensionSpec(
                        element_name="ds",
                        entity_links=(EntityReference(element_name="booking"),),
                        time_granularity=TimeGranularity.DAY,
                    ),
                )
            ),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=where_constraint_node,
    )


@pytest.mark.sql_engine_snapshot
def test_measure_aggregation_node(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf measure aggregation node.

    Covers SUM, AVERAGE, SUM_BOOLEAN (transformed to SUM upstream), and COUNT_DISTINCT agg types
    """
    sum_spec = MeasureSpec(
        element_name="bookings",
    )
    sum_boolean_spec = MeasureSpec(
        element_name="instant_bookings",
    )
    avg_spec = MeasureSpec(
        element_name="average_booking_value",
    )
    count_distinct_spec = MeasureSpec(
        element_name="bookers",
    )
    measure_specs: List[MeasureSpec] = [sum_spec, sum_boolean_spec, avg_spec, count_distinct_spec]
    metric_input_measure_specs = tuple(MetricInputMeasureSpec(measure_spec=x) for x in measure_specs)

    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filtered_measure_node = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(measure_specs=tuple(measure_specs)),
    )

    aggregated_measure_node = AggregateMeasuresNode(
        parent_node=filtered_measure_node, metric_input_measure_specs=metric_input_measure_specs
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=aggregated_measure_node,
    )


@pytest.mark.sql_engine_snapshot
def test_single_join_node(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 1 dimension."""
    measure_spec = MeasureSpec(
        element_name="bookings",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filtered_measure_node = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(
            measure_specs=(measure_spec,),
            entity_specs=(entity_spec,),
        ),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(EntityReference("listing"),),
    )
    dimension_source_node = consistent_id_object_repository.simple_model_read_nodes["listings_latest"]
    filtered_dimension_node = FilterElementsNode(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinToBaseOutputNode(
        left_node=filtered_measure_node,
        join_targets=[
            JoinDescription(
                join_node=filtered_dimension_node,
                join_on_entity=entity_spec,
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            )
        ],
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=join_node,
    )


@pytest.mark.sql_engine_snapshot
def test_multi_join_node(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions."""
    measure_spec = MeasureSpec(
        element_name="bookings",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filtered_measure_node = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(measure_specs=(measure_spec,), entity_specs=(entity_spec,)),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(),
    )
    dimension_source_node = consistent_id_object_repository.simple_model_read_nodes["listings_latest"]
    filtered_dimension_node = FilterElementsNode(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinToBaseOutputNode(
        left_node=filtered_measure_node,
        join_targets=[
            JoinDescription(
                join_node=filtered_dimension_node,
                join_on_entity=LinklessEntitySpec.from_element_name(element_name="listing"),
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
            JoinDescription(
                join_node=filtered_dimension_node,
                join_on_entity=LinklessEntitySpec.from_element_name(element_name="listing"),
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ],
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=join_node,
    )


@pytest.mark.sql_engine_snapshot
def test_compute_metrics_node(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf compute metrics node."""
    measure_spec = MeasureSpec(
        element_name="bookings",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    metric_input_measure_specs = (MetricInputMeasureSpec(measure_spec=measure_spec),)
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filtered_measure_node = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(
            measure_specs=(measure_spec,),
            entity_specs=(entity_spec,),
        ),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(),
    )
    dimension_source_node = consistent_id_object_repository.simple_model_read_nodes["listings_latest"]
    filtered_dimension_node = FilterElementsNode(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinToBaseOutputNode(
        left_node=filtered_measure_node,
        join_targets=[
            JoinDescription(
                join_node=filtered_dimension_node,
                join_on_entity=entity_spec,
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            )
        ],
    )

    aggregated_measure_node = AggregateMeasuresNode(
        parent_node=join_node, metric_input_measure_specs=metric_input_measure_specs
    )

    metric_spec = MetricSpec(element_name="bookings")
    compute_metrics_node = ComputeMetricsNode(parent_node=aggregated_measure_node, metric_specs=[metric_spec])

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=compute_metrics_node,
    )


@pytest.mark.sql_engine_snapshot
def test_compute_metrics_node_simple_expr(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests the compute metrics node for expr type metrics sourced from a single measure."""
    measure_spec = MeasureSpec(
        element_name="booking_value",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    metric_input_measure_specs = (MetricInputMeasureSpec(measure_spec=measure_spec),)
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filtered_measure_node = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(measure_specs=(measure_spec,), entity_specs=(entity_spec,)),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(),
    )
    dimension_source_node = consistent_id_object_repository.simple_model_read_nodes["listings_latest"]
    filtered_dimension_node = FilterElementsNode(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinToBaseOutputNode(
        left_node=filtered_measure_node,
        join_targets=[
            JoinDescription(
                join_node=filtered_dimension_node,
                join_on_entity=entity_spec,
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            )
        ],
    )

    aggregated_measures_node = AggregateMeasuresNode(
        parent_node=join_node, metric_input_measure_specs=metric_input_measure_specs
    )
    metric_spec = MetricSpec(element_name="booking_fees")
    compute_metrics_node = ComputeMetricsNode(parent_node=aggregated_measures_node, metric_specs=[metric_spec])

    sink_node = WriteToResultDataframeNode(compute_metrics_node)
    dataflow_plan = DataflowPlan("plan0", sink_output_nodes=[sink_node])

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=compute_metrics_node,
    )


@pytest.mark.sql_engine_snapshot
def test_join_to_time_spine_node_without_offset(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests JoinToTimeSpineNode for a single metric with offset_window."""
    measure_spec = MeasureSpec(element_name="booking_value")
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    metric_input_measure_specs = (MetricInputMeasureSpec(measure_spec=measure_spec),)
    metric_time_spec = TimeDimensionSpec(
        element_name="metric_time", entity_links=(), time_granularity=TimeGranularity.DAY
    )
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    metric_time_node = MetricTimeDimensionTransformNode(
        parent_node=measure_source_node,
        aggregation_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    )

    filtered_measure_node = FilterElementsNode(
        parent_node=metric_time_node,
        include_specs=InstanceSpecSet(
            measure_specs=(measure_spec,), entity_specs=(entity_spec,), dimension_specs=(metric_time_spec,)
        ),
    )
    aggregated_measures_node = AggregateMeasuresNode(
        parent_node=filtered_measure_node, metric_input_measure_specs=metric_input_measure_specs
    )
    metric_spec = MetricSpec(element_name="booking_fees")
    compute_metrics_node = ComputeMetricsNode(parent_node=aggregated_measures_node, metric_specs=[metric_spec])
    join_to_time_spine_node = JoinToTimeSpineNode(
        parent_node=compute_metrics_node,
        requested_metric_time_dimension_specs=[MTD_SPEC_DAY],
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"), end_time=as_datetime("2021-01-01")
        ),
        join_type=SqlJoinType.INNER,
    )
    sink_node = WriteToResultDataframeNode(join_to_time_spine_node)
    dataflow_plan = DataflowPlan("plan0", sink_output_nodes=[sink_node])

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=join_to_time_spine_node,
    )


@pytest.mark.sql_engine_snapshot
def test_join_to_time_spine_node_with_offset_window(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests JoinToTimeSpineNode for a single metric with offset_window."""
    measure_spec = MeasureSpec(element_name="booking_value")
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    metric_input_measure_specs = (MetricInputMeasureSpec(measure_spec=measure_spec),)
    metric_time_spec = TimeDimensionSpec(
        element_name="metric_time", entity_links=(), time_granularity=TimeGranularity.DAY
    )
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    metric_time_node = MetricTimeDimensionTransformNode(
        parent_node=measure_source_node,
        aggregation_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    )
    filtered_measure_node = FilterElementsNode(
        parent_node=metric_time_node,
        include_specs=InstanceSpecSet(
            measure_specs=(measure_spec,), entity_specs=(entity_spec,), dimension_specs=(metric_time_spec,)
        ),
    )
    aggregated_measures_node = AggregateMeasuresNode(
        parent_node=filtered_measure_node, metric_input_measure_specs=metric_input_measure_specs
    )
    metric_spec = MetricSpec(element_name="booking_fees")
    compute_metrics_node = ComputeMetricsNode(parent_node=aggregated_measures_node, metric_specs=[metric_spec])
    join_to_time_spine_node = JoinToTimeSpineNode(
        parent_node=compute_metrics_node,
        requested_metric_time_dimension_specs=[MTD_SPEC_DAY],
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"), end_time=as_datetime("2021-01-01")
        ),
        offset_window=PydanticMetricTimeWindow(count=10, granularity=TimeGranularity.DAY),
        join_type=SqlJoinType.INNER,
    )

    sink_node = WriteToResultDataframeNode(join_to_time_spine_node)
    dataflow_plan = DataflowPlan("plan0", sink_output_nodes=[sink_node])

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=join_to_time_spine_node,
    )


@pytest.mark.sql_engine_snapshot
def test_join_to_time_spine_node_with_offset_to_grain(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests JoinToTimeSpineNode for a single metric with offset_to_grain."""
    measure_spec = MeasureSpec(element_name="booking_value")
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    metric_input_measure_specs = (MetricInputMeasureSpec(measure_spec=measure_spec),)
    metric_time_spec = TimeDimensionSpec(
        element_name="metric_time", entity_links=(), time_granularity=TimeGranularity.DAY
    )
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    metric_time_node = MetricTimeDimensionTransformNode(
        parent_node=measure_source_node,
        aggregation_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    )
    filtered_measure_node = FilterElementsNode(
        parent_node=metric_time_node,
        include_specs=InstanceSpecSet(
            measure_specs=(measure_spec,), entity_specs=(entity_spec,), dimension_specs=(metric_time_spec,)
        ),
    )
    aggregated_measures_node = AggregateMeasuresNode(
        parent_node=filtered_measure_node, metric_input_measure_specs=metric_input_measure_specs
    )
    metric_spec = MetricSpec(element_name="booking_fees")
    compute_metrics_node = ComputeMetricsNode(parent_node=aggregated_measures_node, metric_specs=[metric_spec])
    join_to_time_spine_node = JoinToTimeSpineNode(
        parent_node=compute_metrics_node,
        requested_metric_time_dimension_specs=[MTD_SPEC_DAY],
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"), end_time=as_datetime("2021-01-01")
        ),
        offset_window=None,
        offset_to_grain=TimeGranularity.MONTH,
        join_type=SqlJoinType.INNER,
    )

    sink_node = WriteToResultDataframeNode(join_to_time_spine_node)
    dataflow_plan = DataflowPlan("plan0", sink_output_nodes=[sink_node])

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=join_to_time_spine_node,
    )


@pytest.mark.sql_engine_snapshot
def test_compute_metrics_node_ratio_from_single_semantic_model(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests the compute metrics node for ratio type metrics sourced from a single semantic model."""
    numerator_spec = MeasureSpec(
        element_name="bookings",
    )
    denominator_spec = MeasureSpec(
        element_name="bookers",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    metric_input_measure_specs = (
        MetricInputMeasureSpec(measure_spec=numerator_spec),
        MetricInputMeasureSpec(measure_spec=denominator_spec),
    )
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filtered_measures_node = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(measure_specs=(numerator_spec, denominator_spec), entity_specs=(entity_spec,)),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(),
    )
    dimension_source_node = consistent_id_object_repository.simple_model_read_nodes["listings_latest"]
    filtered_dimension_node = FilterElementsNode(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinToBaseOutputNode(
        left_node=filtered_measures_node,
        join_targets=[
            JoinDescription(
                join_node=filtered_dimension_node,
                join_on_entity=entity_spec,
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            )
        ],
    )

    aggregated_measures_node = AggregateMeasuresNode(
        parent_node=join_node, metric_input_measure_specs=metric_input_measure_specs
    )
    metric_spec = MetricSpec(element_name="bookings_per_booker")
    compute_metrics_node = ComputeMetricsNode(parent_node=aggregated_measures_node, metric_specs=[metric_spec])

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=compute_metrics_node,
    )


@pytest.mark.sql_engine_snapshot
def test_order_by_node(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf compute metrics node."""
    measure_spec = MeasureSpec(
        element_name="bookings",
    )
    metric_input_measure_specs = (MetricInputMeasureSpec(measure_spec=measure_spec),)

    dimension_spec = DimensionSpec(
        element_name="is_instant",
        entity_links=(),
    )

    time_dimension_spec = TimeDimensionSpec(
        element_name="ds",
        entity_links=(),
    )
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]

    filtered_measure_node = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(
            measure_specs=(measure_spec,),
            dimension_specs=(dimension_spec,),
            time_dimension_specs=(time_dimension_spec,),
        ),
    )

    aggregated_measure_node = AggregateMeasuresNode(
        parent_node=filtered_measure_node, metric_input_measure_specs=metric_input_measure_specs
    )

    metric_spec = MetricSpec(element_name="bookings")
    compute_metrics_node = ComputeMetricsNode(parent_node=aggregated_measure_node, metric_specs=[metric_spec])

    order_by_node = OrderByLimitNode(
        order_by_specs=[
            OrderBySpec(
                instance_spec=time_dimension_spec,
                descending=False,
            ),
            OrderBySpec(
                instance_spec=metric_spec,
                descending=True,
            ),
        ],
        parent_node=compute_metrics_node,
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=order_by_node,
    )


@pytest.mark.sql_engine_snapshot
def test_semi_additive_join_node(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan using a SemiAdditiveJoinNode."""
    non_additive_dimension_spec = NonAdditiveDimensionSpec(name="ds", window_choice=AggregationType.MIN)
    time_dimension_spec = TimeDimensionSpec(element_name="ds", entity_links=())

    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["accounts_source"]
    semi_additive_join_node = SemiAdditiveJoinNode(
        parent_node=measure_source_node,
        entity_specs=tuple(),
        time_dimension_spec=time_dimension_spec,
        agg_by_function=non_additive_dimension_spec.window_choice,
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=semi_additive_join_node,
    )


@pytest.mark.sql_engine_snapshot
def test_semi_additive_join_node_with_queried_group_by(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan using a SemiAdditiveJoinNode."""
    non_additive_dimension_spec = NonAdditiveDimensionSpec(name="ds", window_choice=AggregationType.MIN)
    time_dimension_spec = TimeDimensionSpec(element_name="ds", entity_links=())
    queried_time_dimension_spec = TimeDimensionSpec(
        element_name="ds", entity_links=(), time_granularity=TimeGranularity.WEEK
    )

    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["accounts_source"]
    semi_additive_join_node = SemiAdditiveJoinNode(
        parent_node=measure_source_node,
        entity_specs=tuple(),
        time_dimension_spec=time_dimension_spec,
        agg_by_function=non_additive_dimension_spec.window_choice,
        queried_time_dimension_spec=queried_time_dimension_spec,
    )
    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=semi_additive_join_node,
    )


@pytest.mark.sql_engine_snapshot
def test_semi_additive_join_node_with_grouping(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan using a SemiAdditiveJoinNode with a window_grouping."""
    non_additive_dimension_spec = NonAdditiveDimensionSpec(
        name="ds",
        window_choice=AggregationType.MAX,
        window_groupings=("user",),
    )
    entity_spec = LinklessEntitySpec(element_name="user", entity_links=())
    time_dimension_spec = TimeDimensionSpec(element_name="ds", entity_links=())

    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["accounts_source"]
    semi_additive_join_node = SemiAdditiveJoinNode(
        parent_node=measure_source_node,
        entity_specs=(entity_spec,),
        time_dimension_spec=time_dimension_spec,
        agg_by_function=non_additive_dimension_spec.window_choice,
    )
    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=semi_additive_join_node,
    )


@pytest.mark.sql_engine_snapshot
def test_constrain_time_range_node(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting the ConstrainTimeRangeNode to SQL."""
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filtered_measure_node = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(
            measure_specs=(
                MeasureSpec(
                    element_name="bookings",
                ),
            ),
            time_dimension_specs=(
                TimeDimensionSpec(element_name="ds", entity_links=(), time_granularity=TimeGranularity.DAY),
            ),
        ),
    )
    metric_time_node = MetricTimeDimensionTransformNode(
        parent_node=filtered_measure_node,
        aggregation_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    )

    constrain_time_node = ConstrainTimeRangeNode(
        parent_node=metric_time_node,
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"),
            end_time=as_datetime("2020-01-02"),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=constrain_time_node,
    )


@pytest.mark.sql_engine_snapshot
def test_compute_metrics_node_ratio_from_multiple_semantic_models(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests the combine metrics node for ratio type metrics."""
    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(EntityReference(element_name="listing"),),
    )
    time_dimension_spec = TimeDimensionSpec(
        element_name="ds",
        entity_links=(),
    )
    metric_spec = MetricSpec(element_name="bookings_per_view")

    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(metric_spec,),
            dimension_specs=(dimension_spec,),
            time_dimension_specs=(time_dimension_spec,),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_combine_output_node(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests combining AggregateMeasuresNode."""
    sum_spec = MeasureSpec(
        element_name="bookings",
    )
    sum_boolean_spec = MeasureSpec(
        element_name="instant_bookings",
    )
    count_distinct_spec = MeasureSpec(
        element_name="bookers",
    )
    dimension_spec = DimensionSpec(
        element_name="is_instant",
        entity_links=(),
    )
    measure_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]

    # Build compute measures node
    measure_specs: List[MeasureSpec] = [sum_spec]
    filtered_measure_node = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(measure_specs=tuple(measure_specs), dimension_specs=(dimension_spec,)),
    )
    aggregated_measure_node = AggregateMeasuresNode(
        parent_node=filtered_measure_node,
        metric_input_measure_specs=tuple(MetricInputMeasureSpec(measure_spec=x) for x in measure_specs),
    )

    # Build agg measures node
    measure_specs_2 = [sum_boolean_spec, count_distinct_spec]
    filtered_measure_node_2 = FilterElementsNode(
        parent_node=measure_source_node,
        include_specs=InstanceSpecSet(measure_specs=tuple(measure_specs_2), dimension_specs=(dimension_spec,)),
    )
    aggregated_measure_node_2 = AggregateMeasuresNode(
        parent_node=filtered_measure_node_2,
        metric_input_measure_specs=tuple(
            MetricInputMeasureSpec(measure_spec=x, fill_nulls_with=1) for x in measure_specs_2
        ),
    )

    combine_output_node = CombineAggregatedOutputsNode([aggregated_measure_node, aggregated_measure_node_2])
    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=combine_output_node,
    )


@pytest.mark.sql_engine_snapshot
def test_dimensions_requiring_join(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests querying 2 dimensions that require a join."""
    dimension_specs = (
        DimensionSpec(element_name="home_state_latest", entity_links=(EntityReference(element_name="user"),)),
        DimensionSpec(element_name="is_lux_latest", entity_links=(EntityReference(element_name="listing"),)),
    )
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(dimension_specs=dimension_specs)
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_dimension_with_joined_where_constraint(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    """Tests querying 2 dimensions that require a join."""
    query_spec = query_parser.parse_and_validate_query(
        group_by_names=("user__home_state_latest",),
        where_constraint_str="{{ Dimension('listing__country_latest') }} = 'us'",
    )
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(query_spec)

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )
