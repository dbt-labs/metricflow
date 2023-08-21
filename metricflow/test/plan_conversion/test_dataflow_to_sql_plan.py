from __future__ import annotations

from typing import List

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.references import EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import (
    AggregateMeasuresNode,
    BaseOutput,
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
from metricflow.dataset.dataset import DataSet
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    InstanceSpecSet,
    LinklessEntitySpec,
    MeasureSpec,
    MetricFlowQuerySpec,
    MetricInputMeasureSpec,
    MetricSpec,
    NonAdditiveDimensionSpec,
    OrderBySpec,
    TimeDimensionSpec,
)
from metricflow.specs.where_filter_transform import WhereSpecFactory
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_plan_snapshot_text_equal
from metricflow.test.sql.compare_sql_plan import assert_rendered_sql_from_plan_equal, assert_sql_plan_text_equal
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY, MTD_SPEC_QUARTER, MTD_SPEC_WEEK, MTD_SPEC_YEAR
from metricflow.time.date_part import DatePart


@pytest.fixture(scope="session")
def multihop_dataflow_to_sql_converter(  # noqa: D
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
) -> DataflowToSqlQueryPlanConverter:
    return DataflowToSqlQueryPlanConverter(
        column_association_resolver=DunderColumnAssociationResolver(multi_hop_join_semantic_manifest_lookup),
        semantic_manifest_lookup=multi_hop_join_semantic_manifest_lookup,
    )


@pytest.fixture(scope="session")
def scd_dataflow_to_sql_converter(  # noqa: D
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> DataflowToSqlQueryPlanConverter:
    return DataflowToSqlQueryPlanConverter(
        column_association_resolver=DunderColumnAssociationResolver(scd_semantic_manifest_lookup),
        semantic_manifest_lookup=scd_semantic_manifest_lookup,
    )


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
        where_constraint=(
            WhereSpecFactory(
                column_association_resolver=column_association_resolver,
            ).create_from_where_filter(
                PydanticWhereFilter(
                    where_sql_template="{{ TimeDimension('booking__ds', 'day') }} = '2020-01-01'",
                )
            )
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=where_constraint_node,
    )


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
            ),
            JoinDescription(
                join_node=filtered_dimension_node,
                join_on_entity=LinklessEntitySpec.from_element_name(element_name="listing"),
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
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
        metric_time_dimension_specs=[MTD_SPEC_DAY],
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"), end_time=as_datetime("2021-01-01")
        ),
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
        metric_time_dimension_specs=[MTD_SPEC_DAY],
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"), end_time=as_datetime("2021-01-01")
        ),
        offset_window=PydanticMetricTimeWindow(count=10, granularity=TimeGranularity.DAY),
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
        metric_time_dimension_specs=[MTD_SPEC_DAY],
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"), end_time=as_datetime("2021-01-01")
        ),
        offset_window=None,
        offset_to_grain=TimeGranularity.MONTH,
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


def test_compute_metrics_node_ratio_from_multiple_semantic_models(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests the compute metrics node for ratio type metrics.

    This test exercises the functionality provided in JoinAggregatedMeasuresByGroupByColumnsNode for
    merging multiple measures into a single input source for final metrics computation.
    """
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


def test_multihop_node(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    multihop_dataflow_plan_builder: DataflowPlanBuilder,
    multihop_dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions."""
    dataflow_plan = multihop_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="txn_count"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="customer_name",
                    entity_links=(
                        EntityReference(element_name="account_id"),
                        EntityReference(element_name="customer_id"),
                    ),
                ),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=multihop_dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_filter_with_where_constraint_on_join_dim(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(),
                ),
            ),
            where_constraint=(
                WhereSpecFactory(
                    column_association_resolver=column_association_resolver,
                ).create_from_where_filter(
                    PydanticWhereFilter(
                        where_sql_template="{{ Dimension('listing__country_latest') }} = 'us'",
                    )
                )
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


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


def test_cumulative_metric(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a cumulative metric to compute."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
            dimension_specs=(),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    entity_links=(),
                    time_granularity=TimeGranularity.MONTH,
                ),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_cumulative_metric_with_time_constraint(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a cumulative metric to compute."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
            dimension_specs=(),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    entity_links=(),
                    time_granularity=TimeGranularity.MONTH,
                ),
            ),
            time_range_constraint=TimeRangeConstraint(
                start_time=as_datetime("2020-01-01"), end_time=as_datetime("2020-01-01")
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_cumulative_metric_no_ds(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a cumulative metric to compute."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
            dimension_specs=(),
            time_dimension_specs=(),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_cumulative_metric_no_window(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a windowless cumulative metric to compute."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="revenue_all_time"),),
            dimension_specs=(),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    entity_links=(),
                    time_granularity=TimeGranularity.MONTH,
                ),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_cumulative_metric_no_window_with_time_constraint(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a windowless cumulative metric to compute."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="revenue_all_time"),),
            dimension_specs=(),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    entity_links=(),
                    time_granularity=TimeGranularity.MONTH,
                ),
            ),
            time_range_constraint=TimeRangeConstraint(
                start_time=as_datetime("2020-01-01"), end_time=as_datetime("2020-01-01")
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_cumulative_metric_grain_to_date(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where grain_to_date cumulative metric to compute."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="revenue_mtd"),),
            dimension_specs=(),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    entity_links=(),
                    time_granularity=TimeGranularity.MONTH,
                ),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_partitioned_join(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan where there's a join on a partitioned dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="identity_verifications"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="home_state",
                    entity_links=(EntityReference(element_name="user"),),
                ),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_limit_rows(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests a plan with a limit to the number of rows returned."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    entity_links=(),
                ),
            ),
            limit=1,
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_distinct_values(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests a plan to get distinct values for a dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        metric_specs=(MetricSpec(element_name="bookings"),),
        dimension_spec=DimensionSpec(
            element_name="country_latest",
            entity_links=(EntityReference(element_name="listing"),),
        ),
        limit=100,
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_local_dimension_using_local_entity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="listings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    entity_links=(EntityReference(element_name="listing"),),
                ),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


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


def test_measure_constraint(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="lux_booking_value_rate_expr"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_measure_constraint_with_reused_measure(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="instant_booking_value_ratio"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_measure_constraint_with_single_expr_and_alias(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="double_counted_delayed_bookings"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="non_referred_bookings_pct"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_nested_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="instant_plus_non_referred_bookings_pct"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_join_to_scd_dimension(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    scd_column_association_resolver: ColumnAssociationResolver,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests conversion of a plan using a dimension with a validity window inside a measure constraint."""
    dataflow_plan = scd_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(
                    element_name="family_bookings",
                    constraint=(
                        WhereSpecFactory(
                            column_association_resolver=scd_column_association_resolver,
                        ).create_from_where_filter(
                            PydanticWhereFilter(
                                where_sql_template="{{ Dimension('listing__capacity') }} > 2",
                            )
                        )
                    ),
                ),
            ),
            time_dimension_specs=(MTD_SPEC_DAY,),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_multi_hop_through_scd_dimension(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests conversion of a plan using a dimension that is reached through an SCD table."""
    dataflow_plan = scd_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
            dimension_specs=(DimensionSpec.from_name(name="listing__user__home_state_latest"),),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_multi_hop_to_scd_dimension(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests conversion of a plan using an SCD dimension that is reached through another table."""
    dataflow_plan = scd_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
            dimension_specs=(DimensionSpec.from_name(name="listing__lux_listing__is_confirmed_lux"),),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_multiple_metrics_no_dimensions(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="listings")),
            time_range_constraint=TimeRangeConstraint(
                start_time=as_datetime("2020-01-01"), end_time=as_datetime("2020-01-01")
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_metric_with_measures_from_multiple_sources_no_dimensions(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_per_listing"),),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_common_semantic_model(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_derived_metric_with_offset_window(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_2_weeks"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_derived_metric_with_offset_to_grain(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_since_start_of_month"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_derived_metric_with_offset_window_and_offset_to_grain(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_month_start_compared_to_1_month_prior"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_derived_offset_metric_with_one_input_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_5_day_lag"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_derived_metric_with_offset_window_and_granularity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_2_weeks"),),
            time_dimension_specs=(MTD_SPEC_QUARTER,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_derived_metric_with_offset_to_grain_and_granularity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_since_start_of_month"),),
            time_dimension_specs=(MTD_SPEC_WEEK,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_derived_metric_with_offset_window_and_offset_to_grain_and_granularity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_month_start_compared_to_1_month_prior"),),
            time_dimension_specs=(MTD_SPEC_YEAR,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_derived_offset_cumulative_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="every_2_days_bookers_2_days_ago"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_simple_query_with_date_part(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DOW),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_simple_query_with_multiple_date_parts(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DAY),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DOW),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DOY),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.WEEK),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.MONTH),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.QUARTER),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.YEAR),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


def test_offset_window_with_date_part(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_2_weeks"),),
            time_dimension_specs=(
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DOW),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )
