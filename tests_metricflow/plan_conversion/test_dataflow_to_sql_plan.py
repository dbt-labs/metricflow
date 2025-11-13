from __future__ import annotations

from typing import List, Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.references import EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import LinklessEntitySpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec
from metricflow_semantics.specs.order_by_spec import OrderBySpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.aggregation_helper import NullFillValueMapping
from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
from metricflow.dataflow.nodes.aggregate_simple_metric_inputs import AggregateSimpleMetricInputsNode
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_to_base import JoinDescription, JoinOnEntitiesNode
from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataset.dataset_classes import DataSet
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from tests_metricflow.dataflow_plan_to_svg import display_graph_if_requested
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.sql.compare_sql_plan import assert_rendered_sql_from_plan_equal, assert_sql_plan_text_equal


def convert_and_check(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    node: DataflowPlanNode,
) -> None:
    """Convert the dataflow plan to SQL and compare with snapshots."""
    # Generate plans w/o optimizers
    conversion_result = dataflow_to_sql_converter.convert_to_sql_plan(
        sql_engine_type=sql_client.sql_engine_type,
        sql_query_plan_id=DagId.from_str("plan0"),
        dataflow_plan_node=node,
        optimization_level=SqlOptimizationLevel.O0,
    )
    sql_query_plan = conversion_result.sql_plan
    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=sql_query_plan,
    )

    assert_sql_plan_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_query_plan=sql_query_plan,
    )

    assert_rendered_sql_from_plan_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_query_plan=sql_query_plan,
        sql_client=sql_client,
    )

    # Generate plans with optimizers
    conversion_result = dataflow_to_sql_converter.convert_to_sql_plan(
        sql_engine_type=sql_client.sql_engine_type,
        sql_query_plan_id=DagId.from_str("plan0_optimized"),
        dataflow_plan_node=node,
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


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_source_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a single source node."""
    source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=source_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_filter_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf pass filter node."""
    simple_metric_input_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )
    source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]
    filter_node = FilterElementsNode.create(
        parent_node=source_node, include_specs=InstanceSpecSet(simple_metric_input_specs=(simple_metric_input_spec,))
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=filter_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_filter_with_where_constraint_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf pass filter node."""
    simple_metric_input_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )
    source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]

    ds_spec = TimeDimensionSpec(
        element_name="ds",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )
    filter_node = FilterElementsNode.create(
        parent_node=source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(simple_metric_input_spec,), time_dimension_specs=(ds_spec,)
        ),
    )  # need to include ds_spec because where constraint operates on ds
    time_grain = ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY)
    where_constraint_node = WhereConstraintNode.create(
        parent_node=filter_node,
        where_specs=(
            WhereFilterSpec(
                where_sql="booking__ds__day = '2020-01-01'",
                bind_parameters=SqlBindParameterSet(),
                element_set=GroupByItemSet(
                    annotated_specs=(
                        AnnotatedSpec.create(
                            element_type=LinkableElementType.TIME_DIMENSION,
                            element_name="ds",
                            entity_links=(EntityReference(element_name="booking"),),
                            time_grain=time_grain,
                            date_part=None,
                            metric_subquery_entity_links=None,
                            properties=(),
                            origin_model_ids=(),
                            derived_from_semantic_models=(),
                        ),
                    ),
                ),
            ),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=where_constraint_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_simple_metric_aggregation_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf simple-metric input aggregation node.

    Covers SUM, AVERAGE, SUM_BOOLEAN (transformed to SUM upstream), and COUNT_DISTINCT agg types
    """
    sum_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )
    sum_boolean_spec = SimpleMetricInputSpec(
        element_name="instant_bookings",
    )
    avg_spec = SimpleMetricInputSpec(
        element_name="average_booking_value",
    )
    count_distinct_spec = SimpleMetricInputSpec(
        element_name="bookers",
    )
    simple_metric_input_specs: List[SimpleMetricInputSpec] = [sum_spec, sum_boolean_spec, avg_spec, count_distinct_spec]
    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["bookings_source"]
    filtered_measure_node = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(simple_metric_input_specs=tuple(simple_metric_input_specs)),
    )

    aggregated_measure_node = AggregateSimpleMetricInputsNode.create(
        parent_node=filtered_measure_node,
        null_fill_value_mapping=NullFillValueMapping.create(),
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=aggregated_measure_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_single_join_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 simple-metric input and 1 dimension."""
    simple_metric_input_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["bookings_source"]
    filtered_measure_node = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(simple_metric_input_spec,),
            entity_specs=(entity_spec,),
        ),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(EntityReference("listing"),),
    )
    dimension_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "listings_latest"
    ]
    filtered_dimension_node = FilterElementsNode.create(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinOnEntitiesNode.create(
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
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=join_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_multi_join_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 simple-metric input and 2 dimensions."""
    simple_metric_input_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["bookings_source"]
    filtered_measure_node = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(simple_metric_input_spec,), entity_specs=(entity_spec,)
        ),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(),
    )
    dimension_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "listings_latest"
    ]
    filtered_dimension_node = FilterElementsNode.create(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinOnEntitiesNode.create(
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
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=join_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_compute_metrics_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf compute metrics node."""
    simple_metric_input_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["bookings_source"]
    filtered_measure_node = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(simple_metric_input_spec,),
            entity_specs=(entity_spec,),
        ),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(),
    )
    dimension_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "listings_latest"
    ]
    filtered_dimension_node = FilterElementsNode.create(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinOnEntitiesNode.create(
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

    aggregated_measure_node = AggregateSimpleMetricInputsNode.create(
        parent_node=join_node,
        null_fill_value_mapping=NullFillValueMapping.create(),
    )

    metric_spec = MetricSpec(element_name="bookings")
    compute_metrics_node = ComputeMetricsNode.create(
        parent_node=aggregated_measure_node,
        metric_specs=[metric_spec],
        aggregated_to_elements={entity_spec, dimension_spec},
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=compute_metrics_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_compute_metrics_node_simple_expr(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests the compute metrics node for expr type metrics sourced from a single simple-metric input."""
    simple_metric_input_spec = SimpleMetricInputSpec(
        element_name="booking_value",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["bookings_source"]
    filtered_measure_node = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(simple_metric_input_spec,), entity_specs=(entity_spec,)
        ),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(),
    )
    dimension_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "listings_latest"
    ]
    filtered_dimension_node = FilterElementsNode.create(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinOnEntitiesNode.create(
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

    aggregated_measures_node = AggregateSimpleMetricInputsNode.create(
        parent_node=join_node,
        null_fill_value_mapping=NullFillValueMapping.create(),
    )
    metric_spec = MetricSpec(element_name="booking_fees")
    compute_metrics_node = ComputeMetricsNode.create(
        parent_node=aggregated_measures_node,
        metric_specs=[metric_spec],
        aggregated_to_elements={entity_spec, dimension_spec},
    )

    sink_node = WriteToResultDataTableNode.create(compute_metrics_node)
    dataflow_plan = DataflowPlan(sink_nodes=[sink_node], plan_id=DagId.from_str("plan0"))

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=compute_metrics_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_compute_metrics_node_ratio_from_single_semantic_model(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests the compute metrics node for ratio type metrics sourced from a single semantic model."""
    numerator_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )
    denominator_spec = SimpleMetricInputSpec(
        element_name="bookers",
    )
    entity_spec = LinklessEntitySpec.from_element_name(element_name="listing")
    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["bookings_source"]
    filtered_measures_node = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(numerator_spec, denominator_spec), entity_specs=(entity_spec,)
        ),
    )

    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(),
    )
    dimension_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "listings_latest"
    ]
    filtered_dimension_node = FilterElementsNode.create(
        parent_node=dimension_source_node,
        include_specs=InstanceSpecSet(
            entity_specs=(entity_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    join_node = JoinOnEntitiesNode.create(
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

    aggregated_measures_node = AggregateSimpleMetricInputsNode.create(
        parent_node=join_node,
        null_fill_value_mapping=NullFillValueMapping.create(),
    )
    metric_spec = MetricSpec(element_name="bookings_per_booker")
    compute_metrics_node = ComputeMetricsNode.create(
        parent_node=aggregated_measures_node,
        metric_specs=[metric_spec],
        aggregated_to_elements={entity_spec, dimension_spec},
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=compute_metrics_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_order_by_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a leaf compute metrics node."""
    simple_metric_input_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )

    dimension_spec = DimensionSpec(
        element_name="is_instant",
        entity_links=(),
    )

    time_dimension_spec = TimeDimensionSpec(
        element_name="ds",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )
    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["bookings_source"]

    filtered_measure_node = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(simple_metric_input_spec,),
            dimension_specs=(dimension_spec,),
            time_dimension_specs=(time_dimension_spec,),
        ),
    )

    aggregated_measure_node = AggregateSimpleMetricInputsNode.create(
        parent_node=filtered_measure_node,
        null_fill_value_mapping=NullFillValueMapping.create(),
    )

    metric_spec = MetricSpec(element_name="bookings")
    compute_metrics_node = ComputeMetricsNode.create(
        parent_node=aggregated_measure_node,
        metric_specs=[metric_spec],
        aggregated_to_elements={dimension_spec, time_dimension_spec},
    )

    order_by_node = OrderByLimitNode.create(
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
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=order_by_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_by_custom_granularity_node(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
) -> None:
    offset_base_grain_by_custom_grain_node = dataflow_plan_builder.build_custom_offset_time_spine_node(
        offset_window=PydanticMetricTimeWindow(count=3, granularity="alien_day"),
        required_time_spine_specs=(
            DataSet.metric_time_dimension_spec(ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH)),
        ),
        use_offset_custom_granularity_node=False,
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=offset_base_grain_by_custom_grain_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_custom_granularity_node(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
) -> None:
    offset_base_grain_by_custom_grain_node = dataflow_plan_builder.build_custom_offset_time_spine_node(
        offset_window=PydanticMetricTimeWindow(count=3, granularity="alien_day"),
        required_time_spine_specs=(
            DataSet.metric_time_dimension_spec(
                ExpandedTimeGranularity(name="alien_day", base_granularity=TimeGranularity.DAY)
            ),
        ),
        use_offset_custom_granularity_node=True,
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=offset_base_grain_by_custom_grain_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_semi_additive_join_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan using a SemiAdditiveJoinNode."""
    non_additive_dimension_spec = NonAdditiveDimensionSpec(name="ds", window_choice=AggregationType.MIN)
    time_dimension_spec = TimeDimensionSpec(
        element_name="ds",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )

    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["accounts_source"]
    semi_additive_join_node = SemiAdditiveJoinNode.create(
        parent_node=simple_metric_input_source_node,
        entity_specs=tuple(),
        time_dimension_spec=time_dimension_spec,
        agg_by_function=non_additive_dimension_spec.window_choice,
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=semi_additive_join_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_semi_additive_join_node_with_queried_group_by(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan using a SemiAdditiveJoinNode."""
    non_additive_dimension_spec = NonAdditiveDimensionSpec(name="ds", window_choice=AggregationType.MIN)
    time_dimension_spec = TimeDimensionSpec(
        element_name="ds",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )
    queried_time_dimension_spec = TimeDimensionSpec(
        element_name="ds",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.WEEK),
    )

    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["accounts_source"]
    semi_additive_join_node = SemiAdditiveJoinNode.create(
        parent_node=simple_metric_input_source_node,
        entity_specs=tuple(),
        time_dimension_spec=time_dimension_spec,
        agg_by_function=non_additive_dimension_spec.window_choice,
        queried_time_dimension_spec=queried_time_dimension_spec,
    )
    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=semi_additive_join_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_semi_additive_join_node_with_grouping(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan using a SemiAdditiveJoinNode with a window_grouping."""
    non_additive_dimension_spec = NonAdditiveDimensionSpec(
        name="ds",
        window_choice=AggregationType.MAX,
        window_groupings=("user",),
    )
    entity_spec = LinklessEntitySpec(element_name="user", entity_links=())
    time_dimension_spec = TimeDimensionSpec(
        element_name="ds",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )

    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["accounts_source"]
    semi_additive_join_node = SemiAdditiveJoinNode.create(
        parent_node=simple_metric_input_source_node,
        entity_specs=(entity_spec,),
        time_dimension_spec=time_dimension_spec,
        agg_by_function=non_additive_dimension_spec.window_choice,
    )
    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=semi_additive_join_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_constrain_time_range_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting the ConstrainTimeRangeNode to SQL."""
    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["bookings_source"]
    filtered_measure_node = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(
                SimpleMetricInputSpec(
                    element_name="bookings",
                ),
            ),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    entity_links=(),
                    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                ),
            ),
        ),
    )
    metric_time_node = MetricTimeDimensionTransformNode.create(
        parent_node=filtered_measure_node,
        aggregation_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    )

    constrain_time_node = ConstrainTimeRangeNode.create(
        parent_node=metric_time_node,
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"),
            end_time=as_datetime("2020-01-02"),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=constrain_time_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_compute_metrics_node_ratio_from_multiple_semantic_models(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
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
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
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
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_combine_output_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests combining AggregateSimpleMetricInputsNode."""
    sum_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )
    sum_boolean_spec = SimpleMetricInputSpec(
        element_name="instant_bookings",
    )
    count_distinct_spec = SimpleMetricInputSpec(
        element_name="bookers",
    )
    dimension_spec = DimensionSpec(
        element_name="is_instant",
        entity_links=(),
    )
    simple_metric_input_source_node = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].read_node_mapping["bookings_source"]

    # Build compute simple-metric inputs node
    simple_metric_input_specs: List[SimpleMetricInputSpec] = [sum_spec]
    filtered_measure_node = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=tuple(simple_metric_input_specs), dimension_specs=(dimension_spec,)
        ),
    )
    aggregated_measure_node = AggregateSimpleMetricInputsNode.create(
        parent_node=filtered_measure_node,
        null_fill_value_mapping=NullFillValueMapping.create(),
    )

    # Build agg simple-metric inputs node
    simple_metric_input_specs_2 = [sum_boolean_spec, count_distinct_spec]
    filtered_measure_node_2 = FilterElementsNode.create(
        parent_node=simple_metric_input_source_node,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=tuple(simple_metric_input_specs_2), dimension_specs=(dimension_spec,)
        ),
    )
    aggregated_measure_node_2 = AggregateSimpleMetricInputsNode.create(
        parent_node=filtered_measure_node_2,
        null_fill_value_mapping=NullFillValueMapping.create(
            {spec.element_name: 1 for spec in simple_metric_input_specs_2}
        ),
    )

    combine_output_node = CombineAggregatedOutputsNode.create([aggregated_measure_node, aggregated_measure_node_2])
    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=combine_output_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_dimensions_requiring_join(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
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
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_dimension_with_joined_where_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    """Tests querying 2 dimensions that require a join."""
    query_spec = query_parser.parse_and_validate_query(
        group_by_names=("user__home_state_latest",),
        where_constraint_strs=["{{ Dimension('listing__country_latest') }} = 'us'"],
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )
