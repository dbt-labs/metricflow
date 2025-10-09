from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import EntityReference
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_DAY
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import DataflowPlan, DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_simple_metric_inputs import AggregateSimpleMetricInputsNode
from metricflow.dataflow.nodes.alias_specs import AliasSpecsNode
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_conversion_events import JoinConversionEventsNode
from metricflow.dataflow.nodes.join_over_time import JoinOverTimeRangeNode
from metricflow.dataflow.nodes.join_to_base import JoinOnEntitiesNode
from metricflow.dataflow.nodes.join_to_custom_granularity import JoinToCustomGranularityNode
from metricflow.dataflow.nodes.join_to_time_spine import JoinToTimeSpineNode
from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
from metricflow.dataflow.nodes.min_max import MinMaxNode
from metricflow.dataflow.nodes.offset_base_grain_by_custom_grain import OffsetBaseGrainByCustomGrainNode
from metricflow.dataflow.nodes.offset_custom_granularity import OffsetCustomGranularityNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.window_reaggregation_node import WindowReaggregationNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.dataflow.optimizer.source_scan.source_scan_optimizer import SourceScanOptimizer
from tests_metricflow.dataflow_plan_to_svg import display_graph_if_requested

logger = logging.getLogger(__name__)


class ReadSqlSourceNodeCounter(DataflowPlanNodeVisitor[int]):
    """Counts the number of ReadSqlSourceNodes in the dataflow plan."""

    def _sum_parents(self, node: DataflowPlanNode) -> int:
        return sum(parent_node.accept(self) for parent_node in node.parent_nodes)

    def visit_source_node(self, node: ReadSqlSourceNode) -> int:  # noqa: D102
        return 1

    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_aggregate_simple_metric_inputs_node(self, node: AggregateSimpleMetricInputsNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_filter_elements_node(self, node: FilterElementsNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_min_max_node(self, node: MinMaxNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> int:  # noqa :D
        return self._sum_parents(node)

    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_alias_specs_node(self, node: AliasSpecsNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_offset_base_grain_by_custom_grain_node(self, node: OffsetBaseGrainByCustomGrainNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def visit_offset_custom_granularity_node(self, node: OffsetCustomGranularityNode) -> int:  # noqa: D102
        return self._sum_parents(node)

    def count_source_nodes(self, dataflow_plan: DataflowPlan) -> int:  # noqa: D102
        return dataflow_plan.sink_node.accept(self)


def check_optimization(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_spec: MetricFlowQuerySpec,
    expected_num_sources_in_unoptimized: int,
    expected_num_sources_in_optimized: int,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

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

    source_counter = ReadSqlSourceNodeCounter()
    assert source_counter.count_source_nodes(dataflow_plan) == expected_num_sources_in_unoptimized

    optimizer = SourceScanOptimizer()
    optimized_dataflow_plan = optimizer.optimize(dataflow_plan)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=optimized_dataflow_plan,
        plan_snapshot_text=optimized_dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=optimized_dataflow_plan,
    )
    assert source_counter.count_source_nodes(optimized_dataflow_plan) == expected_num_sources_in_optimized


@pytest.mark.sql_engine_snapshot
def test_2_metrics_from_1_semantic_model(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimizing the plan for 2 simple-metrics that are defined from the same model.

    Since they are defined from the same model, the number of scans should be halved.
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(
                MTD_SPEC_DAY,
                DimensionSpec(element_name="country_latest", entity_links=(EntityReference("listing"),)),
            ),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_2_metrics_from_2_semantic_models(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 2 metrics from 2 different semantic models results in 2 scans."""
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="listings")),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_3_metrics_from_2_semantic_models(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 3 metrics from the 2 different semantic models results in 2 scans."""
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings"),
                MetricSpec(element_name="booking_value"),
                MetricSpec(element_name="listings"),
            ),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=3,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_constrained_metric_not_combined(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests that 2 metrics from the same semantic model but where 1 is constrained results in 2 scans.

    If there is a constraint for a metric, it needs to be handled in a separate query because the constraint applies to
    all rows.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("booking_value", "instant_booking_value"),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
    ).query_spec
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of a query that use a derived metrics with simple-metric inputs coming from a single semantic model.

    non_referred_bookings_pct is a derived metric that uses simple metrics `[bookings, referred_bookings]`
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="non_referred_bookings_pct"),),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )


@pytest.mark.sql_engine_snapshot
def test_nested_derived_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of a query that use a nested derived metric from a single semantic model.

    The optimal solution would reduce this to 1 source scan, but there are challenges with derived metrics e.g. aliases,
    so that is left as a future improvement.
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="instant_plus_non_referred_bookings_pct"),),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_non_derived_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of queries that use derived metrics and non-derived metrics.

    `non_referred_bookings_pct` is a derived metric that uses metrics [bookings, referred_bookings]
    `booking_value` is a simple metric.

    All of these simple metrics are defined from a common semantic model.

    Computation of `non_referred_bookings_pct` can be optimized to a single source, but isn't combined with the
    computation for `booking_value` as it's not yet supported e.g. alias needed to be handled.
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="booking_value"),
                MetricSpec(element_name="non_referred_bookings_pct"),
            ),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=3,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_same_alias_components_combined(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of querying 2 metrics which give the same alias to the same thing in their components.

    In this case we DO combine source nodes, since the components are the same exact thing so we don't need to
    scan over it twice
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="derived_shared_alias_1a"),
                MetricSpec(element_name="derived_shared_alias_1b"),
            ),
            dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_same_alias_components_not_combined(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of querying 2 metrics which give the same alias different things in their components.

    In this case we should NOT combine source nodes, since this would generate two columns with
    the same alias.
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="derived_shared_alias_1a"),
                MetricSpec(element_name="derived_shared_alias_2"),
            ),
            dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_2_ratio_metrics_from_1_semantic_model(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 2 ratio metrics with simple-metric inputs from a 1 semantic model result in 1 scan."""
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings_per_booker"),
                MetricSpec(element_name="bookings_per_dollar"),
            ),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=1,
    )


@pytest.mark.sql_engine_snapshot
def test_duplicate_simple_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a case where derived metrics in a query use the same simple-metric input (in the same form e.g. filters)."""
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="derived_bookings_0"),
                MetricSpec(element_name="derived_bookings_1"),
            ),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )
