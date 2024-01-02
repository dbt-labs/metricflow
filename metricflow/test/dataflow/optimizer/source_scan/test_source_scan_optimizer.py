from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import (
    AddGeneratedUuidColumnNode,
    AggregateMeasuresNode,
    CombineAggregatedOutputsNode,
    ComputeMetricsNode,
    ConstrainTimeRangeNode,
    DataflowPlan,
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
    FilterElementsNode,
    JoinConversionEventsNode,
    JoinOverTimeRangeNode,
    JoinToBaseOutputNode,
    JoinToTimeSpineNode,
    MetricTimeDimensionTransformNode,
    MinMaxNode,
    OrderByLimitNode,
    ReadSqlSourceNode,
    SemiAdditiveJoinNode,
    WhereConstraintNode,
    WriteToResultDataframeNode,
    WriteToResultTableNode,
)
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.dataflow.optimizer.source_scan.source_scan_optimizer import SourceScanOptimizer
from metricflow.dataset.dataset import DataSet
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    EntityReference,
    MetricFlowQuerySpec,
    MetricSpec,
)
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_plan_snapshot_text_equal

logger = logging.getLogger(__name__)


class ReadSqlSourceNodeCounter(DataflowPlanNodeVisitor[int]):
    """Counts the number of ReadSqlSourceNodes in the dataflow plan."""

    def _sum_parents(self, node: DataflowPlanNode) -> int:
        return sum(parent_node.accept(self) for parent_node in node.parent_nodes)

    def visit_source_node(self, node: ReadSqlSourceNode) -> int:  # noqa: D
        return 1

    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_pass_elements_filter_node(self, node: FilterElementsNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_min_max_node(self, node: MinMaxNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> int:  # noqa :D
        return self._sum_parents(node)

    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> int:  # noqa: D
        return self._sum_parents(node)

    def count_source_nodes(self, dataflow_plan: DataflowPlan) -> int:  # noqa: D
        return dataflow_plan.sink_output_node.accept(self)


def check_optimization(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_spec: MetricFlowQuerySpec,
    expected_num_sources_in_unoptimized: int,
    expected_num_sources_in_optimized: int,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

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

    source_counter = ReadSqlSourceNodeCounter()
    assert source_counter.count_source_nodes(dataflow_plan) == expected_num_sources_in_unoptimized

    optimizer = SourceScanOptimizer()
    optimized_dataflow_plan = optimizer.optimize(dataflow_plan)

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=optimized_dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(optimized_dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=optimized_dataflow_plan,
    )
    assert source_counter.count_source_nodes(optimized_dataflow_plan) == expected_num_sources_in_optimized


@pytest.mark.sql_engine_snapshot
def test_2_metrics_from_1_semantic_model(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that optimizing the plan for 2 metrics from 2 measure semantic models results in half the number of scans.

    Each metric is computed from the same measure semantic model and the dimension semantic model.
    """
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(
                DataSet.metric_time_dimension_spec(TimeGranularity.DAY),
                DimensionSpec(element_name="country_latest", entity_links=(EntityReference("listing"),)),
            ),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_2_metrics_from_2_semantic_models(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 2 metrics from the 2 semantic models results in 2 scans."""
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="listings")),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_3_metrics_from_2_semantic_models(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 3 metrics from the 2 semantic models results in 2 scans."""
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings"),
                MetricSpec(element_name="booking_value"),
                MetricSpec(element_name="listings"),
            ),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=3,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_constrained_metric_not_combined(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
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
    )
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of a query that use a derived metrics with measures coming from a single semantic model.

    non_referred_bookings_pct is a derived metric that uses measures [bookings, referred_bookings]
    """
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="non_referred_bookings_pct"),),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )


@pytest.mark.sql_engine_snapshot
def test_nested_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of a query that use a nested derived metric from a single semantic model.

    The optimal solution would reduce this to 1 source scan, but there are challenges with derived metrics e.g. aliases,
    so that is left as a future improvement.
    """
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="instant_plus_non_referred_bookings_pct"),),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_non_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of queries that use derived metrics and non-derived metrics.

    non_referred_bookings_pct is a derived metric that uses measures [bookings, referred_bookings]
    booking_value is a proxy metric that uses measures [bookings]

    All these measures are from a single semantic model.

    Computation of non_referred_bookings_pct can be optimized to a single source, but isn't combined with the
    computation for booking_value as it's not yet supported e.g. alias needed to be handled.
    """
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="booking_value"),
                MetricSpec(element_name="non_referred_bookings_pct"),
            ),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=3,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_2_ratio_metrics_from_1_semantic_model(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 2 ratio metrics with measures from a 1 semantic model result in 1 scan."""
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings_per_booker"),
                MetricSpec(element_name="bookings_per_dollar"),
            ),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=1,
    )


@pytest.mark.sql_engine_snapshot
def test_duplicate_measures(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a case where derived metrics in a query use the same measure (in the same form e.g. filters)."""
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="derived_bookings_0"),
                MetricSpec(element_name="derived_bookings_1"),
            ),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )
