from __future__ import annotations

import logging
from typing import Generic

from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import (
    SourceDataSetT,
    DataflowPlanNodeVisitor,
    MetricTimeDimensionTransformNode,
    SemiAdditiveJoinNode,
    JoinOverTimeRangeNode,
    ConstrainTimeRangeNode,
    CombineMetricsNode,
    FilterElementsNode,
    WriteToResultTableNode,
    WriteToResultDataframeNode,
    WhereConstraintNode,
    OrderByLimitNode,
    ComputeMetricsNode,
    AggregateMeasuresNode,
    JoinAggregatedMeasuresByGroupByColumnsNode,
    JoinToBaseOutputNode,
    ReadSqlSourceNode,
    DataflowPlanNode,
    DataflowPlan,
    JoinToTimeSpineNode,
)
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.dataflow.optimizer.source_scan.source_scan_optimizer import SourceScanOptimizer
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.dataset.dataset import DataSet
from metricflow.specs import (
    DimensionSpec,
    IdentifierReference,
    LinkableSpecSet,
    MetricFlowQuerySpec,
    MetricSpec,
    SpecWhereClauseConstraint,
)
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.plan_utils import assert_plan_snapshot_text_equal
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class ReadSqlSourceNodeCounter(Generic[SourceDataSetT], DataflowPlanNodeVisitor[SourceDataSetT, int]):
    """Counts the number of ReadSqlSourceNodes in the dataflow plan."""

    def _sum_parents(self, node: DataflowPlanNode[SourceDataSetT]) -> int:
        return sum(parent_node.accept(self) for parent_node in node.parent_nodes)

    def visit_source_node(self, node: ReadSqlSourceNode[SourceDataSetT]) -> int:  # noqa: D
        return 1

    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_join_aggregated_measures_by_groupby_columns_node(  # noqa: D
        self, node: JoinAggregatedMeasuresByGroupByColumnsNode[SourceDataSetT]
    ) -> int:
        return self._sum_parents(node)

    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_order_by_limit_node(self, node: OrderByLimitNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_where_constraint_node(self, node: WhereConstraintNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_pass_elements_filter_node(self, node: FilterElementsNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_combine_metrics_node(self, node: CombineMetricsNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def visit_metric_time_dimension_transform_node(  # noqa: D
        self, node: MetricTimeDimensionTransformNode[SourceDataSetT]
    ) -> int:
        return self._sum_parents(node)

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode[SourceDataSetT]) -> int:  # noqa: D
        return self._sum_parents(node)

    def count_source_nodes(self, dataflow_plan: DataflowPlan[SourceDataSetT]) -> int:  # noqa: D
        return dataflow_plan.sink_output_node.accept(self)


def check_optimization(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
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

    source_counter = ReadSqlSourceNodeCounter[DataSourceDataSet]()
    assert source_counter.count_source_nodes(dataflow_plan) == expected_num_sources_in_unoptimized

    optimizer = SourceScanOptimizer[DataSourceDataSet]()
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


def test_2_metrics_from_1_data_source(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests that optimizing the plan for 2 metrics from 2 measure data sources results in half the number of scans.

    Each metric is computed from the same measure data source and the dimension data source.
    """
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(
                DataSet.metric_time_dimension_spec(TimeGranularity.DAY),
                DimensionSpec(element_name="country_latest", identifier_links=(IdentifierReference("listing"),)),
            ),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=2,
    )


def test_2_metrics_from_2_data_sources(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests that 2 metrics from the 2 data sources results in 2 scans."""

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


def test_3_metrics_from_2_data_sources(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests that 3 metrics from the 2 data sources results in 2 scans."""

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


def test_constrained_metric_not_combined(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests that 2 metrics from the same data source but where 1 is constrained results in 2 scans.

    If there is a constraint, need needs to be handled in a separate query because the constraint applies to all rows.
    """
    check_optimization(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="booking_value"),
                MetricSpec(
                    element_name="instant_booking_value",
                    constraint=SpecWhereClauseConstraint(
                        where_condition="is_instant",
                        linkable_names=("is_instant",),
                        linkable_spec_set=LinkableSpecSet(
                            dimension_specs=(
                                DimensionSpec(
                                    element_name="is_instant",
                                    identifier_links=(),
                                ),
                            )
                        ),
                        execution_parameters=SqlBindParameters(),
                    ),
                ),
            ),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


def test_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests optimization of a query that use a derived metrics with measures coming from a single data source.

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


def test_nested_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests optimization of a query that use a nested derived metric from a single data source.

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


def test_derived_metric_with_non_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests optimization of queries that use derived metrics and non-derived metrics.

    non_referred_bookings_pct is a derived metric that uses measures [bookings, referred_bookings]
    booking_value is a proxy metric that uses measures [bookings]

    All these measures are from a single data source.

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


def test_2_ratio_metrics_from_1_data_source(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests that 2 ratio metrics with measures from a 1 data source result in 1 scan."""
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
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )
