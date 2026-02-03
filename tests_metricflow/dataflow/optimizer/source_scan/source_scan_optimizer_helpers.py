from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_plan_snapshot_text_equal,
)

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


def check_source_scan_optimization(  # noqa: D103
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

    source_counter = _ReadSqlSourceNodeCounter()
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


class _ReadSqlSourceNodeCounter(DataflowPlanNodeVisitor[int]):
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
