from __future__ import annotations

from collections import defaultdict
from typing import Dict, FrozenSet, Mapping, Sequence

from typing_extensions import override

from metricflow.dataflow.dataflow_plan import DataflowPlan, DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
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
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.window_reaggregation_node import WindowReaggregationNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode


class DataflowPlanAnalyzer:
    """CLass to determine more complex properties of the dataflow plan.

    These could also be made as member methods of the dataflow plan, but this requires resolving some circular
    dependency issues to break out the functionality into separate files.
    """

    @staticmethod
    def find_common_branches(dataflow_plan: DataflowPlan) -> Sequence[DataflowPlanNode]:
        """Starting from the sink node, find the common branches that exist in the associated DAG.

        Returns a sequence for reproducibility.
        """
        counting_visitor = _CountCommonDataflowNodeVisitor()
        dataflow_plan.sink_node.accept(counting_visitor)

        node_to_common_count = counting_visitor.get_node_counts()

        common_nodes = []
        for node, count in node_to_common_count.items():
            if count > 1:
                common_nodes.append(node)

        common_branches_visitor = _FindCommonBranchesVisitor(frozenset(common_nodes))

        return tuple(dataflow_plan.sink_node.accept(common_branches_visitor).keys())


class _CountCommonDataflowNodeVisitor(DataflowPlanNodeVisitor[None]):
    """Helper visitor to build a dict from a node in the plan to the number of times it appears in the plans."""

    def __init__(self) -> None:
        self._node_to_count: Dict[DataflowPlanNode, int] = defaultdict(int)

    def get_node_counts(self) -> Mapping[DataflowPlanNode, int]:
        return self._node_to_count

    def _default_handler(self, node: DataflowPlanNode) -> None:
        for parent_node in node.parent_nodes:
            parent_node.accept(self)
        self._node_to_count[node] += 1

    @override
    def visit_source_node(self, node: ReadSqlSourceNode) -> None:
        self._default_handler(node)

    @override
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> None:
        self._default_handler(node)

    @override
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> None:
        self._default_handler(node)

    @override
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> None:
        self._default_handler(node)

    @override
    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> None:
        self._default_handler(node)

    @override
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> None:
        self._default_handler(node)

    @override
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> None:
        self._default_handler(node)

    @override
    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> None:
        self._default_handler(node)

    @override
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> None:
        self._default_handler(node)

    @override
    def visit_filter_elements_node(self, node: FilterElementsNode) -> None:
        self._default_handler(node)

    @override
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> None:
        self._default_handler(node)

    @override
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> None:
        self._default_handler(node)

    @override
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> None:
        self._default_handler(node)

    @override
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> None:
        self._default_handler(node)

    @override
    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> None:
        self._default_handler(node)

    @override
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> None:
        self._default_handler(node)

    @override
    def visit_min_max_node(self, node: MinMaxNode) -> None:
        self._default_handler(node)

    @override
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> None:
        self._default_handler(node)

    @override
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> None:
        self._default_handler(node)

    @override
    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> None:
        self._default_handler(node)


class _FindCommonBranchesVisitor(DataflowPlanNodeVisitor[Dict[DataflowPlanNode, None]]):
    """Given the nodes that are known to appear in the DAG multiple times, find the common branches.

    A dict is used instead of a set for iteration consistency in the returned result.
    """

    def __init__(self, common_nodes: FrozenSet[DataflowPlanNode]) -> None:
        self._common_nodes = common_nodes

    def _default_handler(self, node: DataflowPlanNode) -> Dict[DataflowPlanNode, None]:
        if node in self._common_nodes:
            return {node: None}

        common_branches: Dict[DataflowPlanNode, None] = {}

        self_re_typed: DataflowPlanNodeVisitor[Dict[DataflowPlanNode, None]] = self
        for parent_node in node.parent_nodes:
            common_branches.update(parent_node.accept(self_re_typed))

        return common_branches

    @override
    def visit_source_node(self, node: ReadSqlSourceNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_filter_elements_node(self, node: FilterElementsNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_metric_time_dimension_transform_node(
        self, node: MetricTimeDimensionTransformNode
    ) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_min_max_node(self, node: MinMaxNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)

    @override
    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> Dict[DataflowPlanNode, None]:
        return self._default_handler(node)
