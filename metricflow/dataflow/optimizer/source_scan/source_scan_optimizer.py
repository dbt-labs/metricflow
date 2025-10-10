from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
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
from metricflow.dataflow.optimizer.dataflow_plan_optimizer import DataflowPlanOptimizer
from metricflow.dataflow.optimizer.source_scan.cm_branch_combiner import (
    ComputeMetricsBranchCombiner,
    ComputeMetricsBranchCombinerResult,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OptimizeBranchResult:  # noqa: D101
    optimized_branch: DataflowPlanNode


@dataclass(frozen=True)
class BranchCombinationResult:
    """Holds the results of combining a branch (right_branch) with one of the branches in a list (left_branch)."""

    left_branch: DataflowPlanNode
    right_branch: DataflowPlanNode
    combined_branch: Optional[DataflowPlanNode] = None


class SourceScanOptimizer(
    DataflowPlanNodeVisitor[OptimizeBranchResult],
    DataflowPlanOptimizer,
):
    """Reduces the number of scans (ReadSqlSourceNodes) in a dataflow plan.

    This attempts to reduce the number of scans by combining the parent nodes of CombineAggregatedOutputsNode via the
    ComputeMetricsBranchCombiner.

    A plan with a structure similar to
        ...
        <CombineAggregatedOutputsNode>
            <ComputeMetricsNode metrics="[metric0]">
                <AggregateSimpleMetricInputsNode>
                ...
                </AggregateSimpleMetricInputsNode>
            </ComputeMetricsNode>
            <ComputeMetricsNode metrics="[metrics1]">
                <AggregateSimpleMetricInputsNode>
                ...
                </AggregateSimpleMetricInputsNode>
            </ComputeMetricsNode>
            <ComputeMetricsNode metric="[metrics2]">
                <AggregateSimpleMetricInputsNode>
                ...
                </AggregateSimpleMetricInputsNode>
            </ComputeMetricsNode>
        </CombineAggregatedOutputsNode>
        ...
    will be converted to
        ...
        <CombineAggregatedOutputsNode>
            <ComputeMetricsNode metrics="[metric0, metric1]">
                <AggregateSimpleMetricInputsNode>
                ...
                </AggregateSimpleMetricInputsNode>
            </ComputeMetricsNode>
            <ComputeMetricsNode metrics="[metric2]">
                <AggregateSimpleMetricInputsNode>
                ...
                </AggregateSimpleMetricInputsNode>
            </ComputeMetricsNode>
        </CombineAggregatedOutputsNode>
        ...
    when possible.

    In cases where all ComputeMetricsNodes can be combined into a single one, the CombineAggregatedOutputsNode may be removed as
    well.

    This traverses the dataflow plan using DFS. When visiting a node (current_node), it first runs the optimization
    process on the parent branches, then tries to create a more optimal version of the current_node with those optimized
    parents.
    """

    def __init__(self) -> None:  # noqa: D107
        self._node_to_result: Dict[DataflowPlanNode, OptimizeBranchResult] = {}

    def _log_visit_node_type(self, node: DataflowPlanNode) -> None:
        logger.debug(LazyFormat(lambda: f"Visiting {node.node_id}"))

    def _default_base_output_handler(
        self,
        node: DataflowPlanNode,
    ) -> OptimizeBranchResult:
        memoized_result = self._node_to_result.get(node)
        if memoized_result is not None:
            return memoized_result

        optimized_parent_nodes: Sequence[DataflowPlanNode] = tuple(
            parent_node.accept(self).optimized_branch for parent_node in node.parent_nodes
        )

        # If no optimization is done, use the same nodes so that common operations can be identified for CTE generation.
        if tuple(node.parent_nodes) == optimized_parent_nodes:
            result = OptimizeBranchResult(optimized_branch=node)
        else:
            result = OptimizeBranchResult(optimized_branch=node.with_new_parents(optimized_parent_nodes))

        self._node_to_result[node] = result
        return result

    def visit_source_node(self, node: ReadSqlSourceNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_aggregate_simple_metric_inputs_node(  # noqa: D102
        self, node: AggregateSimpleMetricInputsNode
    ) -> OptimizeBranchResult:
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        # Run the optimizer on the parent branch to handle derived metrics, which are defined recursively in the DAG.

        memoized_result = self._node_to_result.get(node)
        if memoized_result is not None:
            return memoized_result

        optimized_parent_result: OptimizeBranchResult = node.parent_node.accept(self)
        if optimized_parent_result.optimized_branch is not None:
            result = OptimizeBranchResult(
                optimized_branch=ComputeMetricsNode.create(
                    parent_node=optimized_parent_result.optimized_branch,
                    metric_specs=node.metric_specs,
                    for_group_by_source_node=node.for_group_by_source_node,
                    aggregated_to_elements=node.aggregated_to_elements,
                )
            )
        else:
            result = OptimizeBranchResult(optimized_branch=node)

        self._node_to_result[node] = result
        return result

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_write_to_result_data_table_node(  # noqa: D102
        self, node: WriteToResultDataTableNode
    ) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_filter_elements_node(self, node: FilterElementsNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    @staticmethod
    def _combine_branches(
        left_branches: Sequence[DataflowPlanNode], right_branch: DataflowPlanNode
    ) -> Sequence[BranchCombinationResult]:
        """Combine the right branch with one of the left branches.

        This is intended to be used in a loop where the goal is to combine a set of branches with each other in the most
        optimal way. This should be the case if the combination of branches is commutative e.g. if combining branches
        (a + b) + c is the same as a + (b + c).
        """
        results = []
        combined = False
        for left_branch in left_branches:
            # Try combining only if we haven't combined before.
            if not combined:
                combiner = ComputeMetricsBranchCombiner(left_branch_node=left_branch)
                combiner_result: ComputeMetricsBranchCombinerResult = right_branch.accept(combiner)
                if combiner_result.combined_branch is not None:
                    combined = True
                    results.append(
                        BranchCombinationResult(
                            left_branch=left_branch,
                            right_branch=right_branch,
                            combined_branch=combiner_result.combined_branch,
                        )
                    )
                    continue

            # Add to results if we didn't combine with left_branch
            results.append(
                BranchCombinationResult(
                    left_branch=left_branch,
                    right_branch=right_branch,
                )
            )
        return results

    def visit_combine_aggregated_outputs_node(  # noqa: D102
        self, node: CombineAggregatedOutputsNode
    ) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)

        memoized_result = self._node_to_result.get(node)
        if memoized_result is not None:
            return memoized_result

        # The parent node of the CombineAggregatedOutputsNode can be either ComputeMetricsNodes or
        # CombineAggregatedOutputsNodes.
        # Stores the result of running this optimizer on each parent branch separately.
        optimized_parent_branches = []
        logger.debug(LazyFormat(lambda: f"{node.node_id} has {len(node.parent_nodes)} parent branches"))

        # Run the optimizer on the parent branch to handle derived metrics, which are defined recursively in the DAG.
        for parent_branch in node.parent_nodes:
            result: OptimizeBranchResult = parent_branch.accept(self)
            optimized_parent_branches.append(result.optimized_branch)

        # Try to combine (using ComputeMetricsBranchCombiner) as many parent branches as possible in a
        # greedy N^2 approach. The optimality of this approach needs more thought to prove conclusively, but given
        # the seemingly transitive properties of the combination operation, this seems reasonable.
        combined_parent_branches: List[DataflowPlanNode] = []
        for optimized_parent_branch in optimized_parent_branches:
            combination_results = SourceScanOptimizer._combine_branches(
                left_branches=combined_parent_branches, right_branch=optimized_parent_branch
            )

            # If optimized_parent_branch couldn't be combined with any of the existing ones, add it to the list.
            if not any(x.combined_branch is not None for x in combination_results):
                combined_parent_branches.append(optimized_parent_branch)
            # Otherwise, replaced the branch with the one that was combined in combined_parent_branches
            else:
                combined_parent_branches = [
                    (
                        branch_combination_result.left_branch
                        if branch_combination_result.combined_branch is None
                        else branch_combination_result.combined_branch
                    )
                    for branch_combination_result in combination_results
                ]

        logger.debug(
            LazyFormat(
                "Possible branches combined.",
                count_of_branches_before_combination=len(optimized_parent_branches),
                count_of_branches_after_combination=len(combined_parent_branches),
            )
        )

        assert len(combined_parent_branches) > 0

        # If we were able to reduce the parent branches of the CombineAggregatedOutputsNode into a single one, there's
        # no need for a CombineAggregatedOutputsNode.
        if len(combined_parent_branches) == 1:
            result = OptimizeBranchResult(optimized_branch=combined_parent_branches[0])
        else:
            result = OptimizeBranchResult(
                optimized_branch=CombineAggregatedOutputsNode.create(parent_nodes=combined_parent_branches)
            )

        self._node_to_result[node] = result
        return result

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_metric_time_dimension_transform_node(  # noqa: D102
        self, node: MetricTimeDimensionTransformNode
    ) -> OptimizeBranchResult:
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def optimize(self, dataflow_plan: DataflowPlan) -> DataflowPlan:  # noqa: D102
        optimized_result: OptimizeBranchResult = dataflow_plan.sink_node.accept(self)

        logger.debug(
            LazyFormat(
                "Optimized dataflow plan",
                original_plan=dataflow_plan.sink_node.structure_text,
                optimized_plan=optimized_result.optimized_branch.structure_text,
            )
        )

        return DataflowPlan(
            plan_id=DagId.from_id_prefix(StaticIdPrefix.OPTIMIZED_DATAFLOW_PLAN_PREFIX),
            sink_nodes=[optimized_result.optimized_branch],
        )

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_add_generated_uuid_column_node(  # noqa: D102
        self, node: AddGeneratedUuidColumnNode
    ) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_join_to_custom_granularity_node(  # noqa: D102
        self, node: JoinToCustomGranularityNode
    ) -> OptimizeBranchResult:
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_min_max_node(self, node: MinMaxNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_alias_specs_node(self, node: AliasSpecsNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_offset_base_grain_by_custom_grain_node(  # noqa: D102
        self, node: OffsetBaseGrainByCustomGrainNode
    ) -> OptimizeBranchResult:
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_offset_custom_granularity_node(  # noqa: D102
        self, node: OffsetCustomGranularityNode
    ) -> OptimizeBranchResult:
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)
