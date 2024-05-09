from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
from metricflow.dataflow.dfs_walker import DataflowDagWalker
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
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
    DataflowDagWalker[OptimizeBranchResult],
    DataflowPlanOptimizer,
):
    """Reduces the number of scans (ReadSqlSourceNodes) in a dataflow plan.

    This attempts to reduce the number of scans by combining the parent nodes of CombineAggregatedOutputsNode via the
    ComputeMetricsBranchCombiner.

    A plan with a structure similar to
        ...
        <CombineAggregatedOutputsNode>
            <ComputeMetricsNode metrics="[metric0]">
                <AggregateMeasuresNode>
                ...
                </AggregateMeasuresNode>
            </ComputeMetricsNode>
            <ComputeMetricsNode metrics="[metrics1]">
                <AggregateMeasuresNode>
                ...
                </AggregateMeasuresNode>
            </ComputeMetricsNode>
            <ComputeMetricsNode metric="[metrics2]">
                <AggregateMeasuresNode>
                ...
                </AggregateMeasuresNode>
            </ComputeMetricsNode>
        </CombineAggregatedOutputsNode>
        ...
    will be converted to
        ...
        <CombineAggregatedOutputsNode>
            <ComputeMetricsNode metrics="[metric0, metric1]">
                <AggregateMeasuresNode>
                ...
                </AggregateMeasuresNode>
            </ComputeMetricsNode>
            <ComputeMetricsNode metrics="[metric2]">
                <AggregateMeasuresNode>
                ...
                </AggregateMeasuresNode>
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
        super().__init__(visit_log_level=logging.ERROR, default_action_recursion=True)

    @override
    def default_visit_action(
        self, current_node: DataflowPlanNode, inputs: Sequence[OptimizeBranchResult]
    ) -> OptimizeBranchResult:
        optimized_parents = inputs
        return OptimizeBranchResult(
            optimized_branch=current_node.with_new_parents(tuple(x.optimized_branch for x in optimized_parents))
        )

    def _visit_compute_metrics_node(self, node: ComputeMetricsNode) -> OptimizeBranchResult:  # noqa: D102
        # Run the optimizer on the parent branch to handle derived metrics, which are defined recursively in the DAG.
        optimized_parent_result: OptimizeBranchResult = node.parent_node.accept(self)
        if optimized_parent_result.optimized_branch is not None:
            return OptimizeBranchResult(
                optimized_branch=ComputeMetricsNode(
                    parent_node=optimized_parent_result.optimized_branch,
                    metric_specs=node.metric_specs,
                    for_group_by_source_node=node.for_group_by_source_node,
                    aggregated_to_elements=node.aggregated_to_elements,
                )
            )

        return OptimizeBranchResult(optimized_branch=node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> OptimizeBranchResult:  # noqa: D102
        result = None
        try:
            result = self._visit_compute_metrics_node(node)
            return result
        finally:
            self.log_visit_end(node, result)

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
    ) -> OptimizeBranchResult:
        inputs = self.walk_parents(node)
        self.log_visit_start(node, inputs)
        result = None
        try:
            result = self._visit_combine_aggregated_outputs_node(node, inputs)
            return result
        finally:
            self.log_visit_end(node, result)

    def _visit_combine_aggregated_outputs_node(  # noqa: D102
        self, node: CombineAggregatedOutputsNode, inputs: Sequence[OptimizeBranchResult]
    ) -> OptimizeBranchResult:
        # The parent node of the CombineAggregatedOutputsNode can be either ComputeMetricsNodes or
        # CombineAggregatedOutputsNodes

        # Stores the result of running this optimizer on each parent branch separately.
        optimized_parent_branches = tuple(input.optimized_branch for input in inputs)
        if self.should_log:
            self.log(f"{node} has {len(node.parent_nodes)} parent branches")

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

        if self.should_log:
            self.log(f"Got {len(combined_parent_branches)} branches after combination")
        assert len(combined_parent_branches) > 0

        # If we were able to reduce the parent branches of the CombineAggregatedOutputsNode into a single one, there's no need
        # for a CombineAggregatedOutputsNode.
        if len(combined_parent_branches) == 1:
            return OptimizeBranchResult(optimized_branch=combined_parent_branches[0])

        return OptimizeBranchResult(
            optimized_branch=CombineAggregatedOutputsNode(parent_nodes=combined_parent_branches)
        )

    def optimize(self, dataflow_plan: DataflowPlan) -> DataflowPlan:  # noqa: D102
        optimized_result: OptimizeBranchResult = dataflow_plan.checked_sink_node.accept(self)

        if self.should_log:
            self.log(
                f"Optimized:\n\n"
                f"{dataflow_plan.checked_sink_node.structure_text()}\n\n"
                f"to:\n\n"
                f"{optimized_result.optimized_branch.structure_text()}",
            )

        return DataflowPlan(
            plan_id=DagId.from_id_prefix(StaticIdPrefix.OPTIMIZED_DATAFLOW_PLAN_PREFIX),
            sink_nodes=[optimized_result.optimized_branch],
        )
