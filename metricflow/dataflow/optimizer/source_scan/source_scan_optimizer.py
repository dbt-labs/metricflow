from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence

from metricflow.dag.id_generation import OPTIMIZED_DATAFLOW_PLAN_PREFIX, IdGeneratorRegistry
from metricflow.dataflow.dataflow_plan import (
    AggregateMeasuresNode,
    BaseOutput,
    CombineMetricsNode,
    ComputeMetricsNode,
    ConstrainTimeRangeNode,
    DataflowPlan,
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
    FilterElementsNode,
    JoinAggregatedMeasuresByGroupByColumnsNode,
    JoinOverTimeRangeNode,
    JoinToBaseOutputNode,
    JoinToTimeSpineNode,
    MetricTimeDimensionTransformNode,
    OrderByLimitNode,
    ReadSqlSourceNode,
    SemiAdditiveJoinNode,
    SinkOutput,
    WhereConstraintNode,
    WriteToResultDataframeNode,
    WriteToResultTableNode,
)
from metricflow.dataflow.dataflow_plan_to_text import dataflow_dag_as_text
from metricflow.dataflow.optimizer.dataflow_plan_optimizer import DataflowPlanOptimizer
from metricflow.dataflow.optimizer.source_scan.cm_branch_combiner import (
    ComputeMetricsBranchCombiner,
    ComputeMetricsBranchCombinerResult,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OptimizeBranchResult:  # noqa: D
    base_output_node: Optional[BaseOutput] = None
    sink_node: Optional[SinkOutput] = None

    @property
    def checked_base_output(self) -> BaseOutput:  # noqa: D
        assert self.base_output_node, f"Expected the result of traversal to produce a {BaseOutput}"
        return self.base_output_node

    @property
    def checked_sink_node(self) -> SinkOutput:  # noqa: D
        assert self.sink_node, f"Expected the result of traversal to produce a {SinkOutput}"
        return self.sink_node


@dataclass(frozen=True)
class BranchCombinationResult:
    """Holds the results of combining a branch (right_branch) with one of the branches in a list (left_branch)."""

    left_branch: BaseOutput
    right_branch: BaseOutput
    combined_branch: Optional[BaseOutput] = None


class SourceScanOptimizer(
    DataflowPlanNodeVisitor[OptimizeBranchResult],
    DataflowPlanOptimizer,
):
    """Reduces the number of scans (ReadSqlSourceNodes) in a dataflow plan.

    This attempts to reduce the number of scans by combining the parent nodes of CombineMetricsNode via the
    ComputeMetricsBranchCombiner.

    A plan with a structure similar to
        ...
        <CombineMetricsNode>
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
        </CombineMetricsNode>
        ...
    will be converted to
        ...
        <CombineMetricsNode>
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
        </CombineMetricsNode>
        ...
    when possible.

    In cases where all ComputeMetricsNodes can be combined into a single one, the CombineMetricsNode may be removed as
    well.

    This traverses the dataflow plan using DFS. When visiting a node (current_node), it first runs the optimization
    process on the parent branches, then tries to create a more optimal version of the current_node with those optimized
    parents.
    """

    def __init__(self) -> None:  # noqa: D
        self._log_level = logging.DEBUG

    def _log_visit_node_type(self, node: DataflowPlanNode) -> None:
        logger.log(level=self._log_level, msg=f"Visiting {node}")

    def _default_base_output_handler(
        self,
        node: BaseOutput,
    ) -> OptimizeBranchResult:
        optimized_parents: Sequence[OptimizeBranchResult] = tuple(
            parent_node.accept(self) for parent_node in node.parent_nodes
        )
        # Parents should always be BaseOutput
        return OptimizeBranchResult(
            base_output_node=node.with_new_parents(tuple(x.checked_base_output for x in optimized_parents))
        )

    def _default_sink_node_handler(
        self,
        node: SinkOutput,
    ) -> OptimizeBranchResult:
        optimized_parents: Sequence[OptimizeBranchResult] = tuple(
            parent_node.accept(self) for parent_node in node.parent_nodes
        )
        # Parents should always be BaseOutput
        return OptimizeBranchResult(
            sink_node=node.with_new_parents(tuple(x.checked_base_output for x in optimized_parents))
        )

    def visit_source_node(self, node: ReadSqlSourceNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_join_aggregated_measures_by_groupby_columns_node(  # noqa: D
        self, node: JoinAggregatedMeasuresByGroupByColumnsNode
    ) -> OptimizeBranchResult:
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        # Run the optimizer on the parent branch to handle derived metrics, which are defined recursively in the DAG.
        optimized_parent_result: OptimizeBranchResult = node.parent_node.accept(self)
        if optimized_parent_result.base_output_node is not None:
            return OptimizeBranchResult(
                base_output_node=ComputeMetricsNode(
                    parent_node=optimized_parent_result.base_output_node,
                    metric_specs=node.metric_specs,
                )
            )

        return OptimizeBranchResult(base_output_node=node)

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_sink_node_handler(node)

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_sink_node_handler(node)

    def visit_pass_elements_filter_node(self, node: FilterElementsNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    @staticmethod
    def _combine_branches(
        left_branches: Sequence[BaseOutput], right_branch: BaseOutput
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

    def visit_combine_metrics_node(self, node: CombineMetricsNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        # The parent node of the CombineMetricsNode can be either ComputeMetricsNodes or CombineMetricsNodes

        # Stores the result of running this optimizer on each parent branch separately.
        optimized_parent_branches = []
        logger.log(level=self._log_level, msg=f"{node} has {len(node.parent_nodes)} parent branches")

        # Run the optimizer on the parent branch to handle derived metrics, which are defined recursively in the DAG.
        for parent_branch in node.parent_nodes:
            result: OptimizeBranchResult = parent_branch.accept(self)

            assert result.sink_node is None, (
                f"Traversing the parents of of {node.__class__.__name__} should not have produced any "
                f"{SinkOutput.__class__.__name__} nodes"
            )

            assert (
                result.base_output_node is not None
            ), f"Traversing the parents of a CombineMetricsNode should always produce a BaseOutput. Got: {result}"
            optimized_parent_branches.append(result.base_output_node)

        # Try to combine (using ComputeMetricsBranchCombiner) as many parent branches as possible in a
        # greedy N^2 approach. The optimality of this approach needs more thought to prove conclusively, but given
        # the seemingly transitive properties of the combination operation, this seems reasonable.
        combined_parent_branches: List[BaseOutput] = []
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
                    branch_combination_result.left_branch
                    if branch_combination_result.combined_branch is None
                    else branch_combination_result.combined_branch
                    for branch_combination_result in combination_results
                ]

        logger.log(level=self._log_level, msg=f"Got {len(combined_parent_branches)} branches after combination")
        assert len(combined_parent_branches) > 0

        # If we were able to reduce the parent branches of the CombineMetricsNode into a single one, there's no need
        # for a CombineMetricsNode.
        if len(combined_parent_branches) == 1:
            return OptimizeBranchResult(base_output_node=combined_parent_branches[0])

        return OptimizeBranchResult(
            base_output_node=CombineMetricsNode(parent_nodes=combined_parent_branches, join_type=node.join_type)
        )

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_metric_time_dimension_transform_node(  # noqa: D
        self, node: MetricTimeDimensionTransformNode
    ) -> OptimizeBranchResult:
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def optimize(self, dataflow_plan: DataflowPlan) -> DataflowPlan:  # noqa: D
        optimized_result: OptimizeBranchResult = dataflow_plan.sink_output_node.accept(self)

        logger.log(
            level=self._log_level,
            msg=f"Optimized:\n\n"
            f"{dataflow_dag_as_text(dataflow_plan.sink_output_node)}\n\n"
            f"to:\n\n"
            f"{dataflow_dag_as_text(optimized_result.checked_sink_node)}",
        )

        plan_id = IdGeneratorRegistry.for_class(self.__class__).create_id(OPTIMIZED_DATAFLOW_PLAN_PREFIX)
        logger.log(level=self._log_level, msg=f"Optimized plan ID is {plan_id}")
        if optimized_result.sink_node:
            return DataflowPlan(
                plan_id=plan_id,
                sink_output_nodes=[optimized_result.sink_node],
            )
        logger.log(level=self._log_level, msg="Optimizer didn't produce a result, so returning the same plan")
        return DataflowPlan(
            plan_id=plan_id,
            sink_output_nodes=[dataflow_plan.sink_output_node],
        )

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> OptimizeBranchResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)
