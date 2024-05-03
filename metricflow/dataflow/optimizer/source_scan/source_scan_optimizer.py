from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId

from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    DataflowPlan,
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
    SinkOutput,
)
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_conversion_events import JoinConversionEventsNode
from metricflow.dataflow.nodes.join_over_time import JoinOverTimeRangeNode
from metricflow.dataflow.nodes.join_to_base import JoinToBaseOutputNode
from metricflow.dataflow.nodes.join_to_time_spine import JoinToTimeSpineNode
from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
from metricflow.dataflow.nodes.min_max import MinMaxNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.write_to_dataframe import WriteToResultDataframeNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.dataflow.optimizer.dataflow_plan_optimizer import DataflowPlanOptimizer
from metricflow.dataflow.optimizer.source_scan.cm_branch_combiner import (
    ComputeMetricsBranchCombiner,
    ComputeMetricsBranchCombinerResult,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OptimizeBranchResult:  # noqa: D101
    base_output_node: Optional[BaseOutput] = None
    sink_node: Optional[SinkOutput] = None

    @property
    def checked_base_output(self) -> BaseOutput:  # noqa: D102
        assert self.base_output_node, f"Expected the result of traversal to produce a {BaseOutput}"
        return self.base_output_node

    @property
    def checked_sink_node(self) -> SinkOutput:  # noqa: D102
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

    def visit_source_node(self, node: ReadSqlSourceNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        # Run the optimizer on the parent branch to handle derived metrics, which are defined recursively in the DAG.
        optimized_parent_result: OptimizeBranchResult = node.parent_node.accept(self)
        if optimized_parent_result.base_output_node is not None:
            return OptimizeBranchResult(
                base_output_node=ComputeMetricsNode(
                    parent_node=optimized_parent_result.base_output_node,
                    metric_specs=node.metric_specs,
                    for_group_by_source_node=node.for_group_by_source_node,
                    is_aggregated_to_elements=node.is_aggregated_to_elements,
                )
            )

        return OptimizeBranchResult(base_output_node=node)

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)

    def visit_write_to_result_dataframe_node(  # noqa: D102
        self, node: WriteToResultDataframeNode
    ) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_sink_node_handler(node)

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_sink_node_handler(node)

    def visit_filter_elements_node(self, node: FilterElementsNode) -> OptimizeBranchResult:  # noqa: D102
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

    def visit_combine_aggregated_outputs_node(  # noqa: D102
        self, node: CombineAggregatedOutputsNode
    ) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        # The parent node of the CombineAggregatedOutputsNode can be either ComputeMetricsNodes or CombineAggregatedOutputsNodes

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
            ), f"Traversing the parents of a CombineAggregatedOutputsNode should always produce a BaseOutput. Got: {result}"
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
                    (
                        branch_combination_result.left_branch
                        if branch_combination_result.combined_branch is None
                        else branch_combination_result.combined_branch
                    )
                    for branch_combination_result in combination_results
                ]

        logger.log(level=self._log_level, msg=f"Got {len(combined_parent_branches)} branches after combination")
        assert len(combined_parent_branches) > 0

        # If we were able to reduce the parent branches of the CombineAggregatedOutputsNode into a single one, there's no need
        # for a CombineAggregatedOutputsNode.
        if len(combined_parent_branches) == 1:
            return OptimizeBranchResult(base_output_node=combined_parent_branches[0])

        return OptimizeBranchResult(
            base_output_node=CombineAggregatedOutputsNode(parent_nodes=combined_parent_branches)
        )

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
        optimized_result: OptimizeBranchResult = dataflow_plan.sink_output_node.accept(self)

        logger.log(
            level=self._log_level,
            msg=f"Optimized:\n\n"
            f"{dataflow_plan.sink_output_node.structure_text()}\n\n"
            f"to:\n\n"
            f"{optimized_result.checked_sink_node.structure_text()}",
        )

        if optimized_result.sink_node:
            return DataflowPlan(
                plan_id=DagId.from_id_prefix(StaticIdPrefix.OPTIMIZED_DATAFLOW_PLAN_PREFIX),
                sink_output_nodes=[optimized_result.sink_node],
            )
        logger.log(level=self._log_level, msg="Optimizer didn't produce a result, so returning the same plan")
        return DataflowPlan(
            sink_output_nodes=[dataflow_plan.sink_output_node],
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

    def visit_min_max_node(self, node: MinMaxNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_base_output_handler(node)
