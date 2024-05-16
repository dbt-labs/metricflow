from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence

from metricflow_semantics.specs.spec_classes import MetricSpec
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.dfs_walker import DataflowDagWalker
from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.optimizer.source_scan.matching_linkable_specs import MatchingLinkableSpecsTransform

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ComputeMetricsBranchCombinerResult:  # noqa: D101
    # Perhaps adding more metadata about how nodes got combined would be useful.
    # If combined_branch is None, it means combination could not occur.
    combined_branch: Optional[DataflowPlanNode] = None

    @property
    def combined(self) -> bool:
        """Returns true if this result indicates that the branch could be combined."""
        return self.combined_branch is not None

    @property
    def checked_combined_branch(self) -> DataflowPlanNode:  # noqa: D102
        assert self.combined_branch is not None
        return self.combined_branch


class ComputeMetricsBranchCombiner(DataflowDagWalker[ComputeMetricsBranchCombinerResult]):
    """Combines branches where the leaf node is a ComputeMetricsNode.

    This considers two branches, a left branch and a right branch. The left branch is supplied via the argument in the
    initializer while the right branch is supplied via .accept(). This then attempts to create a similar branch that is
    the superposition of the two branches. For this to be possible, the two branches must be of the same structure,
    and the respective nodes in each branch must be the same type and have compatible parameters.

    For example, the superposition / combination of

    left_branch:
        <ComputeMetricsNode metrics=["bookings"]
            <AggregateMeasuresNode>
                <FilterElementsNode include_specs=["bookings"]>
                    <ReadSqlSourceNode semantic_model="bookings_source"/>
                </>
            </>
        </>
    right_branch:
        <ComputeMetricsNode metrics=["booking_value"]
            <AggregateMeasuresNode>
                <FilterElementsNode include_specs=["booking_value"]>
                    <ReadSqlSourceNode semantic_model="bookings_source"/>
                </>
            </>
        </>

    is

    <ComputeMetricsNode metrics=["bookings", "booking_value"]
        <AggregateMeasuresNode>
            <FilterElementsNode include_specs=["bookings", "booking_value"]>
                <ReadSqlSourceNode semantic_model="bookings_source"/>
            </>
        </>
    </>

    However,

    left_branch:
        <ComputeMetricsNode metrics=["bookings"]
            <AggregateMeasuresNode>
                <FilterElementsNode include_specs=["bookings", "is_instant"]>
                    <ReadSqlSourceNode semantic_model="bookings_source"/>
                </>
            </>
        </>
    right_branch:
        <ComputeMetricsNode metrics=["booking_value"]
            <AggregateMeasuresNode>
                <FilterElementsNode include_specs=["booking_value"]>
                    <ReadSqlSourceNode semantic_model="bookings_source"/>
                </>
            </>
        </>

    can't be superpositioned / combined because the different set of linkable specs in the FilterElementsNode will cause
    the AggregatedMeasuresNode to produce values for the measure that is different from the original branches. The logic
    to determine whether this is possible for each node type is encapsulated into the handler for each node type.

    In general, the questions to consider for combination are:

    * For nodes with no parents, can you combine the corresponding left and right nodes to produce an output
    that is has the same linkable specs, but a superset of the measures / metrics?
    * For other nodes, does a set of inputs that have the same linkable specs as the inputs to the left and right nodes
    but a superset of measures / metrics produce an output that superset of measures / metrics (with the same values)
    as the left and right nodes?

    The visitor traverses the dataflow plan via DFS, recursively combining the parent nodes first and combining the
    current node. During each step, current_left_node and current_right node always points to nodes that should be
    correlated between the two branches. If the structure is different or if combination can't happen due to node
    differences between the branches, a ComputeMetricsBranchCombinerResult (indicating that combination not possible)
    is propagated up to the result at the root node.
    """

    def __init__(self, left_branch_node: DataflowPlanNode) -> None:  # noqa: D107
        self._current_left_node: DataflowPlanNode = left_branch_node
        super().__init__(visit_log_level=logging.DEBUG, default_action_recursion=False)

    def _log_combine_failure(
        self,
        left_node: DataflowPlanNode,
        right_node: DataflowPlanNode,
        combine_failure_reason: str,
    ) -> None:
        logger.log(
            level=self._visit_log_level,
            msg=f"Because {combine_failure_reason}, unable to combine nodes "
            f"left_node={left_node} right_node={right_node}",
        )

    def _log_combine_success(
        self,
        left_node: DataflowPlanNode,
        right_node: DataflowPlanNode,
        combined_node: DataflowPlanNode,
    ) -> None:
        logger.log(
            level=self._visit_log_level,
            msg=f"Combined left_node={left_node} right_node={right_node} combined_node: {combined_node}",
        )

    def _combine_parent_branches(self, node: DataflowPlanNode) -> Optional[Sequence[DataflowPlanNode]]:
        if len(self._current_left_node.parent_nodes) != len(node.parent_nodes):
            self._log_combine_failure(
                left_node=self._current_left_node,
                right_node=node,
                combine_failure_reason="parent counts are unequal",
            )
            return None

        results_of_visiting_parent_nodes: List[ComputeMetricsBranchCombinerResult] = []

        for i, right_node_parent_node in enumerate(node.parent_nodes):
            left_position_before_recursion = self._current_left_node
            self._current_left_node = self._current_left_node.parent_nodes[i]
            results_of_visiting_parent_nodes.append(right_node_parent_node.accept(self))
            self._current_left_node = left_position_before_recursion

        combined_parents: List[DataflowPlanNode] = []
        for result in results_of_visiting_parent_nodes:
            if result.combined_branch is None:
                self._log_combine_failure(
                    left_node=self._current_left_node,
                    right_node=node,
                    combine_failure_reason="not all parents could be combined",
                )
                return None
            combined_parents.append(result.combined_branch)

        return combined_parents

    def _default_action(
        self, current_node: DataflowPlanNode, inputs: Sequence[ComputeMetricsBranchCombinerResult]
    ) -> ComputeMetricsBranchCombinerResult:
        combined_parent_nodes = self._combine_parent_branches(current_node)
        if combined_parent_nodes is None:
            return ComputeMetricsBranchCombinerResult()

        new_parent_nodes = combined_parent_nodes

        # If the parent nodes were combined, and the left node is the same as the right node, then the left and
        # right nodes can be combined.
        if self._current_left_node.functionally_identical(current_node):
            combined_node = current_node.with_new_parents(new_parent_nodes)
            self._log_combine_success(
                left_node=self._current_left_node, right_node=current_node, combined_node=combined_node
            )
            return ComputeMetricsBranchCombinerResult(combined_node)

        self._log_combine_failure(
            left_node=self._current_left_node,
            right_node=current_node,
            combine_failure_reason="there are functional differences",
        )
        return ComputeMetricsBranchCombinerResult()

    @override
    def default_visit_action(
        self, current_node: DataflowPlanNode, inputs: Sequence[ComputeMetricsBranchCombinerResult]
    ) -> ComputeMetricsBranchCombinerResult:
        self.log_visit_start(current_node, inputs)
        result = None
        try:
            result = self._default_action(current_node, inputs)
            return result
        finally:
            self.log_visit_end(current_node, result)

    def visit_aggregate_measures_node(  # noqa: D102
        self, node: AggregateMeasuresNode
    ) -> ComputeMetricsBranchCombinerResult:
        current_right_node = node

        combined_parent_nodes = self._combine_parent_branches(current_right_node)
        if combined_parent_nodes is None:
            return ComputeMetricsBranchCombinerResult()

        if not isinstance(self._current_left_node, current_right_node.__class__):
            self._log_combine_failure(
                left_node=self._current_left_node,
                right_node=current_right_node,
                combine_failure_reason=f"left node is not a {current_right_node.__class__.__name__}",
            )
            return ComputeMetricsBranchCombinerResult()

        assert len(combined_parent_nodes) == 1
        combined_parent_node = combined_parent_nodes[0]
        assert combined_parent_node is not None

        combined_metric_input_measure_specs = tuple(
            dict.fromkeys(
                self._current_left_node.metric_input_measure_specs + current_right_node.metric_input_measure_specs
            ).keys()
        )

        for spec in combined_metric_input_measure_specs:
            # Avoid combining branches if the AggregateMeasuresNode specifies a metric with an alias to avoid
            # collisions e.g. two metrics use the same alias for two different measures. This is not always the case,
            # so this could be improved later.
            if spec.alias is not None:
                self._log_combine_failure(
                    left_node=self._current_left_node,
                    right_node=current_right_node,
                    combine_failure_reason=f"Metric input measure spec {spec} has an alias",
                )
                return ComputeMetricsBranchCombinerResult()

        combined_node = AggregateMeasuresNode(
            parent_node=combined_parent_node,
            metric_input_measure_specs=combined_metric_input_measure_specs,
        )
        self._log_combine_success(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combined_node=combined_node,
        )
        return ComputeMetricsBranchCombinerResult(combined_node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
        current_right_node = node
        combined_parent_nodes = self._combine_parent_branches(current_right_node)
        if combined_parent_nodes is None:
            return ComputeMetricsBranchCombinerResult()

        if not isinstance(self._current_left_node, current_right_node.__class__):
            self._log_combine_failure(
                left_node=self._current_left_node,
                right_node=current_right_node,
                combine_failure_reason=f"left node is not a {current_right_node.__class__.__name__}",
            )
            return ComputeMetricsBranchCombinerResult()

        can_combine, combine_failure_reason = self._current_left_node.can_combine(current_right_node)
        if not can_combine:
            self._log_combine_failure(
                left_node=self._current_left_node,
                right_node=current_right_node,
                combine_failure_reason=combine_failure_reason,
            )
            return ComputeMetricsBranchCombinerResult()

        assert len(combined_parent_nodes) == 1
        combined_parent_node = combined_parent_nodes[0]
        assert combined_parent_node is not None

        # Dedupe (preserving order for output consistency) as it's possible for multiple derived metrics to use the same
        # metric.
        unique_metric_specs: List[MetricSpec] = []
        for metric_spec in tuple(self._current_left_node.metric_specs) + tuple(current_right_node.metric_specs):
            if metric_spec not in unique_metric_specs:
                unique_metric_specs.append(metric_spec)

        combined_node = ComputeMetricsNode(
            parent_node=combined_parent_node,
            metric_specs=unique_metric_specs,
            aggregated_to_elements=current_right_node.aggregated_to_elements,
            for_group_by_source_node=current_right_node.for_group_by_source_node,
        )
        self._log_combine_success(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combined_node=combined_node,
        )
        return ComputeMetricsBranchCombinerResult(combined_node)

    def visit_filter_elements_node(  # noqa: D102
        self, node: FilterElementsNode
    ) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
        current_right_node = node
        results_of_visiting_parent_nodes = self._combine_parent_branches(current_right_node)
        if results_of_visiting_parent_nodes is None:
            return ComputeMetricsBranchCombinerResult()

        if not isinstance(self._current_left_node, current_right_node.__class__):
            self._log_combine_failure(
                left_node=self._current_left_node,
                right_node=current_right_node,
                combine_failure_reason=f"left node is not a {current_right_node.__class__.__name__}",
            )
            return ComputeMetricsBranchCombinerResult()

        assert len(results_of_visiting_parent_nodes) == 1
        combined_parent_node = results_of_visiting_parent_nodes[0]
        assert combined_parent_node is not None

        # For the FilterElementsNode to be combined, the linkable specs have to be the same for the left and right.
        if not MatchingLinkableSpecsTransform(self._current_left_node.include_specs).transform(
            current_right_node.include_specs
        ):
            self._log_combine_failure(
                left_node=self._current_left_node,
                right_node=current_right_node,
                combine_failure_reason="linkable specs in the filter do not match",
            )
            return ComputeMetricsBranchCombinerResult()

        # De-dupe so that we don't see the same spec twice in include specs. For example, this can happen with dimension
        # specs since any branch that is merged together needs to output the same set of dimensions.
        combined_node = FilterElementsNode(
            parent_node=combined_parent_node,
            include_specs=self._current_left_node.include_specs.merge(current_right_node.include_specs).dedupe(),
        )
        self._log_combine_success(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combined_node=combined_node,
        )
        return ComputeMetricsBranchCombinerResult(combined_node)
