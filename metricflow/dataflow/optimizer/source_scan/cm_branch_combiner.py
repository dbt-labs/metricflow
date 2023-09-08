from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence

from metricflow.dataflow.dataflow_plan import (
    AggregateMeasuresNode,
    BaseOutput,
    CombineMetricsNode,
    ComputeMetricsNode,
    ConstrainTimeRangeNode,
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
    WhereConstraintNode,
    WriteToResultDataframeNode,
    WriteToResultTableNode,
)
from metricflow.dataflow.optimizer.source_scan.matching_linkable_specs import MatchingLinkableSpecsTransform
from metricflow.specs.specs import InstanceSpecSet

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ComputeMetricsBranchCombinerResult:  # noqa: D
    # Perhaps adding more metadata about how nodes got combined would be useful.
    # If combined_branch is None, it means combination could not occur.
    combined_branch: Optional[BaseOutput] = None

    @property
    def combined(self) -> bool:
        """Returns true if this result indicates that the branch could be combined."""
        return self.combined_branch is not None

    @property
    def checked_combined_branch(self) -> BaseOutput:  # noqa: D
        assert self.combined_branch is not None
        return self.combined_branch


class ComputeMetricsBranchCombiner(DataflowPlanNodeVisitor[ComputeMetricsBranchCombinerResult]):
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

    def __init__(self, left_branch_node: BaseOutput) -> None:  # noqa: D
        self._current_left_node: DataflowPlanNode = left_branch_node
        self._log_level = logging.DEBUG

    def _log_visit_node_type(self, node: DataflowPlanNode) -> None:
        logger.log(level=self._log_level, msg=f"Visiting {node}")

    def _log_combine_failure(
        self,
        left_node: DataflowPlanNode,
        right_node: DataflowPlanNode,
        combine_failure_reason: str,
    ) -> None:
        logger.log(
            level=self._log_level,
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
            level=self._log_level,
            msg=f"Combined left_node={left_node} right_node={right_node} combined_node: {combined_node}",
        )

    def _combine_parent_branches(self, current_right_node: BaseOutput) -> Optional[Sequence[BaseOutput]]:
        if len(self._current_left_node.parent_nodes) != len(current_right_node.parent_nodes):
            self._log_combine_failure(
                left_node=self._current_left_node,
                right_node=current_right_node,
                combine_failure_reason="parent counts are unequal",
            )
            return None

        results_of_visiting_parent_nodes: List[ComputeMetricsBranchCombinerResult] = []

        for i, right_node_parent_node in enumerate(current_right_node.parent_nodes):
            left_position_before_recursion = self._current_left_node
            self._current_left_node = self._current_left_node.parent_nodes[i]
            results_of_visiting_parent_nodes.append(right_node_parent_node.accept(self))
            self._current_left_node = left_position_before_recursion

        combined_parents: List[BaseOutput] = []
        for result in results_of_visiting_parent_nodes:
            if result.combined_branch is None:
                self._log_combine_failure(
                    left_node=self._current_left_node,
                    right_node=current_right_node,
                    combine_failure_reason="not all parents could be combined",
                )
                return None
            combined_parents.append(result.combined_branch)

        return combined_parents

    def _default_handler(self, current_right_node: BaseOutput) -> ComputeMetricsBranchCombinerResult:  # noqa: D
        combined_parent_nodes = self._combine_parent_branches(current_right_node)
        if combined_parent_nodes is None:
            return ComputeMetricsBranchCombinerResult()

        new_parent_nodes = combined_parent_nodes

        # If the parent nodes were combined, and the left node is the same as the right node, then the left and right
        # nodes can be combined.
        if self._current_left_node.functionally_identical(current_right_node):
            combined_node = current_right_node.with_new_parents(new_parent_nodes)
            self._log_combine_success(
                left_node=self._current_left_node, right_node=current_right_node, combined_node=combined_node
            )
            return ComputeMetricsBranchCombinerResult(combined_node)

        self._log_combine_failure(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combine_failure_reason="there are functional differences",
        )
        return ComputeMetricsBranchCombinerResult()

    def visit_source_node(self, node: ReadSqlSourceNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_to_base_output_node(  # noqa: D
        self, node: JoinToBaseOutputNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_aggregated_measures_by_groupby_columns_node(  # noqa: D
        self, node: JoinAggregatedMeasuresByGroupByColumnsNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_aggregate_measures_node(  # noqa: D
        self, node: AggregateMeasuresNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
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

        combined_metric_input_measure_specs = (
            self._current_left_node.metric_input_measure_specs + current_right_node.metric_input_measure_specs
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

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D
        current_right_node = node
        self._log_visit_node_type(current_right_node)
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
        combined_node = ComputeMetricsNode(
            parent_node=combined_parent_node,
            metric_specs=self._current_left_node.metric_specs + current_right_node.metric_specs,
        )
        self._log_combine_success(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combined_node=combined_node,
        )
        return ComputeMetricsBranchCombinerResult(combined_node)

    def _handle_unsupported_node(self, current_right_node: DataflowPlanNode) -> ComputeMetricsBranchCombinerResult:
        self._log_combine_failure(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combine_failure_reason=(
                f"right node is of type {current_right_node.__class__.__name__} which is not yet handled"
            ),
        )
        return ComputeMetricsBranchCombinerResult()

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._handle_unsupported_node(node)

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_write_to_result_dataframe_node(  # noqa: D
        self, node: WriteToResultDataframeNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._handle_unsupported_node(node)

    def visit_write_to_result_table_node(  # noqa: D
        self, node: WriteToResultTableNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._handle_unsupported_node(node)

    def visit_pass_elements_filter_node(  # noqa: D
        self, node: FilterElementsNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)

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
            include_specs=InstanceSpecSet.merge(
                (self._current_left_node.include_specs, current_right_node.include_specs)
            ).dedupe(),
        )
        self._log_combine_success(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combined_node=combined_node,
        )
        return ComputeMetricsBranchCombinerResult(combined_node)

    def visit_combine_metrics_node(self, node: CombineMetricsNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._handle_unsupported_node(node)

    def visit_constrain_time_range_node(  # noqa: D
        self, node: ConstrainTimeRangeNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_over_time_range_node(  # noqa: D
        self, node: JoinOverTimeRangeNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_semi_additive_join_node(  # noqa: D
        self, node: SemiAdditiveJoinNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_metric_time_dimension_transform_node(  # noqa: D
        self, node: MetricTimeDimensionTransformNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D
        self._log_visit_node_type(node)
        return self._default_handler(node)
