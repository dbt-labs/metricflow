from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence

from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.dataflow.dataflow_plan import (
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


class ComputeMetricsBranchCombiner(DataflowPlanNodeVisitor[ComputeMetricsBranchCombinerResult]):
    """Combines branches where the leaf node is a ComputeMetricsNode.

    This considers two branches, a left branch and a right branch. The left branch is supplied via the argument in the
    initializer while the right branch is supplied via .accept(). This then attempts to create a similar branch that is
    the superposition of the two branches. For this to be possible, the two branches must be of the same structure,
    and the respective nodes in each branch must be the same type and have compatible parameters.

    For example, the superposition / combination of

    left_branch:
        <ComputeMetricsNode metrics=["bookings"]
            <AggregateSimpleMetricInputsNode>
                <FilterElementsNode include_specs=["bookings"]>
                    <ReadSqlSourceNode semantic_model="bookings_source"/>
                </>
            </>
        </>
    right_branch:
        <ComputeMetricsNode metrics=["booking_value"]
            <AggregateSimpleMetricInputsNode>
                <FilterElementsNode include_specs=["booking_value"]>
                    <ReadSqlSourceNode semantic_model="bookings_source"/>
                </>
            </>
        </>

    is

    <ComputeMetricsNode metrics=["bookings", "booking_value"]
        <AggregateSimpleMetricInputsNode>
            <FilterElementsNode include_specs=["bookings", "booking_value"]>
                <ReadSqlSourceNode semantic_model="bookings_source"/>
            </>
        </>
    </>

    However,

    left_branch:
        <ComputeMetricsNode metrics=["bookings"]
            <AggregateSimpleMetricInputsNode>
                <FilterElementsNode include_specs=["bookings", "is_instant"]>
                    <ReadSqlSourceNode semantic_model="bookings_source"/>
                </>
            </>
        </>
    right_branch:
        <ComputeMetricsNode metrics=["booking_value"]
            <AggregateSimpleMetricInputsNode>
                <FilterElementsNode include_specs=["booking_value"]>
                    <ReadSqlSourceNode semantic_model="bookings_source"/>
                </>
            </>
        </>

    can't be superpositioned / combined because the different set of linkable specs in the FilterElementsNode will cause
    the AggregatedMeasuresNode to produce values for the simple-metric input that is different from the original branches. The logic
    to determine whether this is possible for each node type is encapsulated into the handler for each node type.

    In general, the questions to consider for combination are:

    * For nodes with no parents, can you combine the corresponding left and right nodes to produce an output
    that is has the same linkable specs, but a superset of the simple-metric inputs / metrics?
    * For other nodes, does a set of inputs that have the same linkable specs as the inputs to the left and right nodes
    but a superset of simple-metric inputs / metrics produce an output that superset of simple-metric inputs / metrics (with the same values)
    as the left and right nodes?

    The visitor traverses the dataflow plan via DFS, recursively combining the parent nodes first and combining the
    current node. During each step, current_left_node and current_right node always points to nodes that should be
    correlated between the two branches. If the structure is different or if combination can't happen due to node
    differences between the branches, a ComputeMetricsBranchCombinerResult (indicating that combination not possible)
    is propagated up to the result at the root node.
    """

    def __init__(self, left_branch_node: DataflowPlanNode) -> None:  # noqa: D107
        self._current_left_node: DataflowPlanNode = left_branch_node

    def _log_visit_node_type(self, node: DataflowPlanNode) -> None:
        logger.debug(LazyFormat(lambda: f"Visiting {node.node_id}"))

    def _log_combine_failure(
        self,
        left_node: DataflowPlanNode,
        right_node: DataflowPlanNode,
        combine_failure_reason: str,
    ) -> None:
        logger.debug(
            LazyFormat(
                "Unable to combine nodes",
                combine_failure_reason=combine_failure_reason,
                left_node=left_node.node_id,
                right_node=right_node.node_id,
            )
        )

    def _log_combine_success(
        self,
        left_node: DataflowPlanNode,
        right_node: DataflowPlanNode,
        combined_node: DataflowPlanNode,
    ) -> None:
        logger.debug(
            LazyFormat(
                "Successfully combined nodes",
                left_node=left_node.node_id,
                right_node=right_node.node_id,
                combined_node=combined_node.node_id,
            )
        )

    def _combine_parent_branches(self, current_right_node: DataflowPlanNode) -> Optional[Sequence[DataflowPlanNode]]:
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

        combined_parents: List[DataflowPlanNode] = []
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

    def _default_handler(self, current_right_node: DataflowPlanNode) -> ComputeMetricsBranchCombinerResult:
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

    def visit_source_node(self, node: ReadSqlSourceNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_on_entities_node(  # noqa: D102
        self, node: JoinOnEntitiesNode
    ) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_aggregate_simple_metric_inputs_node(  # noqa: D102
        self, node: AggregateSimpleMetricInputsNode
    ) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
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

        if self._current_left_node.null_fill_value_mapping != current_right_node.null_fill_value_mapping:
            self._log_combine_failure(
                left_node=self._current_left_node,
                right_node=current_right_node,
                combine_failure_reason=f"Conflicting null-fill-value mapping - left: {self._current_left_node.null_fill_value_mapping} right: {current_right_node.null_fill_value_mapping}",
            )
            return ComputeMetricsBranchCombinerResult()

        combined_node = AggregateSimpleMetricInputsNode.create(
            parent_node=combined_parent_node,
            null_fill_value_mapping=self._current_left_node.null_fill_value_mapping.merge(
                current_right_node.null_fill_value_mapping
            ),
        )
        self._log_combine_success(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combined_node=combined_node,
        )
        return ComputeMetricsBranchCombinerResult(combined_node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
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

        combined_node = ComputeMetricsNode.create(
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

    def _handle_unsupported_node(self, current_right_node: DataflowPlanNode) -> ComputeMetricsBranchCombinerResult:
        self._log_combine_failure(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combine_failure_reason=(
                f"right node is of type {current_right_node.__class__.__name__} which is not yet handled"
            ),
        )
        return ComputeMetricsBranchCombinerResult()

    def visit_window_reaggregation_node(  # noqa: D102
        self, node: WindowReaggregationNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._handle_unsupported_node(node)

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._handle_unsupported_node(node)

    def visit_where_constraint_node(  # noqa: D102
        self, node: WhereConstraintNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_write_to_result_data_table_node(  # noqa: D102
        self, node: WriteToResultDataTableNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._handle_unsupported_node(node)

    def visit_write_to_result_table_node(  # noqa: D102
        self, node: WriteToResultTableNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._handle_unsupported_node(node)

    def visit_filter_elements_node(self, node: FilterElementsNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
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
        combined_node = FilterElementsNode.create(
            parent_node=combined_parent_node,
            include_specs=self._current_left_node.include_specs.merge(current_right_node.include_specs).dedupe(),
        )
        self._log_combine_success(
            left_node=self._current_left_node,
            right_node=current_right_node,
            combined_node=combined_node,
        )
        return ComputeMetricsBranchCombinerResult(combined_node)

    def visit_combine_aggregated_outputs_node(  # noqa: D102
        self, node: CombineAggregatedOutputsNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._handle_unsupported_node(node)

    def visit_constrain_time_range_node(  # noqa: D102
        self, node: ConstrainTimeRangeNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_over_time_range_node(  # noqa: D102
        self, node: JoinOverTimeRangeNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_semi_additive_join_node(  # noqa: D102
        self, node: SemiAdditiveJoinNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_metric_time_dimension_transform_node(  # noqa: D102
        self, node: MetricTimeDimensionTransformNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_to_time_spine_node(  # noqa: D102
        self, node: JoinToTimeSpineNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_add_generated_uuid_column_node(  # noqa: D102
        self, node: AddGeneratedUuidColumnNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_conversion_events_node(  # noqa: D102
        self, node: JoinConversionEventsNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_to_custom_granularity_node(  # noqa: D102
        self, node: JoinToCustomGranularityNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_min_max_node(self, node: MinMaxNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_alias_specs_node(self, node: AliasSpecsNode) -> ComputeMetricsBranchCombinerResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_offset_base_grain_by_custom_grain_node(  # noqa: D102
        self, node: OffsetBaseGrainByCustomGrainNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_offset_custom_granularity_node(  # noqa: D102
        self, node: OffsetCustomGranularityNode
    ) -> ComputeMetricsBranchCombinerResult:
        self._log_visit_node_type(node)
        return self._default_handler(node)
