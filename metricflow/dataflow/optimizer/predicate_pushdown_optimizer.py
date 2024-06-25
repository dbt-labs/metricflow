from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.references import SemanticModelReference
from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.specs.spec_classes import WhereFilterSpec
from metricflow_semantics.sql.sql_join_type import SqlJoinType

from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_conversion_events import JoinConversionEventsNode
from metricflow.dataflow.nodes.join_over_time import JoinOverTimeRangeNode
from metricflow.dataflow.nodes.join_to_base import JoinOnEntitiesNode
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
from metricflow.dataflow.optimizer.dataflow_plan_optimizer import DataflowPlanOptimizer
from metricflow.dataflow.optimizer.source_scan.source_scan_optimizer import OptimizeBranchResult
from metricflow.plan_conversion.node_processor import PredicateInputType, PredicatePushdownState

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PredicatePushdownBranchState:
    """State tracking class for managing predicate pushdown along a given branch.

    This class is meant to show the state as of a given moment in time, matched up with the history of
    all nodes visited.

    TODO: streamline into a single sequence.
    """

    branch_pushdown_state: Tuple[PredicatePushdownState, ...]
    node_path: Tuple[DataflowPlanNode, ...]


class PredicatePushdownBranchStateTracker:
    """Tracking class for monitoring pushdown state at the node level during a visitor walk."""

    def __init__(self, initial_state: PredicatePushdownState) -> None:  # noqa: D107
        self._initial_state = initial_state
        self._current_branch_state: List[PredicatePushdownState] = []
        self._current_node_path: List[DataflowPlanNode] = []

    @contextmanager
    def track_pushdown_state(
        self, node: DataflowPlanNode, pushdown_state: PredicatePushdownState
    ) -> Iterator[PredicatePushdownBranchState]:
        """Context manager used to track pushdown state along branches in a Dataflow Plan.

        This updates the branch state and node path on entry, and then pops the last entry off on exit in order to
        allow tracking of pushdown state at the level of each node without repeating state or leaking to sibling
        branches.

        TODO: combine this with the DagTraversalPathTracker
        """
        self._current_branch_state.append(pushdown_state)
        self._current_node_path.append(node)
        yield PredicatePushdownBranchState(
            branch_pushdown_state=tuple(self._current_branch_state), node_path=tuple(self._current_node_path)
        )
        self._current_branch_state.pop(-1)
        self._current_node_path.pop(-1)

    @property
    def last_pushdown_state(self) -> PredicatePushdownState:
        """Returns the last seen PredicatePushdownState."""
        if len(self._current_branch_state) > 0:
            return self._current_branch_state[-1]
        return self._initial_state


class PredicatePushdownOptimizer(
    DataflowPlanNodeVisitor[OptimizeBranchResult],
    DataflowPlanOptimizer,
):
    """Pushes filter predicates as close to the source node as possible.

    This evaluates filter predicates to determine which, if any, can be directly to an input source node.
    It operates by walking each branch in the DataflowPlan and collecting pushdown state information, then
    evaluating that state at the input source node and applying the filter node (e.g., a WhereConstraintNode)
    directly to the source. As the optimizer unrolls back through the branch it will remove the duplicated
    constraint node if it is appropriate to do so.
    """

    def __init__(self) -> None:
        """Initializer.

        Initializes predicate pushdown state with all optimizer-managed pushdown types enabled, but nothing to
        push down, since time range constraints and where filter specs will be discovered during traversal.
        """
        self._log_level = logging.DEBUG
        self._predicate_pushdown_tracker = PredicatePushdownBranchStateTracker(
            initial_state=PredicatePushdownState(
                time_range_constraint=None,
                where_filter_specs=tuple(),
                pushdown_enabled_types=frozenset([PredicateInputType.CATEGORICAL_DIMENSION]),
            )
        )

    def optimize(self, dataflow_plan: DataflowPlan) -> DataflowPlan:  # noqa: D102
        optimized_result: OptimizeBranchResult = dataflow_plan.sink_node.accept(self)

        logger.log(
            level=self._log_level,
            msg=f"Optimized:\n\n"
            f"{dataflow_plan.sink_node.structure_text()}\n\n"
            f"to:\n\n"
            f"{optimized_result.optimized_branch.structure_text()}",
        )

        return DataflowPlan(
            plan_id=DagId.from_id_prefix(StaticIdPrefix.OPTIMIZED_DATAFLOW_PLAN_PREFIX),
            sink_nodes=[optimized_result.optimized_branch],
        )

    def _log_visit_node_type(self, node: DataflowPlanNode) -> None:
        logger.log(
            level=self._log_level,
            msg=f"Visiting {node} with initial pushdown state {self._predicate_pushdown_tracker.last_pushdown_state}",
        )

    def _default_handler(
        self, node: DataflowPlanNode, pushdown_state: Optional[PredicatePushdownState] = None
    ) -> OptimizeBranchResult:
        """Encapsulates state updates and handling for most node types.

        The dominant majority of nodes simply propagate the current predicate pushdown state along and return
        whatever output the parent nodes produce. Of the nodes that do not do this, the most common deviation
        is a pushdown state update.

        As such, this method defaults to propagating the last seen state, but allows an override for cases where
        the handling of the node itself is standard but a pushdown state update is required.
        """
        if pushdown_state is None:
            pushdown_state = self._predicate_pushdown_tracker.last_pushdown_state

        with self._predicate_pushdown_tracker.track_pushdown_state(node, pushdown_state):
            optimized_parents: Sequence[OptimizeBranchResult] = tuple(
                parent_node.accept(self) for parent_node in node.parent_nodes
            )
            return OptimizeBranchResult(
                optimized_branch=node.with_new_parents(tuple(x.optimized_branch for x in optimized_parents))
            )

    def _models_for_spec(self, spec: WhereFilterSpec) -> Sequence[SemanticModelReference]:
        """Return the distinct semantic models that source the elements referenced in the given where spec.

        TODO: make this a property of the spec
        """
        return tuple(set(element.semantic_model_origin for element in spec.linkable_elements))

    # Source nodes - potential pushdown targets.

    def visit_metric_time_dimension_transform_node(  # noqa: D102
        self, node: MetricTimeDimensionTransformNode
    ) -> OptimizeBranchResult:
        # TODO: Update docstring and logic to apply filter where needed. For now we simply add a logging
        # hook with a superset of eligible filters we may consider applying here
        self._log_visit_node_type(node)
        current_pushdown_state = self._predicate_pushdown_tracker.last_pushdown_state
        node_semantic_models = node.as_plan().source_semantic_models
        if len(node_semantic_models) != 1 or not current_pushdown_state.has_where_filters_to_push_down:
            return self._default_handler(node)

        source_semantic_model, *_ = node_semantic_models
        filters_to_apply: List[WhereFilterSpec] = []
        filters_left_over: List[WhereFilterSpec] = []
        for spec in current_pushdown_state.where_filter_specs:
            spec_semantic_models = self._models_for_spec(spec)
            if len(spec_semantic_models) == 1 and spec_semantic_models[0] == source_semantic_model:
                # TODO: check columns against the spec elements
                filters_to_apply.append(spec)
            else:
                filters_left_over.append(spec)
        logger.log(level=self._log_level, msg=f"Filter specs to add:\n{filters_to_apply}")
        # TODO: wrap node with a WhereConstraintNode and propagate filters applied back up the branch for removal
        updated_pushdown_state = PredicatePushdownState(
            time_range_constraint=current_pushdown_state.time_range_constraint,
            where_filter_specs=tuple(filters_left_over),
            pushdown_enabled_types=current_pushdown_state.pushdown_enabled_types,
        )
        return self._default_handler(node=node, pushdown_state=updated_pushdown_state)

    def visit_source_node(self, node: ReadSqlSourceNode) -> OptimizeBranchResult:  # noqa: D102
        # TODO: Update docstring and logic to apply filters where needed.
        # The commented out logic is a placeholder for what we'll use, ignore this for now. We only need to push
        # down to this node type if we are doing a dimension-only query on a non-metric dataset, at least for now
        #
        # self._log_visit_node_type(node)
        # current_pushdown_state = self._predicate_pushdown_tracker.last_pushdown_state
        # node_semantic_models = node.as_plan().source_semantic_models
        # if len(node_semantic_models) != 1 or current_pushdown_state.has_where_filters_to_push_down:
        #     return self._default_handler(node)

        # source_semantic_model, *_ = node_semantic_models
        # filters_to_apply: List[WhereFilterSpec] = []
        # for spec in current_pushdown_state.where_filter_specs:
        #     spec_semantic_models = self._models_for_spec(spec)
        #     if len(spec_semantic_models) == 1 and spec_semantic_models[0] == source_semantic_model:
        #         # TODO: check columns against the spec elements
        #         filters_to_apply.append(spec)
        # logger.log(level=self._log_level, msg=f"Filter specs to add:\n{filters_to_apply}")
        # # TODO: wrap node with a WhereConstraintNode and propagate filters applied back up the branch for removal
        # return self._default_handler(node)

        self._log_visit_node_type(node)
        return self._default_handler(node)

    # Constraint nodes - predicate sources for pushdown.

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> OptimizeBranchResult:
        """Adds time range constraint information from the input node to the current pushdown state.

        For now we simply overwrite the window with the time range constraint value we find here. In future
        we may wish to allow for a set of time constraints that we can union over at filter time.
        """
        self._log_visit_node_type(node)
        updated_pushdown_state = PredicatePushdownState.with_time_range_constraint(
            original_pushdown_state=self._predicate_pushdown_tracker.last_pushdown_state,
            time_range_constraint=node.time_range_constraint,
        )
        return self._default_handler(node=node, pushdown_state=updated_pushdown_state)

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> OptimizeBranchResult:
        """Adds where filters from the input node to the current pushdown state.

        The WhereConstraintNode carries the filter information in the form of WhereFilterSpecs. For any
        filter specs that may be eligible for predicate pushdown this node will add them to the pushdown state.
        The fact that they have been added at this point does not mean they will be pushed down, as intervening
        join nodes might remove them from consideration, so we retain them here as well in order to ensure all
        filters are applied as specified.
        """
        self._log_visit_node_type(node)
        current_pushdown_state = self._predicate_pushdown_tracker.last_pushdown_state
        # TODO: update WhereConstraintNode to hold a list of specs instead of merging them all before initialization
        where_specs = (node.where,)
        pushdown_eligible_specs: List[WhereFilterSpec] = []
        for spec in where_specs:
            semantic_models = self._models_for_spec(spec)
            invalid_element_types = [
                element
                for element in spec.linkable_elements
                if element.element_type not in current_pushdown_state.pushdown_eligible_element_types
            ]
            if len(semantic_models) != 1 and len(invalid_element_types) > 0:
                continue
            pushdown_eligible_specs.append(spec)

        updated_pushdown_state = PredicatePushdownState.with_additional_where_filter_specs(
            original_pushdown_state=current_pushdown_state, additional_where_filter_specs=tuple(pushdown_eligible_specs)
        )

        return self._default_handler(node=node, pushdown_state=updated_pushdown_state)

    # Join nodes - these potentially affect pushdown state

    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> OptimizeBranchResult:
        """Removes where filter specs from current pushdown state while allowing subsequent specs to be pushed down.

        The combine aggregated outputs node does a FULL OUTER JOIN, which means any where constraint applied after it
        cannot be safely pushed down if that constraint might include NULLs. However, where constraints applied
        to parents of this node can still be pushed down along the branch, and time range constraints will never allow
        NULLs to pass so those should remain intact.
        """
        self._log_visit_node_type(node)
        # TODO: move this "remove where filters" logic into PredicatePushdownState
        updated_pushdown_state = PredicatePushdownState(
            time_range_constraint=self._predicate_pushdown_tracker.last_pushdown_state.time_range_constraint,
            where_filter_specs=tuple(),
            pushdown_enabled_types=self._predicate_pushdown_tracker.last_pushdown_state.pushdown_enabled_types,
        )
        return self._default_handler(node=node, pushdown_state=updated_pushdown_state)

    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> OptimizeBranchResult:
        """Updates predicate pushdown state in a manner appropriate for the JoinConversionEventsNode.

        As of right now the JoinConversionEvents node does some wonky stuff with filter expressions. More broadly, it
        is not entirely clear if it's ok to push an arbitrary filter expression down past this point in the graph,
        because an outside filter might be meant to apply to the conversion output rather than the conversion input,
        and managing the conversion window and time filters gets tricky.

        TODO: Enable predicate pushdown once we establish clear expectations for conversion metric filter behaviors.

        """
        self._log_visit_node_type(node)
        base_node_pushdown_state = PredicatePushdownState(
            time_range_constraint=self._predicate_pushdown_tracker.last_pushdown_state.time_range_constraint,
            where_filter_specs=tuple(),
            pushdown_enabled_types=self._predicate_pushdown_tracker.last_pushdown_state.pushdown_enabled_types,
        )
        # The conversion metric branch silently removes all filters, so this is a redundant operation.
        # TODO: Enable pushdown for the conversion metric branch when filters are supported
        conversion_node_pushdown_state = PredicatePushdownState.with_pushdown_disabled()

        optimized_parents: List[OptimizeBranchResult] = []
        with self._predicate_pushdown_tracker.track_pushdown_state(node, base_node_pushdown_state):
            optimized_parents.append(node.base_node.accept(self))

        with self._predicate_pushdown_tracker.track_pushdown_state(node, conversion_node_pushdown_state):
            optimized_parents.append(node.conversion_node.accept(self))

        return OptimizeBranchResult(
            optimized_branch=node.with_new_parents(
                new_parent_nodes=tuple(x.optimized_branch for x in optimized_parents)
            )
        )

    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> OptimizeBranchResult:
        """Handles pushdown state propagation for the standard join node type.

        This node type has two sets of parent nodes - a left node and a set of join targets - and the pushdown state
        must be updated separately for each parent based on the relevant join type. What's more, each parent represents
        a branch in the DAG, and as such the state propagation must happen independently for each.

        In particular, if a given branch is ever a target of any OUTER JOIN we cannot safely push down any filter that
        might allow for a NULL value, as that affects query semantics. In other words, the left branch cannot allow
        predicate pushdown for these filter types if there is even a single FULL OUTER or RIGHT OUTER JOIN in the
        target list.

        Note - at this time we only apply time constraints to measure nodes, and those are always on the left side
        of the join. As such, time constraints are not propagated to the right side of the join. This restriction
        may be relaxed at a later time, but for now it is largely irrelevant since we do not allow fanout joins and
        do not yet have support for pre-filters based on time ranges for things like SCD joins.
        """
        self._log_visit_node_type(node)
        left_parent = node.left_node
        if any(join_description.join_type is SqlJoinType.FULL_OUTER for join_description in node.join_targets):
            left_branch_pushdown_state = PredicatePushdownState(
                time_range_constraint=self._predicate_pushdown_tracker.last_pushdown_state.time_range_constraint,
                where_filter_specs=tuple(),
                pushdown_enabled_types=self._predicate_pushdown_tracker.last_pushdown_state.pushdown_enabled_types,
            )
        else:
            left_branch_pushdown_state = self._predicate_pushdown_tracker.last_pushdown_state

        optimized_parents: List[OptimizeBranchResult] = []
        with self._predicate_pushdown_tracker.track_pushdown_state(node, left_branch_pushdown_state):
            optimized_parents.append(left_parent.accept(self))

        base_right_branch_pushdown_state = PredicatePushdownState.without_time_range_constraint(
            self._predicate_pushdown_tracker.last_pushdown_state
        )
        outer_join_right_branch_pushdown_state = PredicatePushdownState(
            time_range_constraint=None,
            where_filter_specs=tuple(),
            pushdown_enabled_types=base_right_branch_pushdown_state.pushdown_enabled_types,
        )
        for join_description in node.join_targets:
            if (
                join_description.join_type is SqlJoinType.LEFT_OUTER
                or join_description.join_type is SqlJoinType.FULL_OUTER
            ):
                right_branch_pushdown_state = outer_join_right_branch_pushdown_state
            else:
                right_branch_pushdown_state = base_right_branch_pushdown_state
            with self._predicate_pushdown_tracker.track_pushdown_state(node, right_branch_pushdown_state):
                optimized_parents.append(join_description.join_node.accept(self))

        return OptimizeBranchResult(
            optimized_branch=node.with_new_parents(tuple(x.optimized_branch for x in optimized_parents))
        )

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> OptimizeBranchResult:
        """Updates time range constraint window to account for join over time range behavior, as needed.

        For the time being we simply pass through in all cases, because time constraint adjustments are
        handled in the DataflowPlanBuilder and the original time constraint is passed through.

        TODO: move constraint window adjustment to the optimizer for application of the relevant TimeRangeConstraint.
        """
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> OptimizeBranchResult:
        """Updates time range constraint window to account for time spine join intervals, as needed.

        For the time being we simply pass through in all cases, because time constraint handling is done between the
        DataflowPlanBuilder and DataflowToSqlQueryPlanConverter.

        TODO: move constraint window management to the optimizer
        """
        self._log_visit_node_type(node)
        return self._default_handler(node)

    # Other nodes - these simply propagate state, as they do not affect predicate pushdown in our context

    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_filter_elements_node(self, node: FilterElementsNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_write_to_result_data_table_node(  # noqa: D102
        self, node: WriteToResultDataTableNode
    ) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_add_generated_uuid_column_node(  # noqa: D102
        self, node: AddGeneratedUuidColumnNode
    ) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_min_max_node(self, node: MinMaxNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)

    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> OptimizeBranchResult:  # noqa: D102
        self._log_visit_node_type(node)
        return self._default_handler(node)