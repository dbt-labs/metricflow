from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generic, Optional, Sequence

from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import DataflowPlan, DataflowPlanNode, DataflowPlanNodeVisitor
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
from metricflow.dataflow.nodes.write_to_dataframe import WriteToResultDataframeNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode

logger = logging.getLogger(__name__)


class DataflowDagWalker(DataflowPlanNodeVisitor, Generic[VisitorOutputT], ABC):
    """A convenience class that simplifies writing algorithms that traverse the dataflow DAG."""

    def __init__(self, visit_log_level: int = logging.DEBUG, default_action_recursion: bool = True) -> None:
        """Initializer.

        When visiting a node, the `visit_log_level` is the logging level that should be used for those messages.

        Args:
            visit_log_level: Logging level that should be used for logging messages generated when visiting a node.
            default_action_recursion: If true, the default action will recursively traverse the parents first.
        """
        self._visit_log_level = visit_log_level
        self._default_action_recursion = default_action_recursion

    def walk_plan(self, dataflow_plan: DataflowPlan) -> VisitorOutputT:
        """Traverse (depth-first) the dataflow DAG starting from the sink node."""
        return self.walk_from_node(dataflow_plan.checked_sink_node)

    def walk_from_node(self, node: DataflowPlanNode) -> VisitorOutputT:
        """Traverse (depth-first) the dataflow DAG starting from the given node."""
        return node.accept(self)

    @abstractmethod
    def default_visit_action(self, current_node: DataflowPlanNode, inputs: Sequence[VisitorOutputT]) -> VisitorOutputT:
        """A default action that runs for each node if there isn't a specific visit-method implemented.

        If default_action_recursion is set, inputs will contain the results of visiting the parents first.
        """
        raise NotImplementedError

    @property
    def should_log(self) -> bool:
        """Returns true if the logging is enabled for the configured `visit_log_level`.

        This is useful as the arguments to log calls are evaluated regardless of whether the given logging is enabled.
        Formatting of complex objects in the log messages cause significant overhead, so one way to handle this is to
        follow a coding pattern like:

            if self.should_log:
                self.log(f"Input is: {mf_pformat(...)}")
        """
        return logger.isEnabledFor(self._visit_log_level)

    def log(self, msg: str) -> None:
        """Convenience method to log using the `visit_log_level`."""
        logger.log(level=self._visit_log_level, msg=msg)

    def log_visit_start(self, node: DataflowPlanNode, inputs: Sequence[VisitorOutputT]) -> None:  # noqa: D102
        if self.should_log:
            self.log(msg=f"Visiting {node} with inputs from parents:\n{indent(mf_pformat(inputs))}")

    def log_visit_end(self, node: DataflowPlanNode, node_output: Optional[VisitorOutputT]) -> None:  # noqa: D102
        if self.should_log:
            self.log(f"Visited {node} and produced:\n{indent(mf_pformat(node_output))}")

    def walk_parents(self, node: DataflowPlanNode) -> Sequence[VisitorOutputT]:  # noqa: D102
        return tuple(parent_node.accept(self) for parent_node in node.parent_nodes)

    @override
    def visit_source_node(self, node: ReadSqlSourceNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_filter_elements_node(self, node: FilterElementsNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_min_max_node(self, node: MinMaxNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())

    @override
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> VisitorOutputT:
        return self.default_visit_action(node, self.walk_parents(node) if self._default_action_recursion else ())
