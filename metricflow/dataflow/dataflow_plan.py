"""Nodes for building a dataflow plan."""

from __future__ import annotations

import logging
import typing
from abc import ABC, abstractmethod
from typing import Generic, Optional, Sequence, Type, TypeVar

from metricflow.dag.id_prefix import StaticIdPrefix
from metricflow.dag.mf_dag import DagId, DagNode, MetricFlowDag, NodeId
from metricflow.visitor import Visitable, VisitorOutputT

if typing.TYPE_CHECKING:
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


logger = logging.getLogger(__name__)

NodeSelfT = TypeVar("NodeSelfT", bound="DataflowPlanNode")


class DataflowPlanNode(DagNode, Visitable, ABC):
    """A node in the graph representation of the dataflow.

    Each node in the graph performs an operation from the data that comes from the parent nodes, and the result is
    passed to the child nodes. The flow of data starts from source nodes, and ends at sink nodes.
    """

    def __init__(self, node_id: NodeId, parent_nodes: Sequence[DataflowPlanNode]) -> None:
        """Constructor.

        Args:
            node_id: the ID for the node
            parent_nodes: data comes from the parent nodes.
        """
        self._parent_nodes = tuple(parent_nodes)
        super().__init__(node_id=node_id)

    @property
    def parent_nodes(self) -> Sequence[DataflowPlanNode]:
        """Return the nodes where data for this node comes from."""
        return self._parent_nodes

    @abstractmethod
    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        raise NotImplementedError

    @abstractmethod
    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:
        """Returns true if this node the same functionality as the other node.

        In other words, this returns true if  all parameters (aside from parent_nodes) are the same.
        """
        raise NotImplementedError

    @abstractmethod
    def with_new_parents(self: NodeSelfT, new_parent_nodes: Sequence[BaseOutput]) -> NodeSelfT:
        """Creates a node with the same behavior as this node, but with a different set of parents.

        typing.Self would be useful here, but not available in Python 3.8.
        """
        raise NotImplementedError

    @property
    def node_type(self) -> Type:  # noqa: D102
        # TODO: Remove.
        return self.__class__


class DataflowPlanNodeVisitor(Generic[VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of a dataflow plan.

    Follows the visitor pattern: https://en.wikipedia.org/wiki/Visitor_pattern
    All visit* methods are similar and one exists for every type of node in the dataflow plan. The appropriate method
    will be called with DataflowPlanNode.accept().
    """

    @abstractmethod
    def visit_source_node(self, node: ReadSqlSourceNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_pass_elements_filter_node(self, node: FilterElementsNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_metric_time_dimension_transform_node(  # noqa: D102
        self, node: MetricTimeDimensionTransformNode
    ) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_min_max_node(self, node: MinMaxNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> VisitorOutputT:  # noqa: D102
        pass


class BaseOutput(DataflowPlanNode, ABC):
    """A node that outputs data in a "base" format.

    The base format is where the columns represent un-aggregated measures, dimensions, and entities.
    """

    pass


class AggregatedMeasuresOutput(BaseOutput, ABC):
    """A node that outputs data where the measures are aggregated.

    The measures are aggregated with respect to the present entities and dimensions.
    """

    pass


class ComputedMetricsOutput(BaseOutput, ABC):
    """A node that outputs data that contains metrics computed from measures."""

    pass


class SinkNodeVisitor(Generic[VisitorOutputT], ABC):
    """Similar to DataflowPlanNodeVisitor, but only for sink nodes."""

    @abstractmethod
    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> VisitorOutputT:  # noqa: D102
        pass


class SinkOutput(DataflowPlanNode, ABC):
    """A node where incoming data goes out of the graph."""

    @abstractmethod
    def accept_sink_node_visitor(self, visitor: SinkNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        pass

    @property
    @abstractmethod
    def parent_node(self) -> BaseOutput:  # noqa: D102
        pass


class DataflowPlan(MetricFlowDag[SinkOutput]):
    """Describes the flow of metric data as it goes from source nodes to sink nodes in the graph."""

    def __init__(self, sink_output_nodes: Sequence[SinkOutput], plan_id: Optional[DagId] = None) -> None:  # noqa: D107
        if len(sink_output_nodes) == 0:
            raise RuntimeError("Can't create a dataflow plan without sink node(s).")
        self._sink_output_nodes = tuple(sink_output_nodes)
        super().__init__(
            dag_id=plan_id or DagId.from_id_prefix(StaticIdPrefix.DATAFLOW_PLAN_PREFIX),
            sink_nodes=tuple(sink_output_nodes),
        )

    @property
    def sink_output_nodes(self) -> Sequence[SinkOutput]:  # noqa: D102
        return self._sink_output_nodes

    @property
    def sink_output_node(self) -> SinkOutput:  # noqa: D102
        assert len(self._sink_output_nodes) == 1, f"Only 1 sink node supported. Got: {self._sink_output_nodes}"
        return self._sink_output_nodes[0]
