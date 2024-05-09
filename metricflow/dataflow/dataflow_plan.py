"""Nodes for building a dataflow plan."""

from __future__ import annotations

import logging
import typing
from abc import ABC, abstractmethod
from typing import Generic, Optional, Sequence, Set, Type, TypeVar

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, DagNode, MetricFlowDag, NodeId
from metricflow_semantics.specs.spec_classes import LinkableInstanceSpec
from metricflow_semantics.visitor import Visitable, VisitorOutputT

if typing.TYPE_CHECKING:
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
    def with_new_parents(self: NodeSelfT, new_parent_nodes: Sequence[DataflowPlanNode]) -> NodeSelfT:
        """Creates a node with the same behavior as this node, but with a different set of parents.

        typing.Self would be useful here, but not available in Python 3.8.
        """
        raise NotImplementedError

    @property
    def node_type(self) -> Type:  # noqa: D102
        # TODO: Remove.
        return self.__class__

    @property
    def aggregated_to_elements(self) -> Set[LinkableInstanceSpec]:
        """Indicates that the node has been aggregated to these specs, guaranteeing uniqueness in all combinations."""
        return set()


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
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> VisitorOutputT:  # noqa: D102
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
    def visit_filter_elements_node(self, node: FilterElementsNode) -> VisitorOutputT:  # noqa: D102
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


class DataflowPlan(MetricFlowDag[DataflowPlanNode]):
    """Describes the flow of metric data as it goes from source nodes to sink nodes in the graph."""

    def __init__(self, sink_nodes: Sequence[DataflowPlanNode], plan_id: Optional[DagId] = None) -> None:  # noqa: D107
        assert len(sink_nodes) == 1, f"Exactly 1 sink node is supported. Got: {sink_nodes}"
        super().__init__(
            dag_id=plan_id or DagId.from_id_prefix(StaticIdPrefix.DATAFLOW_PLAN_PREFIX),
            sink_nodes=tuple(sink_nodes),
        )

    @property
    def checked_sink_node(self) -> DataflowPlanNode:
        """If this has a single sink node, return it. Otherwise, raise an exception."""
        assert len(self._sink_nodes) == 1, f"Exactly 1 sink node is supported. Got: {self._sink_nodes}"
        return self._sink_nodes[0]
