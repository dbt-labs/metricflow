"""Nodes for building a dataflow plan."""

from __future__ import annotations

import functools
import logging
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import FrozenSet, Optional, Sequence, Set, Type, TypeVar

import more_itertools
from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, DagNode, MetricFlowDag
from metricflow_semantics.toolkit.comparison_helpers import ComparisonOtherType
from metricflow_semantics.toolkit.visitor import Visitable, VisitorOutputT

if typing.TYPE_CHECKING:
    from dbt_semantic_interfaces.references import SemanticModelReference
    from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec

    from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor

logger = logging.getLogger(__name__)

NodeSelfT = TypeVar("NodeSelfT", bound="DataflowPlanNode")


@functools.total_ordering
@dataclass(frozen=True, eq=False)
class DataflowPlanNode(DagNode["DataflowPlanNode"], Visitable, ABC):
    """A node in the graph representation of the dataflow.

    Each node in the graph performs an operation from the data that comes from the parent nodes, and the result is
    passed to the child nodes. The flow of data starts from source nodes, and ends at sink nodes.
    """

    @property
    def _input_semantic_model(self) -> Optional[SemanticModelReference]:
        """Return the semantic model serving as direct input for this node, if one exists."""
        return None

    def as_plan(self) -> DataflowPlan:
        """Converter method for taking an arbitrary mode and producing an associated DataflowPlan.

        This is useful for doing lookups for plan-level properties at points in the call stack where we only have
        a subgraph of a complete plan. For example, the total number of nodes represented by this node and all of
        its parents would be a property of a given subgraph of the DAG. Rather than doing recursive property walks
        inside of each node, we make those properties of the DataflowPlan, and this node-level converter makes
        such properties easily accessible.
        """
        return DataflowPlan(
            sink_nodes=(self,), plan_id=DagId.from_id_prefix(id_prefix=StaticIdPrefix.DATAFLOW_PLAN_SUBGRAPH_PREFIX)
        )

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

        Callers are required to call this method with new parent nodes that are of the appropriate type and order as
        required by the subclass.

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

    def __lt__(self, other: ComparisonOtherType) -> bool:  # noqa: D105
        if not isinstance(other, DataflowPlanNode):
            raise NotImplementedError

        return self.node_id < other.node_id


class DataflowPlan(MetricFlowDag[DataflowPlanNode]):
    """Describes the flow of metric data as it goes from source nodes to sink nodes in the graph."""

    def __init__(self, sink_nodes: Sequence[DataflowPlanNode], plan_id: Optional[DagId] = None) -> None:  # noqa: D107
        assert len(sink_nodes) == 1, f"Exactly 1 sink node is supported. Got: {sink_nodes}"
        super().__init__(
            dag_id=plan_id or DagId.from_id_prefix(StaticIdPrefix.DATAFLOW_PLAN_PREFIX),
            sink_nodes=tuple(sink_nodes),
        )

    @property
    def sink_node(self) -> DataflowPlanNode:  # noqa: D102
        return self._sink_nodes[0]

    @property
    def node_count(self) -> int:
        """Returns the number of nodes in the DataflowPlan."""
        return len(DataflowPlan.__all_nodes_in_subgraph(self.sink_node))

    @staticmethod
    def __all_nodes_in_subgraph(node: DataflowPlanNode) -> Sequence[DataflowPlanNode]:
        """Node accessor for retrieving a flattened sequence of all nodes in the subgraph upstream of the input node.

        Useful for gathering nodes for subtype-agnostic operations, such as common property access or simple counts.
        """
        flattened_parent_subgraphs = tuple(
            more_itertools.collapse(
                DataflowPlan.__all_nodes_in_subgraph(parent_node) for parent_node in node.parent_nodes
            )
        )
        return (node,) + flattened_parent_subgraphs

    @property
    def source_semantic_models(self) -> FrozenSet[SemanticModelReference]:
        """Return the complete set of source semantic models for this DataflowPlan."""
        return frozenset(
            [
                node._input_semantic_model
                for node in DataflowPlan.__all_nodes_in_subgraph(self.sink_node)
                if node._input_semantic_model is not None
            ]
        )
