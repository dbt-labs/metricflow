from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, Sequence

from typing_extensions import override

from metricflow.dag.id_prefix import IdPrefix
from metricflow.dag.mf_dag import DagNode, NodeId
from metricflow.visitor import Visitable, VisitorOutputT

if TYPE_CHECKING:
    from metricflow.query.group_by_item.resolution_dag.resolution_nodes.measure_source_node import (
        MeasureGroupByItemSourceNode,
    )
    from metricflow.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
        MetricGroupByItemResolutionNode,
    )
    from metricflow.query.group_by_item.resolution_dag.resolution_nodes.no_metrics_query_source_node import (
        NoMetricsGroupByItemSourceNode,
    )
    from metricflow.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
        QueryGroupByItemResolutionNode,
    )


class GroupByItemResolutionNode(DagNode, Visitable, ABC):
    """Base node type for nodes in a GroupByItemResolutionDag.

    See GroupByItemResolutionDag for more details.
    """

    def __init__(self) -> None:  # noqa: D
        super().__init__(node_id=NodeId.create_unique(self.__class__.id_prefix_enum()))

    @abstractmethod
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        raise NotImplementedError

    @property
    @abstractmethod
    def ui_description(self) -> str:
        """A string that can be used to describe this node as a path element in the UI."""
        raise NotImplementedError

    @property
    @abstractmethod
    def parent_nodes(self) -> Sequence[GroupByItemResolutionNode]:  # noqa: D
        raise NotImplementedError

    @classmethod
    @override
    def id_prefix(cls) -> str:
        return cls.id_prefix()

    @classmethod
    @abstractmethod
    def id_prefix_enum(cls) -> IdPrefix:
        """The ID prefix as an enum instead of a string.

        TODO: Update other node classes to use the enum, then replace the existing id_prefix.
        """
        raise NotImplementedError


class GroupByItemResolutionNodeVisitor(Generic[VisitorOutputT], ABC):
    """Visitor for traversing GroupByItemResolutionNodes."""

    @abstractmethod
    def visit_measure_node(self, node: MeasureGroupByItemSourceNode) -> VisitorOutputT:  # noqa: D
        raise NotImplementedError

    @abstractmethod
    def visit_no_metrics_query_node(self, node: NoMetricsGroupByItemSourceNode) -> VisitorOutputT:  # noqa: D
        raise NotImplementedError

    @abstractmethod
    def visit_metric_node(self, node: MetricGroupByItemResolutionNode) -> VisitorOutputT:  # noqa: D
        raise NotImplementedError

    @abstractmethod
    def visit_query_node(self, node: QueryGroupByItemResolutionNode) -> VisitorOutputT:  # noqa: D
        raise NotImplementedError
