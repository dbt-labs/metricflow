from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Tuple

from typing_extensions import override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.dag.mf_dag import DagNode
from metricflow_semantics.visitor import Visitable, VisitorOutputT

if TYPE_CHECKING:
    from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.measure_source_node import (
        MeasureGroupByItemSourceNode,
    )
    from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
        MetricGroupByItemResolutionNode,
    )
    from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.no_metrics_query_source_node import (
        NoMetricsGroupByItemSourceNode,
    )
    from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
        QueryGroupByItemResolutionNode,
    )


@dataclass(frozen=True)
class GroupByItemResolutionNode(DagNode["GroupByItemResolutionNode"], Visitable, ABC):
    """Base node type for nodes in a GroupByItemResolutionDag.

    See GroupByItemResolutionDag for more details.
    """

    parent_nodes: Tuple[GroupByItemResolutionNode, ...]

    @abstractmethod
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        raise NotImplementedError

    @property
    @abstractmethod
    def ui_description(self) -> str:
        """A string that can be used to describe this node as a path element in the UI."""
        raise NotImplementedError

    @abstractmethod
    def _self_set(self) -> GroupByItemResolutionNodeSet:
        """Return a `GroupByItemResolutionNodeInclusiveAncestorSet` only containing self.

        Use to simplify implementation of `inclusive_ancestors`
        """
        raise NotImplementedError

    def inclusive_ancestors(self) -> GroupByItemResolutionNodeSet:
        """Return a set containing itself and all its ancestors."""
        return GroupByItemResolutionNodeSet.merge_iterable(
            itertools.chain(
                [self._self_set()], (parent_node.inclusive_ancestors() for parent_node in self.parent_nodes)
            )
        )


class GroupByItemResolutionNodeVisitor(Generic[VisitorOutputT], ABC):
    """Visitor for traversing GroupByItemResolutionNodes."""

    @abstractmethod
    def visit_measure_node(self, node: MeasureGroupByItemSourceNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_no_metrics_query_node(self, node: NoMetricsGroupByItemSourceNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_metric_node(self, node: MetricGroupByItemResolutionNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_query_node(self, node: QueryGroupByItemResolutionNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError


@dataclass(frozen=True)
class GroupByItemResolutionNodeSet(Mergeable):
    """Set containing nodes in a group-by-item resolution DAG."""

    measure_nodes: Tuple[MeasureGroupByItemSourceNode, ...] = ()
    no_metrics_query_nodes: Tuple[NoMetricsGroupByItemSourceNode, ...] = ()
    metric_nodes: Tuple[MetricGroupByItemResolutionNode, ...] = ()
    query_nodes: Tuple[QueryGroupByItemResolutionNode, ...] = ()

    @override
    def merge(self, other: GroupByItemResolutionNodeSet) -> GroupByItemResolutionNodeSet:
        return GroupByItemResolutionNodeSet(
            measure_nodes=self.measure_nodes + other.measure_nodes,
            no_metrics_query_nodes=self.no_metrics_query_nodes + other.no_metrics_query_nodes,
            metric_nodes=self.metric_nodes + other.metric_nodes,
            query_nodes=self.query_nodes + other.query_nodes,
        )

    @classmethod
    @override
    def empty_instance(cls) -> GroupByItemResolutionNodeSet:
        return GroupByItemResolutionNodeSet()
