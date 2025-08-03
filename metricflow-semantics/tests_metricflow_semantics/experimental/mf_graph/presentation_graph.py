"""`Flow*` classes are an example implementation of a graph used in test cases."""
from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property
from typing import Iterable, Optional

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import (
    DotEdgeAttributeSet,
    DotGraphAttributeSet,
    DotNodeAttributeSet,
)
from metricflow_semantics.experimental.mf_graph.graph_id import SequentialGraphId
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.experimental.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from typing_extensions import override

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class PresentationNode(MetricflowGraphNode, Singleton):
    """Example graph node."""

    node_name: str
    node_label: str
    invisible: bool

    @classmethod
    def get_instance(  # noqa: D102
        cls, node_name: str, node_label: str, invisible: Optional[bool] = None
    ) -> PresentationNode:
        return cls._get_instance(
            node_name=node_name, node_label=node_label, invisible=invisible if invisible is not None else False
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.node_name,)

    @override
    @property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(node_name=self.node_name, cluster_name=None)

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        """Return this as attributes for a DOT node.

        Args:
            include_graphical_attributes: If set, include attributes for a graphical representation.

        Returns: An attribute set that can be used to create the DOT node.
        """
        additional_kwargs: Optional[Mapping[str, str]] = None
        if include_graphical_attributes and self.invisible:
            additional_kwargs = {
                "style": "invis",
            }

        return DotNodeAttributeSet.create(
            name=self.node_descriptor.node_name,
            label=self.node_label,
            additional_kwargs=additional_kwargs,
        )


@fast_frozen_dataclass(order=False)
class PresentationEdge(MetricflowGraphEdge):
    """Example graph edge."""

    invisible: bool

    @staticmethod
    def create(  # noqa: D102
        tail_node: PresentationNode, head_node: PresentationNode, invisible: Optional[bool] = None
    ) -> PresentationEdge:
        return PresentationEdge(tail_node=tail_node, head_node=head_node, invisible=invisible or False)

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return self.tail_node, self.head_node

    @property
    @override
    def inverse(self) -> PresentationEdge:
        return PresentationEdge(tail_node=self.head_node, head_node=self.tail_node, invisible=self.invisible)

    @override
    def as_dot_edge(self, include_graphical_attributes: bool) -> DotEdgeAttributeSet:
        """Return this as attributes for a DOT edge.

        Args:
            include_graphical_attributes: If set, include attributes for a graphical representation.

        Returns: An attribute set that can be used to create the DOT edge.
        """
        if not include_graphical_attributes:
            return DotEdgeAttributeSet.create(
                tail_name=self.tail_node.node_descriptor.node_name,
                head_name=self.head_node.node_descriptor.node_name,
            )

        return DotEdgeAttributeSet.create(
            tail_name=self.tail_node.node_descriptor.node_name,
            head_name=self.head_node.node_descriptor.node_name,
            additional_kwargs={"style": "invis"} if self.invisible else None,
        )


@dataclass
class PresentationGraph(MutableGraph[PresentationNode, PresentationEdge], MetricFlowPrettyFormattable):
    """Example graph."""

    @override
    def as_dot_graph(self, include_graphical_attributes: bool) -> DotGraphAttributeSet:
        return (
            super()
            .as_dot_graph(include_graphical_attributes=include_graphical_attributes)
            .with_attributes(
                dot_kwargs={
                    "rankdir": "TB",
                    "pad": "0.5",
                }
                if include_graphical_attributes
                else {}
            )
        )

    @classmethod
    def create(  # noqa: D102
        cls, nodes: Iterable[PresentationNode] = (), edges: Iterable[PresentationEdge] = ()
    ) -> PresentationGraph:
        graph = PresentationGraph(
            _graph_id=SequentialGraphId.create(),
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _label_to_nodes=defaultdict(MutableOrderedSet),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            _label_to_edges=defaultdict(MutableOrderedSet),
            _node_to_successor_nodes=defaultdict(MutableOrderedSet),
            _node_to_predecessor_nodes=defaultdict(MutableOrderedSet),
        )
        for node in nodes:
            graph.add_node(node)
        for edge in edges:
            graph.add_edge(edge)
        return graph

    @override
    def intersection(self, other: MetricflowGraph[PresentationNode, PresentationEdge]) -> PresentationGraph:
        intersection_graph = PresentationGraph.create()
        self.add_edges(self._intersect_edges(other))
        return intersection_graph

    @override
    def inverse(self) -> PresentationGraph:
        return PresentationGraph.create(edges=(edge.inverse for edge in self.edges))

    @override
    def as_sorted(self) -> PresentationGraph:
        """Return this graph but with nodes and edges sorted."""
        # noinspection PyArgumentList
        updated_graph = PresentationGraph.create()
        for node in sorted(self._nodes):
            updated_graph.add_node(node)

        for edge in sorted(self._edges):
            updated_graph.add_edge(edge)

        return updated_graph
