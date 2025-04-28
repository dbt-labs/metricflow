"""Base classes for modeling a graph.

The graph in `networkx` could be used, but the classes there were not well typed.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from metricflow_semantics.collection_helpers.ordered_set import OrderedSet
from metricflow_semantics.experimental.mf_graph.comparable import Comparable
from metricflow_semantics.experimental.mf_graph.displayable_graph_element import DisplayableGraphElement
from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import GraphFormatter, OutputT

logger = logging.getLogger(__name__)


class MetricflowGraphNode(DisplayableGraphElement, Comparable, ABC):
    """Base class for nodes in a graph."""

    pass


NodeT = TypeVar("NodeT", bound=MetricflowGraphNode)
EdgeT = TypeVar("EdgeT", bound="MetricflowGraphEdge")


class MetricflowGraphEdge(DisplayableGraphElement, Comparable, Generic[NodeT], ABC):
    """Base class for edges in a graph."""

    @property
    @abstractmethod
    def tail_node(self) -> NodeT:
        raise NotImplementedError()

    @property
    @abstractmethod
    def head_node(self) -> NodeT:
        raise NotImplementedError()

    @property
    @abstractmethod
    def inverse(self: EdgeT) -> EdgeT:
        """Return the edge with the head and tail swapped."""
        raise NotImplementedError()


GraphT = TypeVar("GraphT", bound="MetricflowGraph")


class MetricflowGraph(DisplayableGraphElement, Generic[NodeT, EdgeT], ABC):
    """Base class for a graph."""

    @property
    @abstractmethod
    def nodes(self) -> OrderedSet[NodeT]:
        """Return the set of nodes in the graph."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def edges(self) -> OrderedSet[EdgeT]:
        """Return the set of edges in a graph."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def inverse(self: GraphT) -> GraphT:
        """Return the inverse graph (i.e. edges reversed)."""
        raise NotImplementedError()

    # @staticmethod
    # def _dot_node_section(nodes: Iterable[MetricflowGraphNode]) -> str:
    #     return "\n".join(node.dot_label for node in nodes)
    #
    # @staticmethod
    # def _dot_edge_section(edges: Iterable[MetricflowGraphEdge]) -> str:
    #     lines = []
    #     for edge in edges:
    #         edge_label = edge.dot_label
    #         if "\n" in edge_label:
    #             lines.append(f'{edge.tail_node.dot_label} -> {edge.head_node.dot_label} [label="')
    #             lines.append(mf_indent(edge_label))
    #             lines.append('"]')
    #         else:
    #             lines.append(f'{edge.tail_node.dot_label} -> {edge.head_node.dot_label} [label="{edge.dot_label}"]')
    #     return "\n".join(lines)
    #
    # @cached_property
    # def dot_format(self) -> str:
    #     """Returns the graph in DOT notation format."""
    #     lines = [
    #         f"graph {self.dot_label} {{",
    #         mf_indent(self._dot_node_section(sorted(self.nodes))),
    #         mf_indent(self._dot_edge_section(sorted(self.edges))),
    #         "}",
    #     ]
    #     return "\n".join(lines)

    def format_graph(self, graph_formatter: GraphFormatter[OutputT]) -> OutputT:
        return graph_formatter.format_graph(self)
