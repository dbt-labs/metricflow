"""Base classes for modeling a graph.

The graph in `networkx` could be used, but the classes there were not well typed.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter

from metricflow_semantics.experimental.mf_graph.comparable import Comparable
from metricflow_semantics.experimental.mf_graph.displayable_graph_element import (
    MetricflowGraphElement,
    MetricflowGraphProperty,
)
from metricflow_semantics.experimental.mf_graph.formatting.dot_formatter import DotNotationFormatter
from metricflow_semantics.experimental.ordered_set import OrderedSet

logger = logging.getLogger(__name__)


class MetricflowGraphNode(MetricflowGraphElement, Comparable, ABC):
    """Base class for nodes in a graph."""

    pass


NodeT = TypeVar("NodeT", bound=MetricflowGraphNode)


class MetricflowGraphEdge(MetricflowGraphElement, Comparable, Generic[NodeT], ABC):
    """Base class for edges in a graph.

    An edge can be visualized as an arrow that points from the tail node to the head node.
    """

    @property
    @abstractmethod
    def tail_node(self) -> NodeT:  # noqa: D102
        raise NotImplementedError()

    @property
    @abstractmethod
    def head_node(self) -> NodeT:  # noqa: D102
        raise NotImplementedError()

    @property
    @abstractmethod
    def inverse(self: EdgeT) -> EdgeT:
        """Return the edge with the head and tail swapped."""
        raise NotImplementedError()


EdgeT = TypeVar("EdgeT", bound=MetricflowGraphEdge)


class MetricflowGraph(MetricflowGraphElement, Generic[NodeT, EdgeT], ABC):
    """Base class for a graph."""

    @abstractmethod
    def nodes(self) -> OrderedSet[NodeT]:
        """Return the set of nodes in the graph."""
        raise NotImplementedError()

    @abstractmethod
    def nodes_with_property(self, graph_property: MetricflowGraphProperty) -> OrderedSet[NodeT]:
        raise NotImplementedError()

    @abstractmethod
    def edges(self) -> OrderedSet[EdgeT]:
        """Return the set of edges in a graph."""
        raise NotImplementedError()

    @abstractmethod
    def edges_with_tail_node(self, tail_node: NodeT) -> OrderedSet[EdgeT]:
        raise NotImplementedError()

    def format_svg(self) -> str:
        """Return a representation of the graph in SVG format."""
        return SvgFormatter().format_graph(self)

    def format_dot(self) -> str:
        """Return a representation of the graph in DOT format."""
        return DotNotationFormatter().format_graph(self)
