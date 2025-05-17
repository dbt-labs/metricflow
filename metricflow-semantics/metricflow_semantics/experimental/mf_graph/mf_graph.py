"""Base classes for modeling a graph.

The graph in `networkx` could be used, but the classes there were not well typed.
"""
from __future__ import annotations

import logging
import textwrap
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Generic, Optional, TypeVar

from typing_extensions import Self, override

from metricflow_semantics.collection_helpers.mf_type_aliases import Pair
from metricflow_semantics.experimental.mf_graph.comparable import Comparable
from metricflow_semantics.experimental.mf_graph.displayable_graph_element import (
    MetricflowGraphElement,
    MetricflowGraphLabel,
)
from metricflow_semantics.experimental.mf_graph.formatting.default_graph_formatter import DefaultGraphFormatter
from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import MetricflowGraphFormatter
from metricflow_semantics.experimental.mf_graph.formatting.graphviz_attributes import (
    DotEdgeAttributeSet,
    DotNodeAttributeSet,
    GraphvizShape,
)
from metricflow_semantics.experimental.mf_graph.formatting.graphviz_html import (
    GraphvizHtmlAlignment,
    GraphvizHtmlText,
    GraphvizHtmlTextStyle,
)
from metricflow_semantics.experimental.mf_graph.formatting.graphviz_html_table_builder import (
    GraphvizHtmlTableBuilder,
)
from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)

EdgeT = TypeVar("EdgeT", bound="MetricflowGraphEdge", covariant=True)
NodeT = TypeVar("NodeT", bound="MetricflowGraphNode", covariant=True)


class MetricflowGraphNode(MetricflowGraphElement, Comparable, ABC):
    """Base class for nodes in a directed graph."""

    @property
    def dot_attributes(self) -> DotNodeAttributeSet:
        """Attributes to supply to `graphviz` calls when rendering this element."""
        return DotNodeAttributeSet.create(
            name=self.node_descriptor.node_name,
            label=self._make_dot_label(),
            shape=GraphvizShape.BOX,
        )

    @property
    @abstractmethod
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        raise NotImplementedError

    @property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet()

    def _make_dot_label(self) -> str:
        """Return a `graphviz` / DOT label to render this node.

        See https://graphviz.org/doc/info/shapes.html#html for HTML formatting.

        Currently, this return a `graphviz` HTML table where the first row is 1 column with the ID of
        the node, and subsequent rows have 2 columns to show the `node.displayed_properties` (key and value).
        """
        table_builder = GraphvizHtmlTableBuilder()
        word_wrap_limit = 30
        with table_builder.new_row_builder() as row_builder:
            row_builder.add_column(
                GraphvizHtmlText(self.node_descriptor.node_name, style=GraphvizHtmlTextStyle.TITLE),
                alignment=GraphvizHtmlAlignment.CENTER,
                column_span=2,
                cell_padding=4,
            )

        for displayed_property in self.displayed_properties:
            key = displayed_property.key
            value_lines = textwrap.wrap(str(displayed_property.value), width=word_wrap_limit)
            # <BR> seems to have odd formatting issues, so using multiple rows to display mult-line text.
            if len(value_lines) == 0:
                continue

            for row_index, value_line in enumerate(value_lines):
                with table_builder.new_row_builder() as row_builder:
                    if row_index == 0:
                        row_builder.add_column(
                            GraphvizHtmlText(key + " ", GraphvizHtmlTextStyle.DESCRIPTION),
                            alignment=GraphvizHtmlAlignment.LEFT,
                        )
                        row_builder.add_column(
                            GraphvizHtmlText(
                                value_line,
                                GraphvizHtmlTextStyle.DESCRIPTION,
                            ),
                            alignment=GraphvizHtmlAlignment.LEFT,
                        )
                    else:
                        row_builder.add_column()
                        row_builder.add_column(
                            GraphvizHtmlText(value_line, GraphvizHtmlTextStyle.DESCRIPTION),
                            alignment=GraphvizHtmlAlignment.LEFT,
                        )

        result = "\n".join(table_builder.build())
        # logger.debug(LazyFormat("Generated HTML label for a node", node=self, html=result))
        return result


@singleton_dataclass(order=False)
class MetricflowGraphEdge(MetricflowGraphElement, Comparable, Generic[NodeT], ABC):
    """Base class for edges in a directed graph.

    An edge can be visualized as an arrow that points from the tail node to the head node.
    """

    # TODO: Check if these can be renamed / remove property accessors.
    _tail_node: NodeT
    _head_node: NodeT
    _weight: Optional[int]

    @property
    def tail_node(self) -> NodeT:  # noqa: D102
        return self._tail_node

    @property
    def head_node(self) -> NodeT:  # noqa: D102
        return self._head_node

    @property
    def node_pair(self) -> Pair[NodeT, NodeT]:
        return (self._tail_node, self._head_node)

    @property
    @abstractmethod
    def inverse(self) -> Self:
        raise NotImplementedError()

    @override
    @cached_property
    def dot_attributes(self) -> DotEdgeAttributeSet:
        # Add some padding so that the label is separated from the other shapes.
        tail_node_name = self.tail_node.node_descriptor.node_name
        head_node_name = self.head_node.node_descriptor.node_name
        table_builder = GraphvizHtmlTableBuilder(table_cell_padding=10)
        with table_builder.new_row_builder() as row_builder:
            row_builder.add_column(GraphvizHtmlText(f"{tail_node_name}\n(weight:{self._weight})->\n{head_node_name}"))

        label = "\n".join(table_builder.build())

        return DotEdgeAttributeSet.create(
            tail_node_name=tail_node_name,
            head_node_name=head_node_name,
            label=label,
        )

    @property
    def weight(self) -> Optional[int]:
        return self._weight

    @property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet()


class MetricflowGraph(MetricFlowPrettyFormattable, Generic[NodeT, EdgeT], ABC):
    """Base class for a directed graph."""

    @property
    @abstractmethod
    def nodes(self) -> OrderedSet[NodeT]:
        """Return the set of nodes in the graph."""
        raise NotImplementedError()

    @abstractmethod
    def nodes_with_label(self, graph_label: MetricflowGraphLabel) -> OrderedSet[NodeT]:
        """Return nodes in the graph with the given property."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def edges(self) -> OrderedSet[EdgeT]:
        """Return the set of edges in a graph."""
        raise NotImplementedError()

    @abstractmethod
    def edges_with_tail_node(self, tail_node: MetricflowGraphNode) -> OrderedSet[EdgeT]:
        """Returns edges with the given tail node.

        Raises `UnknownNodeException` if the node does not exist in the graph.
        """
        raise NotImplementedError()

    @abstractmethod
    def edges_with_head_node(self, tail_node: MetricflowGraphNode) -> OrderedSet[EdgeT]:
        """Returns edges with the given head node.

        Raises `UnknownNodeException` if the node does not exist in the graph.
        """
        raise NotImplementedError()

    @abstractmethod
    def edges_with_label(self, label: MetricflowGraphLabel) -> OrderedSet[EdgeT]:
        """Return the set of edges in a graph that have the given label."""
        raise NotImplementedError()

    def format(self, formatter: MetricflowGraphFormatter = DefaultGraphFormatter()) -> str:
        """Return a text representation of this graph using the given formatter."""
        return formatter.format_graph(self)

    @abstractmethod
    def successors(self, node: MetricflowGraphNode) -> OrderedSet[NodeT]:
        """Returns successors of the given node.

        Raises `UnknownNodeException` if the node does not exist in the graph.
        """
        return FrozenOrderedSet(edge.head_node for edge in self.edges_with_tail_node(node))

    @abstractmethod
    def predecessors(self, node: MetricflowGraphNode) -> OrderedSet[NodeT]:
        """Returns predecessors of the given node.

        Raises `UnknownNodeException` if the node does not exist in the graph.
        """
        return FrozenOrderedSet(edge.tail_node for edge in self.edges_with_head_node(node))

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> str:
        formatter = format_context.formatter
        return formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={"nodes": self.nodes, "edges": self.edges},
        )

    @abstractmethod
    def as_sorted(self) -> Self:
        raise NotImplementedError

    def _intersect_edges(self, other: MetricflowGraph[NodeT, EdgeT]) -> FrozenOrderedSet[EdgeT]:
        return FrozenOrderedSet(edge for edge in self.edges if edge in other.edges)

    @abstractmethod
    def intersection(self, other: MetricflowGraph[NodeT, EdgeT]) -> Self:
        raise NotImplementedError()

    @abstractmethod
    def inverse(self) -> Self:
        raise NotImplementedError()
