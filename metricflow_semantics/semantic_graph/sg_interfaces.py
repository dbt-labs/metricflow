from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass

from typing_extensions import Optional, override

from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStepProvider,
)
from metricflow_semantics.semantic_graph.sg_node_grouping import SemanticGraphNodeTypedCollection
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.formatting.dot_attributes import DotGraphAttributeSet
from metricflow_semantics.toolkit.mf_graph.graph_id import MetricFlowGraphId, SequentialGraphId
from metricflow_semantics.toolkit.mf_graph.mf_graph import (
    MetricFlowGraph,
    MetricFlowGraphEdge,
    MetricFlowGraphNode,
)
from metricflow_semantics.toolkit.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


class SemanticGraphNode(MetricFlowGraphNode, AttributeRecipeStepProvider, MetricFlowPrettyFormattable, ABC):
    """A node in the semantic graph.

    At the top level, the semantic graph has two types of nodes: entity nodes and attribute nodes. Entity nodes have
    other entity nodes or attribute nodes as successors while attribute nodes do not have any successors (leaf nodes).

    The entity nodes in the semantic graph are not directly mapped from entities configured in the semantic manifest.
    The one that corresponds to an entity in the manifest are instances of `ConfiguredEntityNode`. However, there
    are other entity nodes that model other relationships. For example, `metric_time` is represented by the
    metric-time entity node, and there is a time-entity node that relates time dimensions and metric time to the
    various time grains (the time grains (e.g. `day`, `year`) map to attribute nodes).

    Please see the subclasses for more details.
    """

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return self.node_descriptor.node_name

    @property
    @override
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties: list[DisplayedProperty] = list(super().displayed_properties)
        if self.recipe_step_to_append is not None:
            properties.extend(self.recipe_step_to_append.displayed_properties)
        properties.extend(DisplayedProperty("label", label) for label in self.labels)
        return tuple(properties)

    @property
    def dunder_name_element(self) -> Optional[str]:
        """This node's associated dunder-name element in the query interface.

        Not all nodes in the semantic graph have a direct relationship to the dunder name, but many do. e.g. the
        metric time entity corresponds to the 'metric_time' dunder-name element.
        """
        return self.recipe_step_to_append.add_dunder_name_element

    @abstractmethod
    def add_to_typed_collection(self, typed_collection: SemanticGraphNodeTypedCollection) -> None:
        """Add this node to the given typed collection."""
        raise NotImplementedError


@fast_frozen_dataclass(order=False)
class SemanticGraphEdge(MetricFlowGraphEdge[SemanticGraphNode], AttributeRecipeStepProvider, ABC):
    """An edge in the semantic graph.

    Currently, the edges in the semantic graph represent entity relationships and also describe how a related attribute
    can be computed from the relationship (see `AttributeRecipe`).

    For example, if a semantic model contains the `listings` foreign entity and another semantic model contains the
    `listings` primary entity, there would be a path from the node representing the first semantic model to the second
    semantic model (via a configured-entity node for `listings`). The path's edges includes a recipe step that
    describes a join between the two semantic models.
    """

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        formatter = format_context.formatter
        return formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "tail_node": self.tail_node,
                "head_node": self.head_node,
            },
        )

    @property
    @override
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties: list[DisplayedProperty] = list(super().displayed_properties)
        if self.recipe_step_to_append is not None:
            properties.extend(self.recipe_step_to_append.displayed_properties)

        return tuple(properties)


class SemanticGraph(MetricFlowGraph[SemanticGraphNode, SemanticGraphEdge], ABC):
    """A read-only interface for a semantic graph based on `MetricFlowGraph`.

    Also see `SemanticGraphNode` and `SemanticGraphEdge`.

    The semantic graph helps to model entity relationships that are defined in the semantic manifest and encodes
    context on how attributes for entities can be computed (e.g. by joining semantic models).

    Currently, the edges in the graph are oriented so that a path from a metric node to an attribute node describes
    the query required to compute that metric. However, additional edge types can be added to better model associative
    entity relationships.

    Note: Some changes obviated the inverse graph, and it needs to be either refined or removed.
    """

    @override
    def as_dot_graph(self, include_graphical_attributes: bool) -> DotGraphAttributeSet:
        return (
            super()
            .as_dot_graph(include_graphical_attributes=include_graphical_attributes)
            .with_attributes(
                dot_kwargs={
                    "rankdir": "LR",
                }
                if include_graphical_attributes
                else {}
            )
        )


@dataclass
class MutableSemanticGraph(MutableGraph[SemanticGraphNode, SemanticGraphEdge], SemanticGraph):
    """A mutable implementation of `SemanticGraph` for building graphs."""

    @classmethod
    def create(cls, graph_id: Optional[MetricFlowGraphId] = None) -> MutableSemanticGraph:  # noqa: D102
        return MutableSemanticGraph(
            _graph_id=graph_id or SequentialGraphId.create(),
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            _label_to_nodes=defaultdict(MutableOrderedSet),
            _label_to_edges=defaultdict(MutableOrderedSet),
            _node_to_predecessor_nodes=defaultdict(MutableOrderedSet),
            _node_to_successor_nodes=defaultdict(MutableOrderedSet),
        )

    @override
    def intersection(self, other: MetricFlowGraph[SemanticGraphNode, SemanticGraphEdge]) -> MutableSemanticGraph:
        intersection_graph = MutableSemanticGraph.create(graph_id=self.graph_id)
        self.add_edges(self._intersect_edges(other))
        return intersection_graph

    @override
    def inverse(self) -> MutableSemanticGraph:
        inverse_graph = MutableSemanticGraph.create()
        for edge in self.edges:
            inverse_graph.add_edge(edge.inverse)
        return inverse_graph

    @override
    def as_sorted(self) -> MutableSemanticGraph:
        updated_graph = MutableSemanticGraph.create(graph_id=self.graph_id)
        for node in sorted(self._nodes):
            updated_graph.add_node(node)

        for edge in sorted(self._edges):
            updated_graph.add_edge(edge)

        return updated_graph
