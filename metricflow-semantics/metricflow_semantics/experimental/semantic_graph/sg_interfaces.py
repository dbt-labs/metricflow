from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property

from typing_extensions import Optional, override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import DotGraphAttributeSet
from metricflow_semantics.experimental.mf_graph.graph_id import MetricflowGraphId, SequentialGraphId
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.experimental.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe import (
    QueryRecipeStepAppender,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe_update import (
    QueryRecipeStep,
)
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


class SemanticGraphNode(MetricflowGraphNode, QueryRecipeStepAppender, MetricFlowPrettyFormattable, ABC):
    """A node in the semantic graph.

    At the top level, the semantic graph has two types of nodes. Entity nodes and attribute nodes. Entity nodes have
    other entity nodes or attribute nodes as successors while attribute nodes do not have any successors (attribute
    nodes are leaf / terminal nodes).

    In the semantic graph, the number of entity nodes are a superset of the entities configured in the semantic manifest.
    The one that corresponds to the entity in the manifest is the `ConfiguredEntityNode`. However, there are other
    entity nodes that are used to model implicit relationships. For example, `metric_time` is represented by the
    metric-time entity node, and there is a time-entity node that relates time dimensions (and metric-time) to the
    various time grains. The time grains (e.g. `day`, `year`) are attribute nodes (e.g. `day` corresponds to an
    attribute node named `day`). Please see the different semantic-graph node classes for more details.
    """

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return self.node_descriptor.node_name

    @override
    @cached_property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties: list[DisplayedProperty] = list(super().displayed_properties)
        if self.recipe_step is not None:
            properties.extend(self.recipe_step.displayed_properties)
        properties.extend(DisplayedProperty("label", label) for label in self.labels)
        return tuple(properties)

    @cached_property
    def dunder_name_element(self) -> Optional[str]:
        """This node's associated dunder-name element in the query interface.

        Not all nodes in the semantic graph have a direct relationship to the dunder name, but many do. e.g. the
        metric time entity corresponds to the 'metric_time' dunder-name element.
        """
        return self.recipe_step.add_dunder_name_element


class SemanticGraphEdge(MetricflowGraphEdge[SemanticGraphNode], QueryRecipeStepAppender, ABC):
    """An edge in the semantic graph.

    Currently, the edges in the semantic graph represent entity relationships / metric relationships. An edge in the
    semantic graph also describes how a particular attribute can be computed from the entity relationship
    (an "attribute recipe").

    For example, if a semantic model contains the `listings` foreign entity and another semantic model contains the
    `listings` primary entity, there would be a path from the node representing the first semantic model to the 2nd
    semantic model (via a configured-entity node for `listings`). The path's edges includes a recipe step that
    includes a join between the two semantic models.
    """

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        formatter = format_context.formatter
        return formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "tail_node": self._tail_node,
                "head_node": self._head_node,
            },
        )

    @override
    @cached_property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties: list[DisplayedProperty] = list(super().displayed_properties)
        if self.recipe_step is not None:
            properties.extend(self.recipe_step.displayed_properties)

        return tuple(properties)

    @cached_property
    def recipe_step_for_path_addition(self) -> QueryRecipeStep:
        """In the context of a path to build the query recipe, the step that this edge + next node would add ."""
        return QueryRecipeStep()


class SemanticGraph(MetricflowGraph[SemanticGraphNode, SemanticGraphEdge], ABC):
    """A read-only interface for a semantic graph based on `MetricflowGraph`.

    First see: `SemanticGraphNode` and `SemanticGraphEdge`.

    The semantic graph helps to model entity relationships that are defined in the semantic manifest and encodes
    context on how attributes for entities can be computed (e.g. by joining semantic models).

    Currently, the edges in the graph are oriented so that a path from a metric node to an attribute node describes
    the query required to compute that metric. However, additional edge types can be added to better model associative
    entity relationships.
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
    """A mutable implementation of `SemanticGraph` helpful for building graphs."""

    @classmethod
    def create(cls, graph_id: Optional[MetricflowGraphId] = None) -> MutableSemanticGraph:  # noqa: D102
        return MutableSemanticGraph(
            _graph_id=graph_id or SequentialGraphId.create(),
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            _label_to_nodes=defaultdict(MutableOrderedSet),
            _label_to_edges=defaultdict(MutableOrderedSet),
        )

    @override
    def intersection(self, other: MetricflowGraph[SemanticGraphNode, SemanticGraphEdge]) -> MutableSemanticGraph:
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
