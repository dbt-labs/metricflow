from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from typing_extensions import override

from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe import (
    AttributeRecipe,
)
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.semantic_graph.sg_interfaces import SemanticGraphEdge, SemanticGraphNode
from metricflow_semantics.toolkit.mf_graph.path_finding.graph_path import MutableGraphPath
from metricflow_semantics.toolkit.mf_graph.path_finding.pathfinder import MetricFlowPathfinder
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)

_EMPTY_RECIPE = AttributeRecipe()


@dataclass
class AttributeRecipeWriterPath(MutableGraphPath[SemanticGraphNode, SemanticGraphEdge], MetricFlowPrettyFormattable):
    """An implementation of a path in the semantic graph that writes recipes.

    The nodes and edges in the semantic graph are annotated with recipe steps that describe the computation of
    attributes. This path takes those steps and merges them into a single recipe as nodes are added to the path.

    In DFS traversal, nodes are added and popped at the end. To support this operation, recipe versions are
    stored in a list so that consistent state can be maintained.
    """

    # Every time a node / edge is added, the updated recipe is added to this list.
    _recipe_versions: list[AttributeRecipe]

    @staticmethod
    def create(start_node: Optional[SemanticGraphNode] = None) -> AttributeRecipeWriterPath:  # noqa: D102
        path = AttributeRecipeWriterPath(
            _nodes=[],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=set(),
            _node_set_addition_order=[],
            _recipe_versions=[_EMPTY_RECIPE],
        )
        if start_node:
            path._append_node(start_node)
            path._append_step(start_node.recipe_step_to_append)
        return path

    @staticmethod
    def create_from_edge(start_edge: SemanticGraphEdge, weight: int) -> AttributeRecipeWriterPath:  # noqa: D102
        path = AttributeRecipeWriterPath(
            _nodes=[],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=set(),
            _node_set_addition_order=[],
            _recipe_versions=[_EMPTY_RECIPE],
        )
        path.append_edge(start_edge, weight)
        return path

    def _append_step(self, recipe_step: AttributeRecipeStep) -> None:
        previous_recipe = self._recipe_versions[-1]
        self._recipe_versions.append(previous_recipe.append_step(recipe_step))

    @property
    def latest_recipe(self) -> AttributeRecipe:  # noqa: D102
        return self._recipe_versions[-1]

    @override
    def append_edge(self, edge: SemanticGraphEdge, weight: int) -> None:
        """Add an edge with the given weight to this path."""
        if self.is_empty:
            self._append_step(edge.tail_node.recipe_step_to_append)
        self._append_step(edge.recipe_step_to_append)
        self._append_step(edge.head_node.recipe_step_to_append)

        super().append_edge(edge, weight)

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format(self._nodes)

    @override
    def pop_end(self) -> None:
        if self._edges:
            self._recipe_versions.pop()
            self._recipe_versions.pop()
        else:
            self._recipe_versions.pop()
        super().pop_end()

    @override
    def copy(self) -> AttributeRecipeWriterPath:
        return AttributeRecipeWriterPath(
            _nodes=self._nodes.copy(),
            _edges=self._edges.copy(),
            _current_weight=self._current_weight,
            _current_node_set=self._current_node_set.copy(),
            _weight_addition_order=self._weight_addition_order.copy(),
            _node_set_addition_order=self._node_set_addition_order.copy(),
            _recipe_versions=self._recipe_versions.copy(),
        )


RecipeWriterPathfinder = MetricFlowPathfinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]
