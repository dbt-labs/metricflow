from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import ClassVar, Optional

from typing_extensions import override

from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeRecipeUpdate,
    AttributeRecipeWriter,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import MutableGraphPath
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


@dataclass
class AttributeRecipeWriterPath(MutableGraphPath[SemanticGraphNode, SemanticGraphEdge], MetricFlowPrettyFormattable):
    recipe_writer: AttributeRecipeWriter

    _verbose_debug_logs: ClassVar[bool] = False

    @staticmethod
    def create() -> AttributeRecipeWriterPath:
        return AttributeRecipeWriterPath(
            _nodes=[],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=MutableOrderedSet(),
            _node_set_addition_order=[],
            recipe_writer=AttributeRecipeWriter(),
        )

    def _append_update(self, update: AttributeRecipeUpdate) -> None:
        self.recipe_writer.append_update(update)

    @override
    def _node_addition_callback(self, node: SemanticGraphNode) -> None:
        self._append_update(node.attribute_recipe_update)

    def _edge_addition_callback(self, edge: SemanticGraphEdge) -> None:
        self._append_update(edge.attribute_recipe_update)

    @override
    def append_edge(self, edge: SemanticGraphEdge, weight: int) -> None:
        super().append_edge(edge, weight)
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Appended edge to computation path",
                    edge=edge,
                    previous_attribute_descriptor=self.recipe_writer.previous_recipe,
                    current_attribute_descriptor=self.recipe_writer.latest_recipe,
                )
            )

    @override
    def pop(self) -> None:
        if len(self._nodes) == 1 and len(self._edges) == 0:
            self.recipe_writer.pop_update()

        if len(self._edges) != 0:
            self.recipe_writer.pop_update()
            self.recipe_writer.pop_update()
        super().pop()

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format(self._nodes)
