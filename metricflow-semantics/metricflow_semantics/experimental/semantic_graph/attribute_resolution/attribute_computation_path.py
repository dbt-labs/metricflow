from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import ClassVar

from typing_extensions import override

from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeComputationUpdate,
    MutableAttributeComputation,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import MutableGraphPath
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@dataclass
class AttributeComputationPath(MutableGraphPath[SemanticGraphNode, SemanticGraphEdge]):
    attribute_computation: MutableAttributeComputation

    _verbose_debug_logs: ClassVar[bool] = False

    @staticmethod
    def create() -> AttributeComputationPath:
        return AttributeComputationPath(
            _nodes=[],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=MutableOrderedSet(),
            _node_set_addition_order=[],
            attribute_computation=MutableAttributeComputation(),
        )

    def _append_update(self, update: AttributeComputationUpdate) -> None:
        self.attribute_computation.append_update(update)

    @override
    def _node_addition_callback(self, node: SemanticGraphNode) -> None:
        self._append_update(node.attribute_computation_update)

    def _edge_addition_callback(self, edge: SemanticGraphEdge) -> None:
        self._append_update(edge.attribute_computation_update)

    @override
    def append_edge(self, edge: SemanticGraphEdge, weight: int) -> None:
        super().append_edge(edge, weight)
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Appended edge to computation path",
                    edge=edge,
                    previous_attribute_descriptor=self.attribute_computation.previous_attribute_descriptor,
                    current_attribute_descriptor=self.attribute_computation.attribute_descriptor,
                )
            )

    @override
    def pop(self) -> None:
        if len(self._nodes) == 1 and len(self._edges) == 0:
            self.attribute_computation.pop_update()

        if len(self._edges) != 0:
            self.attribute_computation.pop_update()
            self.attribute_computation.pop_update()
        super().pop()
