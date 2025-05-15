from __future__ import annotations

from functools import cached_property

from typing_extensions import override

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.ordered_set import OrderedSet
from metricflow_semantics.experimental.semantic_graph.nodes.node_properties import SemanticGraphProperty
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
    SemanticGraphNodeVisitor,
)
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.visitor import VisitorOutputT


@fast_frozen_dataclass(order=False)
class AttributeNode(SemanticGraphNode, Singleton):
    attribute_name: str
    attribute_properties: OrderedSet[SemanticGraphProperty]

    @override
    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_attribute_node(self)

    @property
    @override
    def dot_label(self) -> str:
        return self.attribute_name

    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self.attribute_name,)
