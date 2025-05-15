from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic

from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphNode
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import AttributeNode
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import EntityNode
from metricflow_semantics.visitor import Visitable, VisitorOutputT


class SemanticGraphNode(MetricflowGraphNode, Visitable, ABC):
    @abstractmethod
    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        raise NotImplementedError


class SemanticGraphNodeVisitor(Generic[VisitorOutputT]):
    @abstractmethod
    def visit_entity_node(self, node: EntityNode) -> VisitorOutputT:
        raise NotImplementedError()

    @abstractmethod
    def visit_attribute_node(self, node: AttributeNode) -> VisitorOutputT:
        raise NotImplementedError()
