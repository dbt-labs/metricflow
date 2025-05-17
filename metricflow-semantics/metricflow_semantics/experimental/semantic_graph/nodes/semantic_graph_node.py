from __future__ import annotations

import logging
import typing
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Generic, Optional

from metricflow_semantics.experimental.mf_graph.displayable_graph_element import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphEdge, MetricflowGraphNode
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import DunderNameElementLabel
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.visitor import Visitable, VisitorOutputT

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import AttributeNode
    from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import DsiEntityNode

logger = logging.getLogger(__name__)


class SemanticGraphNode(MetricflowGraphNode, Visitable, ABC):
    @abstractmethod
    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        raise NotImplementedError

    @property
    def dunder_name_element_label(self) -> Optional[DunderNameElementLabel]:
        return None

    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        if self.dunder_name_element_label is not None:
            return FrozenOrderedSet((self.dunder_name_element_label,))
        return FrozenOrderedSet()


class SemanticGraphNodeVisitor(Generic[VisitorOutputT]):
    @abstractmethod
    def visit_entity_node(self, node: DsiEntityNode) -> VisitorOutputT:
        raise NotImplementedError()

    @abstractmethod
    def visit_attribute_node(self, node: AttributeNode) -> VisitorOutputT:
        raise NotImplementedError()


@singleton_dataclass(order=False)
class SemanticGraphEdge(MetricFlowPrettyFormattable, MetricflowGraphEdge[SemanticGraphNode], ABC):
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        formatter = format_context.formatter
        return formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "tail_node": self._tail_node,
                "head_node": self._head_node,
            },
        )
