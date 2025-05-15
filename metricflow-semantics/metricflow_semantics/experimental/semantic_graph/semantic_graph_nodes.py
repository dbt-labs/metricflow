from __future__ import annotations

from abc import ABC
from functools import cached_property

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphNode
from metricflow_semantics.experimental.ordered_set import OrderedSet
from metricflow_semantics.experimental.semantic_graph.entity_id import EntityId
from metricflow_semantics.experimental.semantic_graph.node_properties import SemanticGraphProperty
from metricflow_semantics.experimental.singleton import Singleton


class SemanticGraphNode(MetricflowGraphNode, ABC):
    pass


@fast_frozen_dataclass(order=False)
class EntityNode(SemanticGraphNode, Singleton):
    entity_id: EntityId

    @property
    def dot_label(self) -> str:
        return self.entity_id.dot_label

    @property
    def graphviz_label(self) -> str:
        return self.dot_label

    @property
    def comparison_key(self) -> tuple[EntityId]:
        return (self.entity_id,)


@fast_frozen_dataclass(order=False)
class AttributeNode(SemanticGraphNode, Singleton):
    attribute_name: str
    attribute_properties: OrderedSet[SemanticGraphProperty]

    @cached_property
    def dot_label(self) -> str:
        return self.attribute_name

    @cached_property
    def graphviz_label(self) -> str:
        return self.attribute_name

    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self.attribute_name,)
