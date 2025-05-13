from __future__ import annotations

from functools import cached_property

from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphNode
from metricflow_semantics.experimental.semantic_graph.node_properties import SemanticGraphProperty
from metricflow_semantics.experimental.singleton import Singleton


@fast_frozen_dataclass(order=False)
class EntityNode(MetricflowGraphNode, Singleton):
    entity_name: str

    @cached_property
    def dot_label(self) -> str:
        return self.entity_name

    @cached_property
    def graphviz_label(self) -> str:
        return self.entity_name

    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self.entity_name,)


@fast_frozen_dataclass(order=False)
class AttributeNode(MetricflowGraphNode, Singleton):
    attribute_name: str
    attribute_properties: FrozenOrderedSet[SemanticGraphProperty]

    @cached_property
    def dot_label(self) -> str:
        return self.attribute_name

    @cached_property
    def graphviz_label(self) -> str:
        return self.attribute_name

    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self.attribute_name,)
