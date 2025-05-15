from __future__ import annotations

import logging

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.edges.semantic_graph_edge import SemanticGraphEdge
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import SemanticGraphNode
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty

logger = logging.getLogger(__name__)


class EntityRelationship(OrderedEnum):
    ONE_TO_ONE = "one_to_one"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


@fast_frozen_dataclass(order=False)
class EntityRelationshipEdge(SemanticGraphEdge, Singleton):
    relationship: EntityRelationship
    linkable_element_properties: FrozenOrderedSet[LinkableElementProperty]

    @classmethod
    def get_instance(
        cls,
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        relationship: EntityRelationship,
        linkable_element_properties: OrderedSet[LinkableElementProperty],
    ) -> EntityRelationshipEdge:
        return cls._get_singleton_by_kwargs(
            tail_node=tail_node,
            head_node=head_node,
            relationship=relationship,
            linkable_element_properties=linkable_element_properties.as_frozen(),
        )
