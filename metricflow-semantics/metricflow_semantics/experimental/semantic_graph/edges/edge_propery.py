from __future__ import annotations

import logging
from abc import ABC

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.displayable_graph_element import MetricflowGraphProperty
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import EntityRelationship
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class SemanticGraphEdgeProperty(MetricflowGraphProperty, ABC):
    linkable_element_properties: FrozenOrderedSet[LinkableElementProperty]


@fast_frozen_dataclass(order=False)
class RelationshipProperty(SemanticGraphEdgeProperty, Singleton):
    entity_relationship: EntityRelationship

    @classmethod
    def get_instance(
        cls, entity_relationship: EntityRelationship, linkable_element_properties: OrderedSet[LinkableElementProperty]
    ) -> RelationshipProperty:
        return cls._get_singleton_by_kwargs(
            entity_relationship=entity_relationship, linkable_element_properties=linkable_element_properties.as_frozen()
        )
