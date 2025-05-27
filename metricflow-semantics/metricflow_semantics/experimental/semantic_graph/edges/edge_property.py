from __future__ import annotations

import logging
from abc import ABC

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import EntityRelationship
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class SemanticGraphEdgeProperty(MetricflowGraphLabel, ABC):
    linkable_element_properties: FrozenOrderedSet[LinkableElementProperty]


@singleton_dataclass(order=False)
class RelationshipProperty(SemanticGraphEdgeProperty):
    entity_relationship: EntityRelationship
