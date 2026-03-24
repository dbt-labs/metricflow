from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.edges.sg_edges import EntityAttributeEdge
from metricflow_semantics.semantic_graph.lookups.model_object_lookup import (
    ModelObjectLookup,
)
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.semantic_graph.nodes.attribute_nodes import (
    KeyAttributeNode,
)
from metricflow_semantics.semantic_graph.nodes.entity_nodes import (
    JoinedModelNode,
    LocalModelNode,
)
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
)

logger = logging.getLogger(__name__)


class EntityKeySubgraphGenerator(SemanticSubgraphGenerator):
    """Generator that adds edges for entity-key attributes.

    Each entity defined in a semantic model maps to an entity-key attribute node as the name of the entity can be used
    to query the respective values.
    """

    @override
    def add_edges_for_manifest(self, edge_list: list[SemanticGraphEdge]) -> None:
        for lookup in self._manifest_object_lookup.model_object_lookups:
            self._add_edges_for_model(lookup, edge_list)

    def _add_edges_for_model(self, lookup: ModelObjectLookup, edge_list: list[SemanticGraphEdge]) -> None:
        model_id = SemanticModelId.get_instance(model_name=lookup.semantic_model.name)
        semantic_model_node = JoinedModelNode.get_instance(model_id)
        local_semantic_model_node = LocalModelNode.get_instance(model_id)

        key_attribute_nodes = [KeyAttributeNode.get_instance(entity.name) for entity in lookup.semantic_model.entities]
        edge_list.extend(
            [
                EntityAttributeEdge.create(
                    tail_node=semantic_model_node,
                    head_node=key_attribute_node,
                )
                for key_attribute_node in key_attribute_nodes
            ]
        )
        edge_list.extend(
            [
                EntityAttributeEdge.create(
                    tail_node=local_semantic_model_node,
                    head_node=key_attribute_node,
                )
                for key_attribute_node in key_attribute_nodes
            ]
        )
