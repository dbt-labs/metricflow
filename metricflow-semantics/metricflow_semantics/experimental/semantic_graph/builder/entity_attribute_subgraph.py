from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.experimental.dsi.model_object_lookup import (
    SemanticModelObjectLookup,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe_update import (
    QueryRecipeStep,
)
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_attribute import (
    AttributeEdgeType,
    EntityAttributeEdge,
)
from metricflow_semantics.experimental.semantic_graph.edges.sg_edges import EntityRelationshipEdge
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    KeyAttributeNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    JoinedModelNode,
    KeyEntityNode,
    LocalModelNode,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


class EntityAttributeSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)

    # def _get_attribute_nodes_for_entities(self, lookup: SemanticModelObjectLookup) -> list[AttributeNode]:
    #     return [
    #         DsiEntityKeyEntityNode(
    #             attribute_name=entity.name,
    #         )
    #         for entity in lookup.semantic_model.entities
    #     ]

    def _get_subgraph_for_model(self, lookup: SemanticModelObjectLookup) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()

        model_id = SemanticModelId(model_name=lookup.semantic_model.name)
        semantic_model_node = JoinedModelNode.get_instance(model_id)
        local_semantic_model_node = LocalModelNode.get_instance(model_id)
        recipe_update = QueryRecipeStep(join_model=model_id)

        for entity in lookup.semantic_model.entities:
            key_entity_node = KeyEntityNode.get_instance(entity.name)
            key_attribute_node = KeyAttributeNode.get_instance(entity.name)

            current_subgraph.add_edge(
                EntityRelationshipEdge.get_instance(
                    tail_node=key_entity_node,
                    head_node=key_attribute_node,
                )
            )

            current_subgraph.add_edge(
                EntityAttributeEdge.get_instance(
                    tail_node=semantic_model_node,
                    head_node=key_entity_node,
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                )
            )
            current_subgraph.add_edge(
                EntityAttributeEdge.get_instance(
                    tail_node=local_semantic_model_node,
                    head_node=key_entity_node,
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                )
            )

        return current_subgraph

    @override
    def generate_subgraph(self, predecessor_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        for lookup in self._manifest_object_lookup.model_object_lookups:
            current_subgraph.update(self._get_subgraph_for_model(lookup))

        return current_subgraph
