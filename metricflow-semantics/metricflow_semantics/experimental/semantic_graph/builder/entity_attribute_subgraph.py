from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_attribute import (
    AttributeEdgeType,
    EntityAttributeEdge,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.model_object_lookup import (
    SemanticModelObjectLookup,
)
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    AttributeNode,
    DsiEntityKeyAttributeNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    SemanticModelNode,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


class EntityAttributeSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)

    def _get_attribute_nodes_for_entities(self, lookup: SemanticModelObjectLookup) -> list[AttributeNode]:
        return [
            DsiEntityKeyAttributeNode(
                attribute_name=entity.name,
            )
            for entity in lookup.semantic_model.entities
        ]

    def _get_subgraph_for_model(self, lookup: SemanticModelObjectLookup) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()

        model_id = SemanticModelId(model_name=lookup.semantic_model.name)
        semantic_model_node = SemanticModelNode.get_instance(model_id)

        for attribute_node in self._get_attribute_nodes_for_entities(lookup):
            current_subgraph.add_edge(
                EntityAttributeEdge.get_instance(
                    tail_node=semantic_model_node,
                    head_node=attribute_node,
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                )
            )

        return current_subgraph

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        for lookup in self._manifest_object_lookup.model_object_lookups:
            current_subgraph.update(self._get_subgraph_for_model(lookup))

        return current_subgraph
