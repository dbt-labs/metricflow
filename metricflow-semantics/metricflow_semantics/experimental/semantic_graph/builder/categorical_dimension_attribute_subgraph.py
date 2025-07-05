from __future__ import annotations

import logging

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import DimensionType
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
    CategoricalDimensionAttributeNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    SemanticModelNode,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


class CategoricalDimensionAttributeSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)

    def _get_attribute_nodes_for_categorical_dimensions(self, lookup: SemanticModelObjectLookup) -> list[AttributeNode]:
        attribute_nodes: list[AttributeNode] = []

        for dimension in lookup.semantic_model.dimensions:
            if dimension.type is DimensionType.CATEGORICAL:
                attribute_nodes.append(
                    CategoricalDimensionAttributeNode(
                        attribute_name=dimension.name,
                    )
                )
            elif dimension.type is DimensionType.TIME:
                pass
            else:
                assert_values_exhausted(dimension.type)

        return attribute_nodes

    def _get_subgraph_for_model(self, lookup: SemanticModelObjectLookup) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        model_id = SemanticModelId(model_name=lookup.semantic_model.name)
        semantic_model_node = SemanticModelNode.get_instance(model_id)

        for attribute_node in self._get_attribute_nodes_for_categorical_dimensions(lookup):
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
