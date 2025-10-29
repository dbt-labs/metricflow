from __future__ import annotations

import logging

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import DimensionType
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
    AttributeNode,
    CategoricalDimensionAttributeNode,
)
from metricflow_semantics.semantic_graph.nodes.entity_nodes import (
    JoinedModelNode,
)
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
)

logger = logging.getLogger(__name__)


class CategoricalDimensionSubgraphGenerator(SemanticSubgraphGenerator):
    """Generator that adds edges for categorical dimensions.

    This generator add edges from the joined-model nodes to the relevant categorical-dimension nodes.
    """

    @override
    def add_edges_for_manifest(self, edge_list: list[SemanticGraphEdge]) -> None:
        for lookup in self._manifest_object_lookup.model_object_lookups:
            self._add_edges_for_model(lookup, edge_list)

    def _get_nodes_for_categorical_dimensions(self, lookup: ModelObjectLookup) -> list[AttributeNode]:
        attribute_nodes: list[AttributeNode] = []

        for dimension in lookup.semantic_model.dimensions:
            if dimension.type is DimensionType.CATEGORICAL:
                attribute_nodes.append(CategoricalDimensionAttributeNode.get_instance(dimension.name))
            elif dimension.type is DimensionType.TIME:
                pass
            else:
                assert_values_exhausted(dimension.type)

        return attribute_nodes

    def _add_edges_for_model(self, lookup: ModelObjectLookup, edge_list: list[SemanticGraphEdge]) -> None:
        model_id = SemanticModelId.get_instance(model_name=lookup.semantic_model.name)
        semantic_model_node = JoinedModelNode.get_instance(model_id)
        edge_list.extend(
            [
                EntityAttributeEdge.create(
                    tail_node=semantic_model_node,
                    head_node=attribute_node,
                )
                for attribute_node in self._get_nodes_for_categorical_dimensions(lookup)
            ]
        )
