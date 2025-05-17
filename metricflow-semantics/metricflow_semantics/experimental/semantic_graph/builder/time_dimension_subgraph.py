from __future__ import annotations

import logging

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import DimensionType
from typing_extensions import override

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_require_non_none
from metricflow_semantics.experimental.metricflow_exception import InvalidManifestException
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_attribute import (
    AttributeEdgeType,
    EntityAttributeEdge,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import (
    EntityRelationship,
    EntityRelationshipEdge,
)
from metricflow_semantics.experimental.semantic_graph.model_object_lookup import (
    SemanticModelObjectLookup,
)
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    AttributeNode,
    DsiEntityKeyAttributeNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    JoinToModelNode,
    SemanticModelId,
    TimeDimensionNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.named_node import SemanticGraphNodeFactory
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class TimeDimensionSubgraphGenerator(SemanticSubgraphGenerator):
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
        join_to_semantic_model_node = JoinToModelNode(model_id=model_id)

        for dimension in lookup.semantic_model.dimensions:
            if dimension.type is DimensionType.TIME:
                type_params = mf_require_non_none(
                    dimension.type_params,
                    exception=lambda: InvalidManifestException(
                        LazyFormat(
                            "`type_params` should not be `None` for a time dimension.",
                            dimension=dimension,
                            semantic_model=lookup.semantic_model,
                        )
                    ),
                )
                time_grain = type_params.time_granularity
                time_dimension_node = TimeDimensionNode(
                    dimension_name=dimension.name,
                    time_grain_name=time_grain.value,
                )
                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=time_dimension_node,
                        relationship=EntityRelationship.RIGHT_CARDINALITY_ONE,
                        head_node=SemanticGraphNodeFactory.get_time_base_node(time_grain),
                        weight=0,
                    )
                )
                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=join_to_semantic_model_node,
                        relationship=EntityRelationship.RIGHT_CARDINALITY_ONE,
                        head_node=time_dimension_node,
                        weight=1,
                    )
                )
            elif dimension.type is DimensionType.CATEGORICAL:
                pass
            else:
                assert_values_exhausted(dimension.type)

        for attribute_node in self._get_attribute_nodes_for_entities(lookup):
            current_subgraph.add_edge(
                EntityAttributeEdge.get_instance(
                    tail_node=join_to_semantic_model_node,
                    head_node=attribute_node,
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                )
            )

        return current_subgraph

    @override
    def generate_subgraph(self) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        for lookup in self._manifest_object_lookup.model_object_lookups:
            current_subgraph.update(self._get_subgraph_for_model(lookup))

        return current_subgraph
