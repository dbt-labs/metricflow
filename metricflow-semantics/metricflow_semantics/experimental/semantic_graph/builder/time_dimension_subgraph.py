from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import cached_property

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dsi.model_object_lookup import (
    ModelObjectLookup,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe_update import (
    QueryRecipeStep,
)
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.sg_edges import EntityRelationshipEdge
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_nodes import (
    AttributeNode,
    KeyAttributeNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_nodes import (
    JoinedModelNode,
    TimeDimensionNode,
    TimeEntityNode,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


class TimeDimensionSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)
        self._time_entity_node = TimeEntityNode.get_instance()

    def _get_attribute_nodes_for_entities(self, lookup: ModelObjectLookup) -> list[AttributeNode]:
        return [
            KeyAttributeNode(
                attribute_name=entity.name,
            )
            for entity in lookup.semantic_model.entities
        ]

    def _get_subgraph_for_model(self, lookup: ModelObjectLookup) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()

        model_id = SemanticModelId(model_name=lookup.semantic_model.name)
        semantic_model_node = JoinedModelNode.get_instance(model_id)

        for time_dimension_name, time_grain in lookup.time_dimension_name_to_grain.items():
            time_dimension_node = TimeDimensionNode.get_instance(time_dimension_name)
            current_subgraph.add_edge(
                EntityRelationshipEdge.get_instance(
                    tail_node=semantic_model_node,
                    head_node=time_dimension_node,
                    recipe_update=QueryRecipeStep(
                        add_min_time_grain=time_grain,
                    ),
                )
            )
            current_subgraph.add_edge(
                EntityRelationshipEdge.get_instance(
                    tail_node=time_dimension_node,
                    head_node=self._time_entity_node,
                )
            )

        return current_subgraph

    @override
    def generate_subgraph(self, predecessor_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        for lookup in self._manifest_object_lookup.model_object_lookups:
            current_subgraph.update(self._get_subgraph_for_model(lookup))

        return current_subgraph

    @cached_property
    def _time_grain_to_queryable_time_grains(self) -> Mapping[TimeGranularity, AnyLengthTuple[TimeGranularity]]:
        return {
            min_referenced_time_grain: tuple(
                time_grain
                for time_grain in TimeGranularity
                if time_grain.to_int() >= min_referenced_time_grain.to_int()
            )
            for min_referenced_time_grain in TimeGranularity
        }

    @cached_property
    def _time_grain_to_applicable_date_parts(self) -> Mapping[TimeGranularity, AnyLengthTuple[DatePart]]:
        return {
            time_grain: tuple(date_part for date_part in DatePart if date_part.to_int() >= time_grain.to_int())
            for time_grain in TimeGranularity
        }
