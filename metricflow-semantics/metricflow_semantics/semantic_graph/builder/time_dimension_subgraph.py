from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import cached_property

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.edges.sg_edges import EntityRelationshipEdge
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.lookups.model_object_lookup import (
    ModelObjectLookup,
)
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.semantic_graph.nodes.entity_nodes import (
    JoinedModelNode,
    MetricTimeNode,
    TimeDimensionNode,
    TimeNode,
)
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
)
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


class TimeDimensionSubgraphGenerator(SemanticSubgraphGenerator):
    """Generator to build edges for time-dimension entities.

    A time dimension in a semantic model maps to a time-dimension entity node. All time-dimension entity nodes relate
    to the time-entity node.
    """

    @override
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        super().__init__(manifest_object_lookup)
        self._time_entity_node = TimeNode.get_instance()

    @override
    def add_edges_for_manifest(self, edge_list: list[SemanticGraphEdge]) -> None:
        for lookup in self._manifest_object_lookup.model_object_lookups:
            self._add_edges_for_model(lookup, edge_list)

        edge_list.append(
            EntityRelationshipEdge.create(tail_node=MetricTimeNode.get_instance(), head_node=self._time_entity_node)
        )

    def _add_edges_for_model(self, lookup: ModelObjectLookup, edge_list: list[SemanticGraphEdge]) -> None:
        model_id = SemanticModelId.get_instance(model_name=lookup.semantic_model.name)
        semantic_model_node = JoinedModelNode.get_instance(model_id)

        for time_dimension_name, time_grain in lookup.time_dimension_name_to_grain.items():
            time_dimension_node = TimeDimensionNode.get_instance(time_dimension_name)
            edge_list.append(
                EntityRelationshipEdge.create(
                    tail_node=semantic_model_node,
                    head_node=time_dimension_node,
                    recipe_update=AttributeRecipeStep(
                        set_source_time_grain=time_grain,
                    ),
                )
            )
            edge_list.append(
                EntityRelationshipEdge.create(
                    tail_node=time_dimension_node,
                    head_node=self._time_entity_node,
                )
            )

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
