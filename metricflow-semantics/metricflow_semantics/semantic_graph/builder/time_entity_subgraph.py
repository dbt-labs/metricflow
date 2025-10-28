from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import cached_property

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.edges.sg_edges import (
    EntityAttributeEdge,
)
from metricflow_semantics.semantic_graph.nodes.attribute_nodes import (
    TimeAttributeNode,
)
from metricflow_semantics.semantic_graph.nodes.entity_nodes import (
    TimeNode,
)
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
)
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


class TimeEntitySubgraphGenerator(SemanticSubgraphGenerator):
    """Generates the time-entity subgraph.

    * The time-entity subgraph consists of the time-entity node with edges to various  attribute
      nodes. (e.g. `day`, `dow`, `custom_year`).
    * An alternative approach would be to use a virtual semantic-model and have time be a configured entity.
    * There is a bug in the current resolver that shows time gain of the defined time dimension as available to
      query even if it is smaller than the smallest grain defined in the time spine.
    * To replicate this behavior, this adds time attribute nodes based on the smallest time grain used in the semantic
      manifest.
    * This behavior is replicated to ensure snapshot consistency with the current resolver but should be addressed
      post-migration.
    """

    @override
    def add_edges_for_manifest(self, edge_list: list[SemanticGraphEdge]) -> None:
        min_time_grain_in_models = self._manifest_object_lookup.min_time_grain_used_in_models
        min_time_grains = [self._manifest_object_lookup.min_time_grain_in_time_spine]
        if min_time_grain_in_models is not None:
            min_time_grains.append(min_time_grain_in_models)
        self._add_edges_for_time_entity_subgraph(
            min_time_grain=min(min_time_grains, key=lambda time_grain: time_grain.to_int()),
            edge_list=edge_list,
        )

    def _add_edges_for_time_entity_subgraph(
        self, min_time_grain: TimeGranularity, edge_list: list[SemanticGraphEdge]
    ) -> None:
        time_entity_node = TimeNode.get_instance()

        # Add an edge from the time-entity node to all queryable grains using the manifest.
        for time_grain in self._time_grain_to_queryable_time_grains[min_time_grain]:
            edge_list.append(
                EntityAttributeEdge.create(
                    tail_node=time_entity_node,
                    head_node=TimeAttributeNode.get_instance_for_time_grain(time_grain),
                    recipe_step=AttributeRecipeStep(
                        set_time_grain_access=ExpandedTimeGranularity(
                            name=time_grain.value, base_granularity=time_grain
                        ),
                    ),
                )
            )
        # Add all applicable date parts.
        for queryable_date_part in self._time_grain_to_applicable_date_parts[min_time_grain]:
            attribute_node = TimeAttributeNode.get_instance_for_date_part(queryable_date_part)
            edge_list.append(
                EntityAttributeEdge.create(
                    tail_node=time_entity_node,
                    head_node=attribute_node,
                    recipe_step=AttributeRecipeStep(
                        add_properties=(GroupByItemProperty.DATE_PART,),
                        set_date_part_access=queryable_date_part,
                    ),
                )
            )

        # Add all applicable custom grains.
        for expanded_time_grain in self._manifest_object_lookup.expanded_time_grains:
            if expanded_time_grain.base_granularity.to_int() >= min_time_grain.to_int():
                attribute_node = TimeAttributeNode.get_instance_for_expanded_time_grain(expanded_time_grain)
                edge_list.append(
                    EntityAttributeEdge.create(
                        tail_node=time_entity_node,
                        head_node=attribute_node,
                        recipe_step=AttributeRecipeStep(
                            add_properties=(GroupByItemProperty.DERIVED_TIME_GRANULARITY,),
                            set_time_grain_access=expanded_time_grain,
                        ),
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
