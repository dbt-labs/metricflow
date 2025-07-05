from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_attribute import (
    AttributeEdgeType,
    EntityAttributeEdge,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import EntityRelationshipEdge
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    TimeAttributeNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    MetricTimeNode,
    TimeEntityNode,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


class TimeEntitySubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)

    def _generate_time_entity_subgraph(self, min_time_grain: TimeGranularity) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        time_entity_node = TimeEntityNode.get_instance()
        metric_time_node = MetricTimeNode.get_instance()
        for time_grain in self._time_grain_to_queryable_time_grains[min_time_grain]:
            # aggregation_entity_node = TimeAggregationNode.get_instance(
            #     min_time_grain=time_grain,
            # )
            # current_subgraph.add_edge(
            #     EntityRelationshipEdge.get_instance(
            #         tail_node=aggregation_entity_node,
            #         head_node=metric_time_node,
            #     )
            # )

            current_subgraph.add_edge(
                EntityAttributeEdge.get_instance(
                    tail_node=time_entity_node,
                    head_node=TimeAttributeNode.get_instance_for_time_grain(time_grain),
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                    attribute_computation_update=AttributeRecipeUpdate(
                        set_time_grain=ExpandedTimeGranularity(name=time_grain.value, base_granularity=time_grain),
                    ),
                )
            )

        for queryable_date_part in self._time_grain_to_applicable_date_parts[min_time_grain]:
            attribute_node = TimeAttributeNode.get_instance_for_date_part(queryable_date_part)
            current_subgraph.add_edge(
                EntityAttributeEdge.get_instance(
                    tail_node=time_entity_node,
                    head_node=attribute_node,
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                    attribute_computation_update=AttributeRecipeUpdate(
                        add_properties=(LinkableElementProperty.DATE_PART,),
                        set_date_part=queryable_date_part,
                    ),
                )
            )

        for expanded_time_grain in self._manifest_object_lookup.expanded_time_grains:
            if expanded_time_grain.base_granularity.to_int() >= min_time_grain.to_int():
                attribute_node = TimeAttributeNode.get_instance_for_expanded_time_grain(expanded_time_grain)
                current_subgraph.add_edge(
                    EntityAttributeEdge.get_instance(
                        tail_node=time_entity_node,
                        head_node=attribute_node,
                        attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                        attribute_computation_update=AttributeRecipeUpdate(
                            add_properties=(LinkableElementProperty.DERIVED_TIME_GRANULARITY,),
                            set_time_grain=expanded_time_grain,
                        ),
                    )
                )

        return current_subgraph

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        current_subgraph.add_edge(
            EntityRelationshipEdge.get_instance(
                tail_node=MetricTimeNode.get_instance(),
                head_node=TimeEntityNode.get_instance(),
            )
        )

        min_time_grain: Optional[TimeGranularity] = None

        for lookup in self._manifest_object_lookup.model_object_lookups:
            for time_grain in lookup.time_dimension_name_to_grain.values():
                if min_time_grain is None:
                    min_time_grain = time_grain
                elif time_grain.to_int() < min_time_grain.to_int():
                    min_time_grain = time_grain

        if min_time_grain is not None:
            current_subgraph.update(self._generate_time_entity_subgraph(min_time_grain))

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
