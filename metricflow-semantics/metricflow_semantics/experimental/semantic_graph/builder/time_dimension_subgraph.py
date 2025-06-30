from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import cached_property
from typing import Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import DatePart, DimensionType, TimeGranularity
from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_non_none_or_raise
from metricflow_semantics.experimental.metricflow_exception import InvalidManifestException
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeComputationUpdate
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
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.model_object_lookup import (
    SemanticModelObjectLookup,
)
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    AttributeNode,
    DsiEntityKeyAttributeNode,
    TimeAttributeNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    JoinFromModelNode,
    JoinToModelNode,
    TimeDimensionNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import SemanticGraphEdge
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

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
        join_from_semantic_model_node = JoinFromModelNode(model_id=model_id)

        for dimension in lookup.semantic_model.dimensions:
            # Skip non-time dimensions.
            if dimension.type is DimensionType.TIME:
                pass
            elif dimension.type is DimensionType.CATEGORICAL:
                continue
            else:
                assert_values_exhausted(dimension.type)

            type_params = mf_first_non_none_or_raise(
                dimension.type_params,
                error_supplier=lambda: InvalidManifestException(
                    LazyFormat(
                        "`type_params` should not be `None` for a time dimension.",
                        dimension=dimension,
                        semantic_model=lookup.semantic_model,
                    )
                ),
            )
            time_grain = type_params.time_granularity

            # There is a separate time dimension node to allow for graph intersection to find common attributes.
            for queryable_time_grain in self._time_grain_to_queryable_time_grains[time_grain]:
                time_dimension_node = TimeDimensionNode(
                    dimension_name=dimension.name,
                    time_grain_name=queryable_time_grain.value,
                )
                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=join_to_semantic_model_node,
                        relationship=EntityRelationship.VALID,
                        head_node=time_dimension_node,
                        linkable_element_properties=FrozenOrderedSet()
                        if queryable_time_grain is time_grain
                        else FrozenOrderedSet((LinkableElementProperty.DERIVED_TIME_GRANULARITY,)),
                    )
                )
                # current_subgraph.add_edge(
                #     EntityAttributeEdge.get_instance(
                #         tail_node=join_from_semantic_model_node,
                #         head_node=time_dimension_node,
                #         attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                #     )
                # )
                # Add attribute edges.
                current_subgraph.add_edges(self._generate_attribute_edges(time_dimension_node, queryable_time_grain))

        for attribute_node in self._get_attribute_nodes_for_entities(lookup):
            current_subgraph.add_edge(
                EntityAttributeEdge.get_instance(
                    tail_node=join_to_semantic_model_node,
                    head_node=attribute_node,
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                )
            )

        return current_subgraph

    def _generate_attribute_edges(
        self, time_dimension_node: TimeDimensionNode, node_time_grain: TimeGranularity
    ) -> Sequence[SemanticGraphEdge]:
        edges_to_add = []
        # Add attribute edges from the time dimension node to the queryable time grains.
        # e.g. TimeDimensionNode(`day`) should have edges to {`day`, `month`, `quarter`, `year`}

        # TODO: This seems like it could be consolidated with custom grains.
        # for time_grain in self._time_grain_to_queryable_time_grains[node_time_grain]:
        #     attribute_node = TimeAttributeNode.get_instance_for_time_grain(time_grain)
        #
        #     attribute_computation_update = AttributeComputationUpdate(
        #         linkable_element_property_additions=(LinkableElementProperty.DERIVED_TIME_GRANULARITY,)
        #         if time_grain != node_time_grain
        #         else (),
        #         time_grain_addition=ExpandedTimeGranularity(name=time_grain.value, base_granularity=time_grain),
        #     )
        #
        #     edges_to_add.append(
        #         EntityAttributeEdge.get_instance(
        #             tail_node=time_dimension_node,
        #             head_node=attribute_node,
        #             attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
        #             attribute_computation_update=attribute_computation_update,
        #         )
        #     )

        attribute_computation_update = AttributeComputationUpdate(
            time_grain_addition=ExpandedTimeGranularity(name=node_time_grain.value, base_granularity=node_time_grain),
        )

        attribute_node = TimeAttributeNode.get_instance_for_time_grain(node_time_grain)

        edges_to_add.append(
            EntityAttributeEdge.get_instance(
                tail_node=time_dimension_node,
                head_node=attribute_node,
                attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                attribute_computation_update=attribute_computation_update,
            )
        )

        # Add similar edges for the date part.
        # e.g. `day` should have edges to {`day`, `dow`, `doy`, `month`, `quarter`, `year`}
        for queryable_date_part in self._time_grain_to_applicable_date_parts[node_time_grain]:
            attribute_node = TimeAttributeNode.get_instance_for_date_part(queryable_date_part)
            edges_to_add.append(
                EntityAttributeEdge.get_instance(
                    tail_node=time_dimension_node,
                    head_node=attribute_node,
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                    attribute_computation_update=AttributeComputationUpdate(
                        linkable_element_property_additions=(LinkableElementProperty.DATE_PART,),
                        date_part_addition=queryable_date_part,
                    ),
                )
            )
        # Add similar edges for expanded time grain.
        for expanded_time_grain in self._manifest_object_lookup.expanded_time_grains:
            if expanded_time_grain.base_granularity.to_int() >= node_time_grain.to_int():
                attribute_node = TimeAttributeNode.get_instance_for_expanded_time_grain(expanded_time_grain)
                edges_to_add.append(
                    EntityAttributeEdge.get_instance(
                        tail_node=time_dimension_node,
                        head_node=attribute_node,
                        attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                        attribute_computation_update=AttributeComputationUpdate(
                            linkable_element_property_additions=(LinkableElementProperty.DERIVED_TIME_GRANULARITY,),
                            time_grain_addition=expanded_time_grain,
                        ),
                    )
                )
        return edges_to_add

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
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
