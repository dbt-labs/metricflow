from __future__ import annotations

import logging
from functools import cached_property
from typing import Mapping

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_attribute import (
    AttributeEdgeType,
    EntityAttributeEdge,
)
from metricflow_semantics.experimental.semantic_graph.nodes.named_node import SemanticGraphNodeFactory
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph

logger = logging.getLogger(__name__)


class TimeSubgraphGenerator(SemanticSubgraphGenerator):
    @override
    def generate_subgraph(self) -> MutableSemanticGraph:
        min_time_grain_in_manifest = self._manifest_object_lookup.min_time_grain
        current_subgraph = MutableSemanticGraph.create()

        for time_grain in self._time_grain_to_queryable_time_grains[min_time_grain_in_manifest]:
            time_base_node = SemanticGraphNodeFactory.get_time_base_node(time_grain)
            metric_time_base_node = SemanticGraphNodeFactory.get_metric_time_base_node(time_grain)
            tail_nodes = (time_base_node, metric_time_base_node)
            # Add attribute edge from the base node to the queryable time grains.
            # e.g. `day` should have edges to {`day`, `month`, `quarter`, `year`}
            for queryable_time_grain in self._time_grain_to_queryable_time_grains[time_grain]:
                attribute_node = SemanticGraphNodeFactory.get_time_grain_attribute_node(queryable_time_grain)
                for tail_node in tail_nodes:
                    current_subgraph.add_edge(
                        EntityAttributeEdge.get_instance(
                            tail_node=tail_node,
                            head_node=attribute_node,
                            attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                            weight=0,
                        )
                    )
            # Add attribute edge from the base node to the queryable date parts.
            # e.g. `day` should have edges to {`day`, `dow`, `doy`, `month`, `quarter`, `year`}
            for queryable_date_part in self._time_grain_to_applicable_date_parts[time_grain]:
                attribute_node = SemanticGraphNodeFactory.get_date_part_attribute_node(queryable_date_part)
                for tail_node in tail_nodes:
                    current_subgraph.add_edge(
                        EntityAttributeEdge.get_instance(
                            tail_node=tail_node,
                            head_node=attribute_node,
                            attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                            weight=0,
                        )
                    )

        for expanded_time_grain in self._manifest_object_lookup.expanded_time_grains:
            time_base_node = SemanticGraphNodeFactory.get_time_base_node(expanded_time_grain.base_granularity)
            metric_time_base_node = SemanticGraphNodeFactory.get_metric_time_base_node(
                expanded_time_grain.base_granularity
            )

            attribute_node = SemanticGraphNodeFactory.get_attribute_node_for_expanded_time_grain(expanded_time_grain)
            for tail_node in (time_base_node, metric_time_base_node):
                current_subgraph.add_edge(
                    EntityAttributeEdge.get_instance(
                        tail_node=tail_node,
                        head_node=attribute_node,
                        attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                        weight=0,
                    )
                )

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
