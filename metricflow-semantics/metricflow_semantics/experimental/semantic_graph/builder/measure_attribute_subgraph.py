from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
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
    MeasureContainingModelObjectLookup,
)
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    MeasureNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    AggregationNode,
    JoinFromModelNode,
    JoinToModelNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.named_node import SemanticGraphNodeFactory
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


class MeasureAttributeSubgraphGenerator(TimeDimensionSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)

    def _generate_subgraph_for_measure_containing_model(
        self, lookup: MeasureContainingModelObjectLookup
    ) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        model_id = SemanticModelId(model_name=lookup.semantic_model.name)

        # primary_entity_field_defined = lookup.semantic_model.primary_entity is not None

        join_to_semantic_model_node = JoinToModelNode(model_id=model_id)
        join_from_semantic_model_node = JoinFromModelNode(model_id=model_id)

        for aggregation_configuration, measures in lookup.aggregation_configuration_to_measures.items():
            aggregation_entity_node = AggregationNode(
                model_id=model_id,
                aggregation_time_dimension_name=aggregation_configuration.time_dimension_name,
            )

            # Add an edge from the aggregation entity to the metric time nodes.
            for queryable_time_grain in self._time_grain_to_queryable_time_grains[aggregation_configuration.time_grain]:
                metric_time_node = SemanticGraphNodeFactory.get_metric_time_base_node(
                    base_grain=queryable_time_grain,
                )

                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=aggregation_entity_node,
                        relationship=EntityRelationship.LEFT_CARDINALITY_ONE,
                        head_node=metric_time_node,
                    )
                )

                # Add edges to the time attributes (e.g. `day`, `year`).
                current_subgraph.add_edges(self._generate_attribute_edges(metric_time_node, queryable_time_grain))

                # Add an edge from the aggregation entity node to the join-from node.
                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=aggregation_entity_node,
                        relationship=EntityRelationship.LEFT_CARDINALITY_ONE,
                        head_node=join_from_semantic_model_node,
                    )
                )
                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=aggregation_entity_node,
                        relationship=EntityRelationship.LEFT_CARDINALITY_ONE,
                        head_node=join_to_semantic_model_node,
                    )
                )

                # In the case that the primary entity is not an element in the semantic model, the primary
                # entity node is only reachable from measure attribute nodes in the semantic model.
                # To model this, add an edge from the primary entity to the model entity and the reverse.
                # if primary_entity_field_defined:
                #     current_subgraph.add_edge(
                #         EntityRelationshipEdge.get_instance(
                #             tail_node=aggregation_entity_node,
                #             relationship=EntityRelationship.LEFT_CARDINALITY_ONE,
                #             head_node=primary_entity_node,
                #             weight=0,
                #         )
                #     )
                #     current_subgraph.add_edge(
                #         EntityRelationshipEdge.get_instance(
                #             tail_node=primary_entity_node,
                #             relationship=EntityRelationship.LEFT_CARDINALITY_ONE,
                #             head_node=join_to_semantic_model_node,
                #             weight=1,
                #         )
                #     )
                # else:
                #     current_subgraph.add_edge(
                #         EntityRelationshipEdge.get_instance(
                #             tail_node=aggregation_entity_node,
                #             relationship=EntityRelationship.LEFT_CARDINALITY_ONE,
                #             head_node=join_to_semantic_model_node,
                #             weight=0,
                #         )
                #     )

            # Add edges from the measure attribute nodes to the aggregation nodes.
            for measure in measures:
                measure_name = measure.name

                measure_attribute_node = MeasureNode.get_instance(
                    measure_name=measure_name,
                    model_id=model_id,
                )

                current_subgraph.add_edge(
                    EntityAttributeEdge.get_instance(
                        tail_node=measure_attribute_node,
                        head_node=aggregation_entity_node,
                        attribute_edge_type=AttributeEdgeType.ATTRIBUTE_TO_ENTITY,
                    )
                )

        return current_subgraph

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        for lookup in self._manifest_object_lookup.measure_containing_model_lookups:
            current_subgraph.update(self._generate_subgraph_for_measure_containing_model(lookup))

        return current_subgraph
