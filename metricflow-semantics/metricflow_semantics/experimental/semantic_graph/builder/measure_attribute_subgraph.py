from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import (
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
    JoinedModelNode,
    LocalModelNode,
    MetricTimeNode,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


class MeasureAttributeSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)

    def _generate_subgraph_for_measure_containing_model(
        self, lookup: MeasureContainingModelObjectLookup
    ) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        model_id = SemanticModelId(model_name=lookup.semantic_model.name)
        semantic_model_node = JoinedModelNode.get_instance(model_id)
        local_semantic_model_node = LocalModelNode.get_instance(model_id)

        metric_time_node = MetricTimeNode.get_instance()
        for aggregation_configuration, measures in lookup.aggregation_configuration_to_measures.items():
            # aggregation_entity_node = TimeAggregationNode.get_instance(
            #     # model_id=model_id,
            #     # aggregation_time_dimension_name=aggregation_configuration.time_dimension_name,
            #     min_time_grain=aggregation_configuration.time_grain,
            # )
            # current_subgraph.add_edge(
            #     EntityRelationshipEdge.get_instance(
            #         tail_node=aggregation_entity_node,
            #         head_node=metric_time_node,
            #
            #     )
            # )

            # Add edges from the measure attribute nodes to the aggregation nodes.
            for measure in measures:
                measure_name = measure.name

                measure_node = MeasureNode.get_instance(
                    measure_name=measure_name,
                    model_id=model_id,
                )

                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=measure_node,
                        head_node=metric_time_node,
                        attribute_computation_update=AttributeRecipeUpdate(
                            add_min_time_grain=aggregation_configuration.time_grain,
                        ),
                    )
                )

                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=measure_node,
                        head_node=local_semantic_model_node,
                    )
                )

        return current_subgraph

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        for lookup in self._manifest_object_lookup.measure_containing_model_lookups:
            current_subgraph.update(self._generate_subgraph_for_measure_containing_model(lookup))

        return current_subgraph
