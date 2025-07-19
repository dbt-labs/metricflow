from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.experimental.dsi.measure_model_object_lookup import MeasureContainingModelObjectLookup
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe_update import (
    QueryRecipeStep,
)
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.sg_edges import EntityRelationshipEdge
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.entity_nodes import (
    LocalModelNode,
    MeasureNode,
    MetricTimeNode,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


class MeasureSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)

    def _generate_subgraph_for_measure_containing_model(
        self, lookup: MeasureContainingModelObjectLookup
    ) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        model_id = SemanticModelId(model_name=lookup.semantic_model.name)
        local_semantic_model_node = LocalModelNode.get_instance(model_id)

        metric_time_node = MetricTimeNode.get_instance()
        for aggregation_configuration, measures in lookup.aggregation_configuration_to_measures.items():
            # Add edges from the measure nodes to the metric-time node.
            for measure in measures:
                measure_name = measure.name

                measure_node = MeasureNode.get_instance(
                    measure_name=measure_name,
                    source_model_id=model_id,
                )

                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=measure_node,
                        head_node=metric_time_node,
                        recipe_update=QueryRecipeStep(
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
    def generate_subgraph(self, predecessor_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        for lookup in self._manifest_object_lookup.measure_containing_model_lookups:
            current_subgraph.update(self._generate_subgraph_for_measure_containing_model(lookup))

        return current_subgraph
