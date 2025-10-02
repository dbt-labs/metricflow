from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.experimental.dsi.measure_model_object_lookup import SimpleMetricModelObjectLookup
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.edges.sg_edges import EntityRelationshipEdge
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.entity_nodes import (
    LocalModelNode,
    MeasureNode,
    MetricTimeNode,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
)

logger = logging.getLogger(__name__)


class MeasureSubgraphGenerator(SemanticSubgraphGenerator):
    """Generate the subgraph that relates measures to other entities.

    For each measure, this adds the following edges:

    * MeasureNode -> LocalModelNode (corresponding to the semantic model where the measure is defined)
    * MeasureNode -> MetricTimeNode (edge describes aggregation time grain)
    """

    @override
    def add_edges_for_manifest(self, edge_list: list[SemanticGraphEdge]) -> None:
        for measure_lookup in self._manifest_object_lookup.simple_metric_model_lookups:
            self._add_edges_for_measure_model(measure_lookup, edge_list)

    def _add_edges_for_measure_model(
        self, lookup: SimpleMetricModelObjectLookup, edge_list: list[SemanticGraphEdge]
    ) -> None:
        model_id = SemanticModelId.get_instance(model_name=lookup.semantic_model.name)
        local_model_node = LocalModelNode.get_instance(model_id)

        metric_time_node = MetricTimeNode.get_instance()
        # For each unique aggregation time grain:
        for (
            aggregation_configuration,
            simple_metric_inputs,
        ) in lookup.aggregation_configuration_to_simple_metric_inputs.items():
            # For all simple metric inputs:
            for simple_metric_input in simple_metric_inputs:
                measure_name = simple_metric_input.name

                measure_node = MeasureNode.get_instance(
                    measure_name=measure_name,
                    source_model_id=model_id,
                )
                # Add an edge from the measure node to the metric-time node.
                edge_list.append(
                    EntityRelationshipEdge.create(
                        tail_node=measure_node,
                        head_node=metric_time_node,
                        recipe_update=AttributeRecipeStep(
                            set_source_time_grain=aggregation_configuration.time_grain,
                            add_model_join=model_id,
                        ),
                    )
                )
                # Add an edge from the measure node to the model node.
                edge_list.append(
                    EntityRelationshipEdge.create(
                        tail_node=measure_node,
                        head_node=local_model_node,
                    )
                )
