from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.edges.sg_edges import EntityRelationshipEdge
from metricflow_semantics.semantic_graph.lookups.simple_metric_model_object_lookup import SimpleMetricModelObjectLookup
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.semantic_graph.nodes.entity_nodes import (
    LocalModelNode,
    MetricTimeNode,
    SimpleMetricNode,
)
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
)

logger = logging.getLogger(__name__)


class SimpleMetricSubgraphGenerator(SemanticSubgraphGenerator):
    """Generate the subgraph that relates simple metrics to other entities.

    For each simple metric, this adds the following edges:

    * SimpleMetricNode -> LocalModelNode (corresponding to the semantic model where the simple metric is defined)
    * SimpleMetricNode -> MetricTimeNode (edge describes aggregation time grain)
    """

    @override
    def add_edges_for_manifest(self, edge_list: list[SemanticGraphEdge]) -> None:
        for lookup in self._manifest_object_lookup.simple_metric_model_lookups:
            self._add_edges_for_simple_metric_model(lookup, edge_list)

    def _add_edges_for_simple_metric_model(
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
            # For all simple-metric inputs:
            for simple_metric_input in simple_metric_inputs:
                simple_metric_name = simple_metric_input.name

                simple_metric_node = SimpleMetricNode.get_instance(simple_metric_name)
                # Add an edge from the simple-metric input node to the metric-time node.
                edge_list.append(
                    EntityRelationshipEdge.create(
                        tail_node=simple_metric_node,
                        head_node=metric_time_node,
                        recipe_update=AttributeRecipeStep(
                            set_source_time_grain=aggregation_configuration.time_grain,
                            add_model_join=model_id,
                        ),
                    )
                )
                # Add an edge from the simple-metric input node to the model node.
                edge_list.append(
                    EntityRelationshipEdge.create(
                        tail_node=simple_metric_node,
                        head_node=local_model_node,
                    )
                )
