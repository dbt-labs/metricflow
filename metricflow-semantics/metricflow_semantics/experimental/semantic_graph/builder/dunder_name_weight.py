from __future__ import annotations

import logging
from typing import Optional

from typing_extensions import override

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_tuple_from_optional
from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import MetricDefinitionLabel
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.weight_function import WeightFunction
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS

logger = logging.getLogger(__name__)

from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)


class DunderNameWeightFunction(WeightFunction[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath]):
    MAX_ENTITY_LINKS = MAX_JOIN_HOPS

    _METRIC_DEFINITION_LABEL = MetricDefinitionLabel()

    @override
    def incremental_weight(
        self, path_to_node: AttributeComputationPath, edge_from_node: SemanticGraphEdge
    ) -> Optional[int]:
        # Don't allow traversal of the metric definition edges unless the previous edge in the path was also a metric
        # definition edge. This prevents unnecessary traversal when searching for group-by items as it prevents
        # traversal from the metric node (which is a successor of the `JoinToModelNode` and represents a group-by
        # metric).
        path_edges = path_to_node.edges
        if len(path_edges) > 0 and DunderNameWeightFunction._METRIC_DEFINITION_LABEL in edge_from_node.labels:
            last_edge = path_edges[-1]
            if DunderNameWeightFunction._METRIC_DEFINITION_LABEL not in last_edge.labels:
                return None

        current_attribute_computation = path_to_node.attribute_computation
        dundered_name_elements = (
            current_attribute_computation.attribute_descriptor.dundered_name_elements
            + mf_tuple_from_optional(edge_from_node.attribute_computation_update.dundered_name_element_addition)
            + mf_tuple_from_optional(
                edge_from_node.head_node.attribute_computation_update.dundered_name_element_addition
            )
        )
        # We do not allow repeated element names in the dundered name (e.g. `listing__listing`),
        # so return `None` to indicate a blocked edge.
        if len(dundered_name_elements) >= 2:
            if dundered_name_elements[-1] == dundered_name_elements[-2]:
                return None

        # dundered_name_element_additions = (
        #     edge_from_node.attribute_computation_update.dundered_name_element_additions
        #     + edge_from_node.head_node.attribute_computation_update.dundered_name_element_additions
        # )
        # # return len(dundered_name_element_additions)

        dsi_entity_name_additions = (
            current_attribute_computation.attribute_descriptor.dsi_entity_names
            + mf_tuple_from_optional(edge_from_node.attribute_computation_update.dsi_entity_addition)
            + mf_tuple_from_optional(edge_from_node.head_node.attribute_computation_update.dsi_entity_addition)
        )

        return len(dsi_entity_name_additions)
