from __future__ import annotations

import logging
from typing import Optional

from typing_extensions import override

from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.weight_function import WeightFunction

logger = logging.getLogger(__name__)

from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)


class DunderNameWeightFunction(WeightFunction[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath]):
    @override
    def incremental_weight(
        self, path_to_node: AttributeComputationPath, edge_from_node: SemanticGraphEdge
    ) -> Optional[int]:
        current_attribute_computation = path_to_node.attribute_computation
        dundered_name_element_additions = (
            edge_from_node.attribute_computation_update.dundered_name_element_additions
            + edge_from_node.head_node.attribute_computation_update.dundered_name_element_additions
        )
        dundered_name_elements = (
            current_attribute_computation.attribute_descriptor.dundered_name_elements + dundered_name_element_additions
        )
        # We do not allow repeated element names in the dundered name (e.g. `listing__listing`),
        # so return `None` to indicate a blocked edge.
        if len(dundered_name_elements) >= 2:
            if dundered_name_elements[-1] == dundered_name_elements[-2]:
                return None
        return len(dundered_name_element_additions)
