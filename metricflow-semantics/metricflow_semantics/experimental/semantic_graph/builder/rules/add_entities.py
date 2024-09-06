from __future__ import annotations

import logging

from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_transform_rule import (
    SemanticGraphRecipe,
)
from metricflow_semantics.experimental.semantic_graph.graph_nodes import EntityNode

logger = logging.getLogger(__name__)


class AddEntitiesRule(SemanticGraphRecipe):
    def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
        for semantic_model in self._semantic_manifest.semantic_models:
            for entity in semantic_model.entities:
                semantic_graph.nodes.add(EntityNode(entity.reference))

            primary_entity_reference = semantic_model.primary_entity_reference
            if primary_entity_reference is not None:
                semantic_graph.nodes.add(EntityNode(primary_entity_reference))
