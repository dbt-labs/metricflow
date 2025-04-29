from __future__ import annotations

from metricflow_semantics.experimental.semantic_graph_old.builder.in_progress_semantic_graph import (
    InProgressSemanticGraph,
)
from metricflow_semantics.experimental.semantic_graph_old.builder.recipies.graph_recipe import SemanticGraphRecipe
from metricflow_semantics.experimental.semantic_graph_old.graph_nodes import AssociativeEntityNode


class ExpandAssociativeEntityRecipe(SemanticGraphRecipe):
    def expand_associative_entity(
        self,
        semantic_graph: InProgressSemanticGraph,
        associative_entity_node: AssociativeEntityNode,
    ) -> None:
        semantic_model = self.ingredient_lookup.get_semantic_model_by_reference(
            associative_entity_node.entity_id.via_semantic_model
        )
        raise NotImplementedError
