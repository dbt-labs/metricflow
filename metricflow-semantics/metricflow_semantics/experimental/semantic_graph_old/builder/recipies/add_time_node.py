from __future__ import annotations

from metricflow_semantics.experimental.semantic_graph_old.builder.in_progress_semantic_graph import (
    InProgressSemanticGraph,
)
from metricflow_semantics.experimental.semantic_graph_old.builder.recipies.graph_recipe import SemanticGraphRecipe


class AddTimeNodes(SemanticGraphRecipe):
    def add_time_nodes(self, semantic_graph: InProgressSemanticGraph) -> None:
        semantic_graph
