from __future__ import annotations

from abc import ABC, abstractmethod

from dbt_semantic_interfaces.protocols import SemanticManifest

from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup


class SemanticGraphRecipe(ABC):
    def __init__(  # noqa: D107
        self, semantic_manifest: SemanticManifest, semantic_model_lookup: SemanticModelLookup
    ) -> None:
        self._semantic_manifest = semantic_manifest
        self._semantic_model_lookup = semantic_model_lookup

    @abstractmethod
    def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
        """Transform the given semantic graph to another one. e.g. by added edges or nodes."""
        raise NotImplementedError
