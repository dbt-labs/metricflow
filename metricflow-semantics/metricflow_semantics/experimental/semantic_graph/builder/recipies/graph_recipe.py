from __future__ import annotations

from abc import ABC

from dbt_semantic_interfaces.protocols import SemanticManifest, SemanticModel
from dbt_semantic_interfaces.references import SemanticModelReference

from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup


class IngredientLookup:
    def __init__(self, semantic_manifest: SemanticManifest, semantic_model_lookup: SemanticModelLookup) -> None:
        self._semantic_manifest = semantic_manifest
        self._semantic_model_lookup = semantic_model_lookup
        self._reference_to_semantic_model = {
            semantic_model.reference: semantic_model for semantic_model in semantic_manifest.semantic_models
        }

    def get_semantic_model_by_reference(self, reference: SemanticModelReference) -> SemanticModel:
        return self._reference_to_semantic_model[reference]


class SemanticGraphRecipe(ABC):
    def __init__(self, ingredient_lookup: IngredientLookup) -> None:  # noqa: D107
        self.ingredient_lookup = ingredient_lookup

    #
    # @property
    # def semantic_manifest(self) -> SemanticManifest:
    #     return self._ingredient_lookup.semantic_manifest
    #
    # @property
    # def semantic_model_lookup(self) -> SemanticModelLookup:
    #     return self._ingredient_lookup.semantic_model_lookup
