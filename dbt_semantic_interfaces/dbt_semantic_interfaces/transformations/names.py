import logging

from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.transformations.transform_rule import ModelTransformRule

logger = logging.getLogger(__name__)


class LowerCaseNamesRule(ModelTransformRule):
    """Lowercases the names of both top level objects and semantic model elements in a model"""

    @staticmethod
    def transform_model(model: SemanticManifest) -> SemanticManifest:  # noqa: D
        LowerCaseNamesRule._lowercase_top_level_objects(model)
        for semantic_model in model.semantic_models:
            LowerCaseNamesRule._lowercase_semantic_model_elements(semantic_model)

        return model

    @staticmethod
    def _lowercase_semantic_model_elements(semantic_model: SemanticModel) -> None:
        """Lowercases the names of semantic model elements."""
        if semantic_model.measures:
            for measure in semantic_model.measures:
                measure.name = measure.name.lower()
        if semantic_model.entities:
            for entity in semantic_model.entities:
                entity.name = entity.name.lower()
        if semantic_model.dimensions:
            for dimension in semantic_model.dimensions:
                dimension.name = dimension.name.lower()

    @staticmethod
    def _lowercase_top_level_objects(model: SemanticManifest) -> None:
        """Lowercases the names of model objects"""
        if model.semantic_models:
            for semantic_model in model.semantic_models:
                semantic_model.name = semantic_model.name.lower()
