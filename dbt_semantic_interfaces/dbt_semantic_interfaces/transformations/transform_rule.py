from abc import ABC, abstractmethod

from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest


class ModelTransformRule(ABC):
    """Encapsulates logic for transforming a model. e.g. add metrics based on measures."""

    @staticmethod
    @abstractmethod
    def transform_model(model: SemanticManifest) -> SemanticManifest:
        """Copy and transform the given model into a new model."""
        pass
