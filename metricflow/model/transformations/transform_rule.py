from abc import ABC, abstractmethod

from metricflow.model.objects.user_configured_model import UserConfiguredModel


class ModelTransformRule(ABC):
    """Encapsulates logic for transforming a model. e.g. add metrics based on measures."""

    @staticmethod
    @abstractmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:
        """Copy and transform the given model into a new model."""
        pass
