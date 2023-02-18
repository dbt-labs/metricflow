import logging

from metricflow.model.objects.conversions import MetricFlowMetricFlowEntity
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule

logger = logging.getLogger(__name__)


class LowerCaseNamesRule(ModelTransformRule):
    """Lowercases the names of both top level objects and entity elements in a model"""

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        LowerCaseNamesRule._lowercase_top_level_objects(model)
        for entity in model.entities:
            LowerCaseNamesRule._lowercase_entity_elements(entity)

        return model

    @staticmethod
    def _lowercase_entity_elements(entity: MetricFlowEntity) -> None:
        """Lowercases the names of entity elements."""
        if entity.measures:
            for measure in entity.measures:
                measure.name = measure.name.lower()
        if entity.identifiers:
            for identifier in entity.identifiers:
                identifier.name = identifier.name.lower()
        if entity.dimensions:
            for dimension in entity.dimensions:
                dimension.name = dimension.name.lower()

    @staticmethod
    def _lowercase_top_level_objects(model: UserConfiguredModel) -> None:
        """Lowercases the names of model objects"""
        if model.entities:
            for entity in model.entities:
                entity.name = entity.name.lower()
