import logging
from typing import Tuple, Type

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_transformations.dbt_transform_rule import (
    DbtTransformRule,
    DbtTransformationResult,
    DbtTransformedObjects,
)
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.materialization import Materialization
from metricflow.model.objects.metric import Metric
from metricflow.model.validations.validator_helpers import ModelValidationResults

logger = logging.getLogger(__name__)

DEFAULT_RULES: Tuple[DbtTransformRule, ...] = tuple()


class DbtTransformer:
    """Handles transforming a dbt Manifest into a UserConfiguredModel"""

    def __init__(  # noqa: D
        self,
        rules: Tuple[DbtTransformRule, ...] = DEFAULT_RULES,
        data_source_class: Type[DataSource] = DataSource,
        metric_class: Type[Metric] = Metric,
        materialization_class: Type[Materialization] = Materialization,
    ) -> None:
        self.rules = rules
        self.data_source_class = data_source_class
        self.metric_class = metric_class
        self.materialization_class = materialization_class

    def transform(self, dbt_metrics: Tuple[MetricNode, ...]) -> DbtTransformationResult:
        """Using a series of rules transforms dbt metrics into a mapped dict representing UserConfiguredModel objects"""
        transformed_objects = DbtTransformedObjects()
        validation_results = ModelValidationResults()

        for rule in self.rules:
            transformation_rule_issues = rule.run(dbt_metrics=dbt_metrics, objects=transformed_objects)
            validation_results = ModelValidationResults.merge([validation_results, transformation_rule_issues])

        return DbtTransformationResult(transformed_objects=transformed_objects, validation_results=validation_results)
