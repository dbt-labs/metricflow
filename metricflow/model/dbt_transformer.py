from copy import deepcopy
import logging
import traceback
from typing import List, Tuple, Type

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_transformations.dbt_transform_rule import (
    DbtTransformRule,
    DbtTransformationResult,
    DbtTransformedObjects,
)
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.materialization import Materialization
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationError, ValidationIssue

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

    def build(self, transformed_objects: DbtTransformedObjects) -> ModelBuildResult:
        """Takes in a map of dicts representing UserConfiguredModel objects, and builds a UserConfiguredModel"""
        # we don't want to modify the passed in objects, so we decopy them
        copied_objects = deepcopy(transformed_objects)

        # Move dimensions, identifiers, and measures on to their respective data sources
        for data_source_name, dimensions_map in copied_objects.dimensions.items():
            copied_objects.data_sources[data_source_name]["dimensions"] = list(dimensions_map.values())
        for data_source_name, identifiers_map in copied_objects.identifiers.items():
            copied_objects.data_sources[data_source_name]["identifiers"] = list(identifiers_map.values())
        for data_source_name, measure_map in copied_objects.measures.items():
            copied_objects.data_sources[data_source_name]["measures"] = list(measure_map.values())

        issues: List[ValidationIssue] = []

        data_sources: List[Type[DataSource]] = []
        for data_source_dict in copied_objects.data_sources.values():
            try:
                data_sources.append(self.data_source_class.parse_obj(data_source_dict))
            except Exception as e:
                issues.append(
                    ValidationError(
                        message=f"Failed to parse dict of data source {data_source_dict.get('name')} to object",
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )

        materializations: List[Type[Materialization]] = []
        for materialization_dict in copied_objects.materializations.values():
            try:
                materializations.append(self.materialization_class.parse_obj(materialization_dict))
            except Exception as e:
                issues.append(
                    ValidationError(
                        message=f"Failed to parse dict of materialization {materialization_dict.get('name')} to object",
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )

        metrics: List[Type[Metric]] = []
        for metric_dict in copied_objects.metrics.values():
            try:
                metrics.append(self.metric_class.parse_obj(metric_dict))
            except Exception as e:
                issues.append(
                    ValidationError(
                        message=f"Failed to parse dict of metric {metric_dict.get('name')} to object",
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )

        return ModelBuildResult(
            model=UserConfiguredModel(data_sources=data_sources, materializations=materializations, metrics=metrics),
            issues=ModelValidationResults.from_issues_sequence(issues=issues),
        )

    def transform_and_build(self, dbt_metrics: Tuple[MetricNode, ...]) -> ModelBuildResult:
        """Builds a UserConfiguredModel from dbt MetricNodes"""
        transformation_result = self.transform(dbt_metrics=dbt_metrics)

        if transformation_result.validation_results.has_blocking_issues:
            return ModelBuildResult(
                model=UserConfiguredModel(data_sources=[], metrics=[]), issues=transformation_result.validation_results
            )

        build_result = self.build(transformed_objects=transformation_result.transformed_objects)
        return ModelBuildResult(
            build_result.model,
            ModelValidationResults.merge([transformation_result.validation_results, build_result.issues]),
        )
