from collections import defaultdict
from typing import List, Dict
from metricflow.instances import MetricModelReference
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    DataSourceElementContext,
    DataSourceElementReference,
    DataSourceElementType,
    FileContext,
    MetricContext,
    ModelValidationRule,
    ValidationIssueType,
    ValidationError,
    validate_safely,
)
from metricflow.references import MeasureReference


class DataSourceMeasuresUniqueRule(ModelValidationRule):
    """Asserts all measure names are unique across the model."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring measures exist in only one configured data source"
    )
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []

        measure_references_to_data_sources: Dict[MeasureReference, List] = defaultdict(list)
        for data_source in model.data_sources:
            for measure in data_source.measures:
                if measure.reference in measure_references_to_data_sources:
                    issues.append(
                        ValidationError(
                            context=DataSourceElementContext(
                                file_context=FileContext.from_metadata(metadata=data_source.metadata),
                                data_source_element=DataSourceElementReference(
                                    data_source_name=data_source.name, element_name=measure.name
                                ),
                                element_type=DataSourceElementType.MEASURE,
                            ),
                            message=f"Found measure with name {measure.name} in multiple data sources with names "
                            f"({measure_references_to_data_sources[measure.reference]})",
                        )
                    )
                measure_references_to_data_sources[measure.reference].append(data_source.name)

        return issues


class MetricMeasuresRule(ModelValidationRule):
    """Checks that the measures referenced in the metrics exist."""

    @staticmethod
    @validate_safely(whats_being_done="checking all measures referenced by the metric exist")
    def _validate_metric_measure_references(
        metric: Metric, valid_measure_names: List[str]
    ) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        for measure_reference in metric.measure_references:
            if measure_reference.element_name not in valid_measure_names:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message=(
                            f"Measure {measure_reference.element_name} referenced in metric {metric.name} is not "
                            f"defined in the model!"
                        ),
                    )
                )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring metric measures exist")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        valid_measure_names = []
        for data_source in model.data_sources:
            for measure in data_source.measures:
                valid_measure_names.append(measure.reference.element_name)

        for metric in model.metrics or []:
            issues += MetricMeasuresRule._validate_metric_measure_references(
                metric=metric, valid_measure_names=valid_measure_names
            )
        return issues
