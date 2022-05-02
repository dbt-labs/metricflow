import traceback
from typing import List

from metricflow.errors.errors import ParsingException
from metricflow.model.objects.metric import Metric, MetricType, CumulativeMetricWindow
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationIssue,
    ValidationIssueType,
    ValidationError,
    ValidationFatal,
    ModelObjectType,
    validate_safely,
)


class MetricMeasuresRule(ModelValidationRule):
    """Checks that the measures referenced in the metrics exist."""

    @staticmethod
    @validate_safely(whats_being_done="checking measures referenced by the metric are exist")
    def _validate_metric_measure_references(
        metric: Metric, valid_measure_names: List[str]
    ) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []
        measures_in_metric = []

        if metric.type_params:
            if metric.type_params.measures:
                measures_in_metric.extend(metric.type_params.measures)
            if metric.type_params.numerator:
                measures_in_metric.append(metric.type_params.numerator)
            if metric.type_params.denominator:
                measures_in_metric.append(metric.type_params.denominator)

        for measure_in_metric in measures_in_metric:
            if measure_in_metric.element_name not in valid_measure_names:
                issues.append(
                    ValidationFatal(
                        model_object_reference=ValidationIssue.make_object_reference(
                            metric_name=metric.name,
                        ),
                        message=f"Invalid measure {measure_in_metric.element_name} in metric {metric.name}",
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


class CumulativeMetricRule(ModelValidationRule):
    """Checks that cumulative sum metrics are configured properly"""

    @staticmethod
    @validate_safely(whats_being_done="checking that the params of metric are valid if it is a cumulative sum metric")
    def _validate_cumulative_sum_metric_params(metric: Metric) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        if metric.type == MetricType.CUMULATIVE:
            if metric.type_params.window and metric.type_params.grain_to_date:
                issues.append(
                    ValidationError(
                        model_object_reference=ValidationIssue.make_object_reference(
                            object_type=ModelObjectType.METRIC,
                            object_name=metric.name,
                        ),
                        message="Both window and grain_to_date set for cumulative metric. Please set one or the other",
                    )
                )

            if metric.type_params.window:
                try:
                    _, _ = CumulativeMetricWindow.parse(metric.type_params.window.to_string())
                except ParsingException:
                    issues.append(
                        ValidationError(
                            model_object_reference=ValidationIssue.make_object_reference(
                                metric_name=metric.name,
                            ),
                            message=traceback.format_exc(),
                        )
                    )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring cumulative sum metrics are valid")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []

        for metric in model.metrics or []:
            issues += CumulativeMetricRule._validate_cumulative_sum_metric_params(metric=metric)

        return issues
