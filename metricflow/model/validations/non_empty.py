from collections import OrderedDict
from typing import List

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationError,
    ValidationIssueType,
    validate_safely,
)


class NonEmptyRule(ModelValidationRule):
    """Check if the model contains data sources and metrics."""

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has data sources")
    def _check_model_has_data_sources(model: UserConfiguredModel) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []
        if not model.data_sources:
            issues.append(
                ValidationError(
                    model_object_reference=OrderedDict(),
                    message="No data sources present in the model.",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has metrics")
    def _check_model_has_metrics(model: UserConfiguredModel) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        # If we are going to generate measure proxy metrics that is sufficient as well
        create_measure_proxy_metrics = False
        for data_source in model.data_sources:
            for measure in data_source.measures:
                if measure.create_metric is True:
                    create_measure_proxy_metrics = True
                    break

        if not model.metrics and not create_measure_proxy_metrics:
            issues.append(
                ValidationError(
                    model_object_reference=OrderedDict(),
                    message="No metrics present in the model.",
                )
            )
        return issues

    @staticmethod
    @validate_safely("running model validation rule ensuring metrics and data sources are defined")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        issues += NonEmptyRule._check_model_has_data_sources(model=model)
        issues += NonEmptyRule._check_model_has_metrics(model=model)
        return issues
