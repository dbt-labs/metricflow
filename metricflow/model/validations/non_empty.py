from typing import List

from dbt.dbt_semantic.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationError,
    ValidationIssueType,
    validate_safely,
)


class NonEmptyRule(ModelValidationRule):
    """Check if the model contains entities and metrics."""

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has entities")
    def _check_model_has_entities(model: UserConfiguredModel) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []
        if not model.entities:
            issues.append(
                ValidationError(
                    message="No entities present in the model.",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has metrics")
    def _check_model_has_metrics(model: UserConfiguredModel) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        # If we are going to generate measure proxy metrics that is sufficient as well
        create_measure_proxy_metrics = False
        for entity in model.entities:
            for measure in entity.measures:
                if measure.create_metric is True:
                    create_measure_proxy_metrics = True
                    break

        if not model.metrics and not create_measure_proxy_metrics:
            issues.append(
                ValidationError(
                    message="No metrics present in the model.",
                )
            )
        return issues

    @staticmethod
    @validate_safely("running model validation rule ensuring metrics and entities are defined")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        issues += NonEmptyRule._check_model_has_entities(model=model)
        issues += NonEmptyRule._check_model_has_metrics(model=model)
        return issues
