from typing import List

from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.validations.validator_helpers import (
    ModelValidationRule,
    ValidationError,
    ValidationIssue,
    validate_safely,
)


class NonEmptyRule(ModelValidationRule):
    """Check if the model contains semantic models and metrics."""

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has semantic models")
    def _check_model_has_semantic_models(model: SemanticManifest) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if not model.semantic_models:
            issues.append(
                ValidationError(
                    message="No semantic models present in the model.",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has metrics")
    def _check_model_has_metrics(model: SemanticManifest) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        # If we are going to generate measure proxy metrics that is sufficient as well
        create_measure_proxy_metrics = False
        for semantic_model in model.semantic_models:
            for measure in semantic_model.measures:
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
    @validate_safely("running model validation rule ensuring metrics and semantic models are defined")
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        issues += NonEmptyRule._check_model_has_semantic_models(model=model)
        issues += NonEmptyRule._check_model_has_metrics(model=model)
        return issues
