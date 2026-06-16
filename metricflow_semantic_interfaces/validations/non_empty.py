from __future__ import annotations

from typing import Generic, List, Sequence

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import SemanticManifestT
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationRule,
    ValidationError,
    ValidationIssue,
    validate_safely,
)


class NonEmptyRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Check if the model contains semantic models and metrics."""

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has semantic models")
    def _check_model_has_semantic_models(semantic_manifest: PydanticSemanticManifest) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if not semantic_manifest.semantic_models:
            issues.append(
                ValidationError(
                    message="No semantic models present in the model.",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has metrics")
    def _check_model_has_metrics(semantic_manifest: PydanticSemanticManifest) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        # If we are going to generate measure proxy metrics that is sufficient as well
        create_measure_proxy_metrics = False
        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if measure.create_metric is True:
                    create_measure_proxy_metrics = True
                    break

        if not semantic_manifest.metrics and not create_measure_proxy_metrics:
            issues.append(
                ValidationError(
                    message="No metrics present in the model.",
                )
            )
        return issues

    @staticmethod
    @validate_safely("running model validation rule ensuring metrics and semantic models are defined")
    def validate_manifest(  # noqa: D102
        # PydanticSemanticManifest is required here due to a Measure.create_metric call downstream.
        # TODO: can we add create_metric to the Measure protocol to avoid this type override?
        semantic_manifest: PydanticSemanticManifest,  # type: ignore[override]
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        issues.extend(NonEmptyRule._check_model_has_semantic_models(semantic_manifest=semantic_manifest))
        issues.extend(NonEmptyRule._check_model_has_metrics(semantic_manifest=semantic_manifest))
        return issues
