from __future__ import annotations

from datetime import date
from typing import Generic, List, Sequence

from metricflow_semantic_interfaces.protocols import SemanticManifestT, SemanticModel
from metricflow_semantic_interfaces.references import SemanticModelElementReference
from metricflow_semantic_interfaces.type_enums.dimension_type import DimensionType
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    SemanticManifestValidationRule,
    SemanticModelElementContext,
    SemanticModelElementType,
    ValidationFutureError,
    ValidationIssue,
    validate_safely,
)


class TimeDimensionHasGranularityRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that any time dimension has a granularity set."""

    @staticmethod
    @validate_safely(whats_being_done="checking time dimensions have a granularity set")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        for semantic_model in semantic_manifest.semantic_models:
            issues.extend(TimeDimensionHasGranularityRule._validate_semantic_model(semantic_model))

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking time dimensions have a granularity set for a semantic model")
    def _validate_semantic_model(semantic_model: SemanticModel) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for dimension in semantic_model.dimensions:
            # Only check time dimensions
            if dimension.type != DimensionType.TIME:
                continue

            granularity = dimension.type_params.time_granularity if dimension.type_params else None
            if granularity is None:
                context = SemanticModelElementContext(
                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                    semantic_model_element=SemanticModelElementReference(
                        semantic_model_name=semantic_model.name, element_name=dimension.name
                    ),
                    element_type=SemanticModelElementType.DIMENSION,
                )
                issues.append(
                    ValidationFutureError(
                        context=context,
                        message=(
                            f"In semantic model `{semantic_model.name}`, time dimension `{dimension.name}` "
                            f"must have a time granularity set."
                        ),
                        error_date=date(2027, 1, 1),
                    )
                )

        return issues
