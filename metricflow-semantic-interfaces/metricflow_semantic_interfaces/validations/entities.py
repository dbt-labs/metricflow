from __future__ import annotations

import logging
from typing import Generic, List, Sequence

from metricflow_semantic_interfaces.protocols import SemanticManifestT, SemanticModel
from metricflow_semantic_interfaces.references import SemanticModelReference
from metricflow_semantic_interfaces.type_enums import EntityType
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    SemanticManifestValidationRule,
    SemanticModelContext,
    ValidationError,
    ValidationIssue,
    validate_safely,
)

logger = logging.getLogger(__name__)


class NaturalEntityConfigurationRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Ensures that entities marked as EntityType.NATURAL are configured correctly."""

    @staticmethod
    @validate_safely(
        whats_being_done=(
            "checking that each semantic model has no more than one natural entity, and that "
            "natural entities are used in the appropriate contexts"
        )
    )
    def _validate_semantic_model_natural_entities(semantic_model: SemanticModel) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        context = SemanticModelContext(
            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
            semantic_model=SemanticModelReference(semantic_model_name=semantic_model.name),
        )

        natural_entity_names = set(
            [entity.name for entity in semantic_model.entities if entity.type is EntityType.NATURAL]
        )
        if len(natural_entity_names) > 1:
            error = ValidationError(
                context=context,
                message=f"Semantic models can have at most one natural entity, but semantic model "
                f"`{semantic_model.name}` has {len(natural_entity_names)} distinct natural entities set! "
                f"{natural_entity_names}.",
            )
            issues.append(error)
        if natural_entity_names and not [dim for dim in semantic_model.dimensions if dim.validity_params]:
            error = ValidationError(
                context=context,
                message=f"The use of `natural` entities is currently supported only in conjunction with a validity "
                f"window defined in the set of time dimensions associated with the semantic model. Semantic model "
                f"`{semantic_model.name}` uses a natural entity ({natural_entity_names}) but does not define a "
                f"validity window!",
            )
            issues.append(error)

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that entities marked as EntityType.NATURAL are properly configured")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:
        """Validate entities marked as EntityType.NATURAL."""
        issues: List[ValidationIssue] = []
        for semantic_model in semantic_manifest.semantic_models:
            issues += NaturalEntityConfigurationRule._validate_semantic_model_natural_entities(
                semantic_model=semantic_model
            )

        return issues
