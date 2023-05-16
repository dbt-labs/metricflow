import logging
from datetime import date
from typing import List, MutableSet

from dbt_semantic_interfaces.objects.elements.entity import EntityType
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import SemanticModelReference
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    ModelValidationRule,
    SemanticModelContext,
    ValidationError,
    ValidationFutureError,
    ValidationIssue,
    validate_safely,
)

logger = logging.getLogger(__name__)


class NaturalEntityConfigurationRule(ModelValidationRule):
    """Ensures that entities marked as EntityType.NATURAL are configured correctly."""

    @staticmethod
    @validate_safely(
        whats_being_done=(
            "checking that each semantic model has no more than one natural entity, and that "
            "natural entities are used in the appropriate contexts"
        )
    )
    def _validate_semantic_model_natural_entities(semantic_model: SemanticModel) -> List[ValidationIssue]:
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
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:
        """Validate entities marked as EntityType.NATURAL."""
        issues: List[ValidationIssue] = []
        for semantic_model in model.semantic_models:
            issues += NaturalEntityConfigurationRule._validate_semantic_model_natural_entities(
                semantic_model=semantic_model
            )

        return issues


class OnePrimaryEntityPerSemanticModelRule(ModelValidationRule):
    """Ensures that each semantic model has only one primary entity."""

    @staticmethod
    @validate_safely(whats_being_done="checking semantic model has only one primary entity")
    def _only_one_primary_entity(semantic_model: SemanticModel) -> List[ValidationIssue]:
        primary_entity_names: MutableSet[str] = set()
        for entity in semantic_model.entities or []:
            if entity.type == EntityType.PRIMARY:
                primary_entity_names.add(entity.reference.element_name)

        if len(primary_entity_names) > 1:
            return [
                ValidationFutureError(
                    context=SemanticModelContext(
                        file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                        semantic_model=SemanticModelReference(semantic_model_name=semantic_model.name),
                    ),
                    message=f"Semantic models can have only one primary entity. The semantic model"
                    f" `{semantic_model.name}` has {len(primary_entity_names)}: {', '.join(primary_entity_names)}",
                    error_date=date(2022, 1, 12),  # Wed January 12th 2022
                )
            ]
        return []

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring each semantic model has only one primary entity"
    )
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
        issues = []

        for semantic_model in model.semantic_models:
            issues += OnePrimaryEntityPerSemanticModelRule._only_one_primary_entity(semantic_model=semantic_model)

        return issues
