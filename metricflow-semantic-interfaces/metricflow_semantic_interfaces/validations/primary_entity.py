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


class PrimaryEntityRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that the primary entity has been properly defined in a semantic model.

    * If a semantic model contains dimensions, the primary entity must be available.
    * The primary entity could be defined by the primary_entity field, or by one of the entities defined in a semantic
      model.
    * There should only be one primary entity in the model.
    """

    @staticmethod
    def _model_requires_primary_entity(semantic_model: SemanticModel) -> bool:
        return len(semantic_model.dimensions) > 0

    @staticmethod
    @validate_safely("Check that a semantic model has properly configured primary entities.")
    def _check_model(semantic_model: SemanticModel) -> Sequence[ValidationIssue]:
        context = SemanticModelContext(
            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
            semantic_model=SemanticModelReference(semantic_model_name=semantic_model.name),
        )

        # If there are entities defined in the model, check that there's only one primary entity.
        entities_with_primary_type = tuple(
            entity for entity in semantic_model.entities if entity.type is EntityType.PRIMARY
        )

        if len(entities_with_primary_type) > 0:
            if len(entities_with_primary_type) > 1:
                primary_entity_names = [primary_entity.name for primary_entity in entities_with_primary_type]
                return (
                    ValidationError(
                        message=(
                            f"Semantic models can have only one primary entity. The semantic model"
                            f" `{semantic_model.name}` has {len(primary_entity_names)}: "
                            f"{', '.join(primary_entity_names)}"
                        ),
                        context=context,
                    ),
                )

            entity_with_primary_type = entities_with_primary_type[0]
            # If there is a primary entity, the primary entity field should not be set.
            if semantic_model.primary_entity_reference is not None:
                return (
                    ValidationError(
                        message=(
                            f"The semantic model `{semantic_model.name}` has an entity named "
                            f"`{entity_with_primary_type.name}` with type primary but it also has the `primary_entity` "
                            f"field set to `{semantic_model.primary_entity_reference.element_name}`. Both should not "
                            f"be present in the model."
                        ),
                        context=context,
                    ),
                )

        # Check that a primary entity has been set if required.
        if (
            PrimaryEntityRule._model_requires_primary_entity(semantic_model)
            and semantic_model.primary_entity_reference is None
            and len(entities_with_primary_type) == 0
        ):
            return (
                ValidationError(
                    message=(
                        f"The semantic model {semantic_model.name} contains dimensions, but it does not define a "
                        f"primary entity. Either add an entity with type PRIMARY or set a value for the "
                        f"primary_entity key."
                    ),
                    context=context,
                ),
            )

        return ()

    @staticmethod
    @validate_safely("Check that semantic models in the manifest have properly configured primary entities.")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        for semantic_model in semantic_manifest.semantic_models:
            issues += PrimaryEntityRule._check_model(semantic_model)

        return issues
