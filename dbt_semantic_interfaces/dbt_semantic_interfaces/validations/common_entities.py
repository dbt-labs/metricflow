from typing import Dict, List, Set

from dbt_semantic_interfaces.objects.elements.entity import Entity
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    EntityReference,
    SemanticModelElementReference,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    ModelValidationRule,
    SemanticModelElementContext,
    SemanticModelElementType,
    ValidationIssue,
    ValidationWarning,
    validate_safely,
)


class CommonEntitysRule(ModelValidationRule):
    """Checks that entities exist on more than one semantic model."""

    @staticmethod
    def _map_semantic_model_entities(semantic_models: List[SemanticModel]) -> Dict[EntityReference, Set[str]]:
        """Generate mapping of entity names to the set of semantic_models where it is defined."""
        entities_to_semantic_models: Dict[EntityReference, Set[str]] = {}
        for semantic_model in semantic_models or []:
            for entity in semantic_model.entities or []:
                if entity.reference in entities_to_semantic_models:
                    entities_to_semantic_models[entity.reference].add(semantic_model.name)
                else:
                    entities_to_semantic_models[entity.reference] = {semantic_model.name}
        return entities_to_semantic_models

    @staticmethod
    @validate_safely(whats_being_done="checking entity exists on more than one semantic model")
    def _check_entity(
        entity: Entity,
        semantic_model: SemanticModel,
        entities_to_semantic_models: Dict[EntityReference, Set[str]],
    ) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        # If the entity is the dict and if the set of semantic models minus this semantic model is empty,
        # then we warn the user that their entity will be unused in joins
        if (
            entity.reference in entities_to_semantic_models
            and len(entities_to_semantic_models[entity.reference].difference({semantic_model.name})) == 0
        ):
            issues.append(
                ValidationWarning(
                    context=SemanticModelElementContext(
                        file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                        semantic_model_element=SemanticModelElementReference(
                            semantic_model_name=semantic_model.name, element_name=entity.name
                        ),
                        element_type=SemanticModelElementType.ENTITY,
                    ),
                    message=f"Entity `{entity.reference.element_name}` "
                    f"only found in one semantic model `{semantic_model.name}` "
                    f"which means it will be unused in joins.",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation warning if entities are only one one semantic model")
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:
        """Issues a warning for any entity that is associated with only one semantic_model."""
        issues = []

        entities_to_semantic_models = CommonEntitysRule._map_semantic_model_entities(model.semantic_models)
        for semantic_model in model.semantic_models or []:
            for entity in semantic_model.entities or []:
                issues += CommonEntitysRule._check_entity(
                    entity=entity,
                    semantic_model=semantic_model,
                    entities_to_semantic_models=entities_to_semantic_models,
                )

        return issues
