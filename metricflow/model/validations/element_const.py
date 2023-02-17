from collections import defaultdict
from typing import List, DefaultDict
from metricflow.instances import EntityReference

from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    EntityContext,
    EntityElementType,
    FileContext,
    ModelValidationRule,
    ValidationError,
    ValidationIssueType,
    validate_safely,
)


class ElementConsistencyRule(ModelValidationRule):
    """Checks that elements in entities with the same name are of the same element type across the model

    This reduces the potential confusion that might arise from having an identifier named `country` and a dimension
    named `country` while allowing for things like the `user` identifier to exist in multiple entities. Note not
    all element types allow duplicates, and there are separate validation rules for those cases. See, for example,
    the EntityMeasuresUniqueRule.
    """

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring model wide element consistency")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues = []
        element_name_to_types = ElementConsistencyRule._get_element_name_to_types(model=model)
        invalid_elements = {
            name: type_mapping for name, type_mapping in element_name_to_types.items() if len(type_mapping) > 1
        }

        for element_name, type_to_context in invalid_elements.items():
            # Sort these by value to ensure consistent error messaging
            types_used = [EntityElementType(v) for v in sorted(k.value for k in type_to_context.keys())]
            for element_type in types_used:
                entity_contexts = type_to_context[element_type]
                entity_names = {ctx.entity.entity_name for ctx in entity_contexts}
                entity_context = entity_contexts[0]
                issues.append(
                    ValidationError(
                        context=entity_context,
                        message=f"In entities {entity_names}, element `{element_name}` is of type "
                        f"{element_type}, but it is used as types {types_used} across the model.",
                    )
                )

        return issues

    @staticmethod
    def _get_element_name_to_types(
        model: UserConfiguredModel,
    ) -> DefaultDict[str, DefaultDict[EntityElementType, List[EntityContext]]]:
        """Create a mapping of all element names in the model to types with a list of associated EntityContexts"""
        element_types: DefaultDict[str, DefaultDict[EntityElementType, List[EntityContext]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for entity in model.entities:
            entity_context = EntityContext(
                file_context=FileContext.from_metadata(metadata=entity.metadata),
                entity=EntityReference(entity_name=entity.name),
            )
            if entity.measures:
                for measure in entity.measures:
                    element_types[measure.name][EntityElementType.MEASURE].append(entity_context)
            if entity.dimensions:
                for dimension in entity.dimensions:
                    element_types[dimension.name][EntityElementType.DIMENSION].append(entity_context)
            if entity.identifiers:
                for identifier in entity.identifiers:
                    element_types[identifier.name][EntityElementType.IDENTIFIER].append(entity_context)
        return element_types
