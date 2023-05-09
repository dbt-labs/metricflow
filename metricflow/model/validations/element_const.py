from collections import defaultdict
from typing import List, DefaultDict

from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import SemanticModelReference
from metricflow.model.validations.validator_helpers import (
    SemanticModelContext,
    SemanticModelElementType,
    FileContext,
    ModelValidationRule,
    ValidationError,
    ValidationIssue,
    validate_safely,
)


class ElementConsistencyRule(ModelValidationRule):
    """Checks that elements in data sources with the same name are of the same element type across the model

    This reduces the potential confusion that might arise from having an entity named `country` and a dimension
    named `country` while allowing for things like the `user` entity to exist in multiple data sources. Note not
    all element types allow duplicates, and there are separate validation rules for those cases. See, for example,
    the SemanticModelMeasuresUniqueRule.
    """

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring model wide element consistency")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues = []
        element_name_to_types = ElementConsistencyRule._get_element_name_to_types(model=model)
        invalid_elements = {
            name: type_mapping for name, type_mapping in element_name_to_types.items() if len(type_mapping) > 1
        }

        for element_name, type_to_context in invalid_elements.items():
            # Sort these by value to ensure consistent error messaging
            types_used = [SemanticModelElementType(v) for v in sorted(k.value for k in type_to_context.keys())]
            for element_type in types_used:
                semantic_model_contexts = type_to_context[element_type]
                semantic_model_names = {ctx.data_source.semantic_model_name for ctx in semantic_model_contexts}
                semantic_model_context = semantic_model_contexts[0]
                issues.append(
                    ValidationError(
                        context=semantic_model_context,
                        message=f"In data sources {semantic_model_names}, element `{element_name}` is of type "
                        f"{element_type}, but it is used as types {types_used} across the model.",
                    )
                )

        return issues

    @staticmethod
    def _get_element_name_to_types(
        model: UserConfiguredModel,
    ) -> DefaultDict[str, DefaultDict[SemanticModelElementType, List[SemanticModelContext]]]:
        """Create a mapping of all element names in the model to types with a list of associated SemanticModelContexts"""
        element_types: DefaultDict[
            str, DefaultDict[SemanticModelElementType, List[SemanticModelContext]]
        ] = defaultdict(lambda: defaultdict(list))
        for data_source in model.data_sources:
            semantic_model_context = SemanticModelContext(
                file_context=FileContext.from_metadata(metadata=data_source.metadata),
                data_source=SemanticModelReference(semantic_model_name=data_source.name),
            )
            if data_source.measures:
                for measure in data_source.measures:
                    element_types[measure.name][SemanticModelElementType.MEASURE].append(semantic_model_context)
            if data_source.dimensions:
                for dimension in data_source.dimensions:
                    element_types[dimension.name][SemanticModelElementType.DIMENSION].append(semantic_model_context)
            if data_source.entities:
                for entity in data_source.entities:
                    element_types[entity.name][SemanticModelElementType.ENTITY].append(semantic_model_context)
        return element_types
