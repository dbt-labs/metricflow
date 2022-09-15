from collections import defaultdict
from typing import List, DefaultDict
from metricflow.instances import DataSourceReference

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    DataSourceElementType,
    FileContext,
    ModelValidationRule,
    ValidationError,
    ValidationIssueType,
    validate_safely,
)


class ElementConsistencyRule(ModelValidationRule):
    """Checks that elements in data sources with the same name are of the same element type across the model

    This reduces the potential confusion that might arise from having an identifier named `country` and a dimension
    named `country` while allowing for things like the `user` identifier to exist in multiple data sources. Note not
    all element types allow duplicates, and there are separate validation rules for those cases. See, for example,
    the DataSourceMeasuresUniqueRule.
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
            types_used = [DataSourceElementType(v) for v in sorted(k.value for k in type_to_context.keys())]
            for element_type in types_used:
                data_source_contexts = type_to_context[element_type]
                data_source_names = {ctx.data_source.data_source_name for ctx in data_source_contexts}
                data_source_context = data_source_contexts[0]
                issues.append(
                    ValidationError(
                        context=data_source_context,
                        message=f"In data sources {data_source_names}, element `{element_name}` is of type "
                        f"{element_type}, but it is used as types {types_used} across the model.",
                    )
                )

        return issues

    @staticmethod
    def _get_element_name_to_types(
        model: UserConfiguredModel,
    ) -> DefaultDict[str, DefaultDict[DataSourceElementType, List[DataSourceContext]]]:
        """Create a mapping of all element names in the model to types with a list of associated DataSourceContexts"""
        element_types: DefaultDict[str, DefaultDict[DataSourceElementType, List[DataSourceContext]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for data_source in model.data_sources:
            data_source_context = DataSourceContext(
                file_context=FileContext.from_metadata(metadata=data_source.metadata),
                data_source=DataSourceReference(data_source_name=data_source.name),
            )
            if data_source.measures:
                for measure in data_source.measures:
                    element_types[measure.name][DataSourceElementType.MEASURE].append(data_source_context)
            if data_source.dimensions:
                for dimension in data_source.dimensions:
                    element_types[dimension.name][DataSourceElementType.DIMENSION].append(data_source_context)
            if data_source.identifiers:
                for identifier in data_source.identifiers:
                    element_types[identifier.name][DataSourceElementType.IDENTIFIER].append(data_source_context)
        return element_types
