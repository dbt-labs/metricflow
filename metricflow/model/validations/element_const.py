from typing import Dict, List

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationIssue,
    ModelObjectType,
    ValidationError,
    ValidationIssueType,
    validate_safely,
)


class ElementConsistencyRule(ModelValidationRule):
    """Checks that elements in data sources with the same name are of the same element type across the model"""

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring model wide element consistency")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues = []
        for data_source in model.data_sources:
            issues += ElementConsistencyRule._check_data_source(model=model, data_source=data_source, add_to_dict=True)
        return issues

    @staticmethod
    def _check_element_type(
        name_to_type: Dict[str, ModelObjectType],
        data_source_name: str,
        element_name: str,
        element_type: ModelObjectType,
        add_to_dict: bool,
    ) -> List[ValidationIssueType]:
        """Check if the given element matches the expected type.

        Args:
            name_to_type: dict from the name of the element to the expected type of the element in the model.
            data_source_name: the name of the data source where the element exists
            element_name: name of the element
            element_type: the type of the element
            add_to_dict: if the given element does not exist in the dictionary, whether to add it.
        """
        issues: List[ValidationIssueType] = []

        if element_name in name_to_type:
            existing_type = name_to_type[element_name]
            if existing_type != element_type:
                issues.append(
                    ValidationError(
                        model_object_reference=ValidationIssue.make_object_reference(
                            data_source_name=data_source_name,
                        ),
                        message=f"In data source {data_source_name}, element `{element_name}` is of type "
                        f"{element_type}, but it was previously used earlier in the model as "
                        f"{name_to_type[element_name]}",
                    )
                )
        else:
            if add_to_dict:
                name_to_type[element_name] = element_type
            elif element_type != ModelObjectType.DIMENSION:
                # TODO: Can't check dimensions effectively as their name changes.
                issues.append(
                    ValidationError(
                        model_object_reference=ValidationIssue.make_object_reference(
                            data_source_name=data_source_name,
                            object_type=element_type,
                            object_name=element_name,
                        ),
                        message=f"In data source {data_source_name}, the element named {element_name} "
                        f"of type {element_type} is not known in the model.",
                    )
                )

        return issues

    @staticmethod
    def _get_element_types(model: UserConfiguredModel) -> Dict[str, ModelObjectType]:
        # Store the element types
        element_types: Dict[str, ModelObjectType] = {}
        for data_source in model.data_sources:
            if data_source.measures:
                for measure in data_source.measures:
                    element_types[measure.name.element_name] = ModelObjectType.MEASURE
            if data_source.dimensions:
                for dimension in data_source.dimensions:
                    element_types[dimension.name.element_name] = ModelObjectType.DIMENSION
            if data_source.identifiers:
                for identifier in data_source.identifiers:
                    element_types[identifier.name.element_name] = ModelObjectType.IDENTIFIER
        return element_types

    @staticmethod
    @validate_safely(
        whats_being_done="checking that a data source's elements that share names with other elements across the model also are the same type"
    )
    def _check_data_source(
        model: UserConfiguredModel, data_source: DataSource, add_to_dict: bool
    ) -> List[ValidationIssueType]:  # noqa: D
        """Check if the elements in the data source matches the expected type.

        :param model: UserConfiguredModel to check
        :param data_source: the data source to check
        :param add_to_dict: if the given element does not exist in the dictionary, whether to add it.
        """
        measure_name_tuples = [(x.name, ModelObjectType.MEASURE) for x in data_source.measures or []]
        dimension_name_tuples = [(x.name, ModelObjectType.DIMENSION) for x in data_source.dimensions or []]
        identifier_name_tuples = [(x.name, ModelObjectType.IDENTIFIER) for x in data_source.identifiers or []]
        issues = []
        for element_name, element_type in measure_name_tuples + dimension_name_tuples + identifier_name_tuples:
            issues += ElementConsistencyRule._check_element_type(
                name_to_type=ElementConsistencyRule._get_element_types(model),
                data_source_name=data_source.name,
                element_name=element_name,
                element_type=element_type,
                add_to_dict=add_to_dict,
            )
        return issues
