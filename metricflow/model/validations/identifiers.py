import logging
from datetime import date
from typing import List, MutableSet

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.identifier import IdentifierType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationIssue,
    ValidationError,
    ValidationIssueType,
    validate_safely,
)
from metricflow.model.validations.validator_helpers import ValidationFutureError

logger = logging.getLogger(__name__)


class IdentifierConfigRule(ModelValidationRule):
    """Checks that data source identifiers are valid"""

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring identifiers are valid")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues = []
        for data_source in model.data_sources:
            issues += IdentifierConfigRule._validate_data_source_identifiers(data_source=data_source)
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the data source's identifiers are valid")
    def _validate_data_source_identifiers(data_source: DataSource) -> List[ValidationIssueType]:
        """Checks validity of composite identifiers"""
        issues: List[ValidationIssueType] = []
        for ident in data_source.identifiers:
            if ident.identifiers:
                for sub_id in ident.identifiers:
                    if sub_id.ref and (sub_id.name or sub_id.expr):
                        logger.warning(f"Identifier with error is: {ident}")
                        issues.append(
                            ValidationError(
                                model_object_reference=ValidationIssue.make_object_reference(
                                    data_source_name=data_source.name,
                                    identifier_name=ident.name.element_name,
                                ),
                                message=f"Both ref and name/expr set in sub identifier of identifier "
                                f"({ident.name.element_name}), please set one",
                            )
                        )
                    elif sub_id.ref is not None and sub_id.ref not in [i.name for i in data_source.identifiers]:
                        issues.append(
                            ValidationError(
                                model_object_reference=ValidationIssue.make_object_reference(
                                    data_source_name=data_source.name, identifier_name=ident.name.element_name
                                ),
                                message=f"Identifier ref must reference an existing identifier by name. "
                                f"No identifier in this data source has name: {sub_id.ref}",
                            )
                        )
                    elif not sub_id.ref and not sub_id.name:
                        issues.append(
                            ValidationError(
                                model_object_reference=ValidationIssue.make_object_reference(
                                    data_source_name=data_source.name, identifier_name=ident.name.element_name
                                ),
                                message=f"Must provide either name or ref for sub identifier of identifier "
                                f"with name: {ident.name.element_name}",
                            )
                        )

                    if sub_id.name:
                        for i in data_source.identifiers:
                            if i.name == sub_id.name and i.expr != sub_id.expr:
                                issues.append(
                                    ValidationError(
                                        model_object_reference=ValidationIssue.make_object_reference(
                                            data_source_name=data_source.name, identifier_name=ident.name.element_name
                                        ),
                                        message=f"If sub identifier has same name ({sub_id.name.element_name}) "
                                        f"as an existing Identifier they must have the same expr",
                                    )
                                )
                                break

        return issues


class OnePrimaryIdentifierPerDataSourceRule(ModelValidationRule):
    """Ensures that each data source has only one primary identifier"""

    @staticmethod
    @validate_safely(whats_being_done="checking data source has only one primary identifier")
    def _only_one_primary_identifier(data_source: DataSource) -> List[ValidationIssue]:
        primary_identifier_names: MutableSet[str] = set()
        for identifier in data_source.identifiers or []:
            if identifier.type == IdentifierType.PRIMARY:
                primary_identifier_names.add(identifier.name.element_name)

        if len(primary_identifier_names) > 1:
            return [
                ValidationFutureError(
                    model_object_reference=ValidationIssue.make_object_reference(data_source_name=data_source.name),
                    message=f"Data sources can have only one primary identifier. The data source"
                    f" `{data_source.name}` has {len(primary_identifier_names)}: {', '.join(primary_identifier_names)}",
                    error_date=date(2022, 1, 12),  # Wed January 12th 2022
                )
            ]
        return []

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring each data source has only one primary identifier"
    )
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues = []

        for data_source in model.data_sources:
            issues += OnePrimaryIdentifierPerDataSourceRule._only_one_primary_identifier(data_source=data_source)

        return issues
