import logging
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date
from typing import List, MutableSet, Tuple, Sequence

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.identifier import Identifier, IdentifierType, CompositeSubIdentifier
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationIssue,
    ValidationError,
    ValidationIssueType,
    validate_safely,
    ValidationWarning,
)
from metricflow.model.validations.validator_helpers import ValidationFutureError
from metricflow.specs import IdentifierReference

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


@dataclass(frozen=True)
class SubIdentifierContext:
    """Organizes the context behind identifiers and their sub-identifiers."""

    data_source_name: str
    identifier_reference: IdentifierReference
    sub_identifier_names: Tuple[str, ...]


class IdentifierConsistencyRule(ModelValidationRule):
    """Checks identifiers with the same name are defined with the same set of sub-identifiers in all data sources"""

    @staticmethod
    def _get_sub_identifier_names(identifier: Identifier) -> Sequence[str]:
        sub_identifier_names = []
        sub_identifier: CompositeSubIdentifier
        for sub_identifier in identifier.identifiers or []:
            if sub_identifier.name:
                sub_identifier_names.append(sub_identifier.name.element_name)
            elif sub_identifier.ref:
                sub_identifier_names.append(sub_identifier.ref)
        return sub_identifier_names

    @staticmethod
    def _get_sub_identifier_context(data_source: DataSource) -> Sequence[SubIdentifierContext]:
        contexts = []
        for identifier in data_source.identifiers or []:
            contexts.append(
                SubIdentifierContext(
                    data_source_name=data_source.name,
                    identifier_reference=identifier.name,
                    sub_identifier_names=tuple(IdentifierConsistencyRule._get_sub_identifier_names(identifier)),
                )
            )
        return contexts

    @staticmethod
    @validate_safely(whats_being_done="running model validation to ensure identifiers have consistent sub-identifiers")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        # The expected sub identifiers that an identifier has.
        identifier_to_sub_identifiers: OrderedDict[str, Tuple[str, ...]] = OrderedDict()
        # The identifiers that have different sub-identifiers in different data sources.
        identifiers_with_inconsistent_sub_identifiers = []

        identifier_contexts = []
        issues = []
        for data_source in model.data_sources or []:
            sub_identifier_contexts = IdentifierConsistencyRule._get_sub_identifier_context(data_source)
            for sub_identifier_context in sub_identifier_contexts:
                identifier_contexts.append(sub_identifier_context)

                identifier_reference = sub_identifier_context.identifier_reference
                if identifier_reference.element_name not in identifier_to_sub_identifiers:
                    identifier_to_sub_identifiers[
                        identifier_reference.element_name
                    ] = sub_identifier_context.sub_identifier_names

                elif (
                    identifier_to_sub_identifiers[identifier_reference.element_name]
                    != sub_identifier_context.sub_identifier_names
                ):
                    identifiers_with_inconsistent_sub_identifiers.append(identifier_reference)

        for identifier_reference in identifiers_with_inconsistent_sub_identifiers:
            for identifier_context in identifier_contexts:
                if identifier_context.identifier_reference.element_name == identifier_reference.element_name:
                    issues.append(
                        ValidationWarning(
                            model_object_reference=ValidationIssue.make_object_reference(
                                data_source_name=identifier_context.data_source_name,
                                identifier_name=identifier_context.identifier_reference.element_name,
                            ),
                            message=(
                                f"Identifier '{identifier_reference.element_name}' does not have consistent sub-identifiers "
                                f"throughout the model. In data source '{identifier_context.data_source_name}', "
                                f"it has sub-identifiers {list(identifier_context.sub_identifier_names)}."
                            ),
                        )
                    )
        return issues
