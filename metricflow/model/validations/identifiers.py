import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import List, MutableSet, Tuple, Sequence, DefaultDict

import more_itertools
from metricflow.instances import DataSourceElementReference, DataSourceReference

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.identifier import Identifier, IdentifierType, CompositeSubIdentifier
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    DataSourceElementContext,
    DataSourceElementType,
    FileContext,
    ModelValidationRule,
    ValidationIssue,
    ValidationError,
    ValidationIssueType,
    validate_safely,
    ValidationWarning,
)
from metricflow.model.validations.validator_helpers import ValidationFutureError
from metricflow.references import IdentifierReference

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
                context = DataSourceElementContext(
                    file_context=FileContext.from_metadata(metadata=data_source.metadata),
                    data_source_element=DataSourceElementReference(
                        data_source_name=data_source.name, element_name=ident.name
                    ),
                    element_type=DataSourceElementType.IDENTIFIER,
                )

                for sub_id in ident.identifiers:
                    if sub_id.ref and (sub_id.name or sub_id.expr):
                        logger.warning(f"Identifier with error is: {ident}")
                        issues.append(
                            ValidationError(
                                context=context,
                                message=f"Both ref and name/expr set in sub identifier of identifier "
                                f"({ident.name}), please set one",
                            )
                        )
                    elif sub_id.ref is not None and sub_id.ref not in [i.name for i in data_source.identifiers]:
                        issues.append(
                            ValidationError(
                                context=context,
                                message=f"Identifier ref must reference an existing identifier by name. "
                                f"No identifier in this data source has name: {sub_id.ref}",
                            )
                        )
                    elif not sub_id.ref and not sub_id.name:
                        issues.append(
                            ValidationError(
                                context=context,
                                message=f"Must provide either name or ref for sub identifier of identifier "
                                f"with name: {ident.reference.element_name}",
                            )
                        )

                    if sub_id.name:
                        for i in data_source.identifiers:
                            if i.name == sub_id.name and i.expr != sub_id.expr:
                                issues.append(
                                    ValidationError(
                                        context=context,
                                        message=f"If sub identifier has same name ({sub_id.name}) "
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
                primary_identifier_names.add(identifier.reference.element_name)

        if len(primary_identifier_names) > 1:
            return [
                ValidationFutureError(
                    context=DataSourceContext(
                        file_context=FileContext.from_metadata(metadata=data_source.metadata),
                        data_source=DataSourceReference(data_source_name=data_source.name),
                    ),
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

    data_source: DataSource
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
                sub_identifier_names.append(sub_identifier.name)
            elif sub_identifier.ref:
                sub_identifier_names.append(sub_identifier.ref)
        return sub_identifier_names

    @staticmethod
    def _get_sub_identifier_context(data_source: DataSource) -> Sequence[SubIdentifierContext]:
        contexts = []
        for identifier in data_source.identifiers or []:
            contexts.append(
                SubIdentifierContext(
                    data_source=data_source,
                    identifier_reference=identifier.reference,
                    sub_identifier_names=tuple(IdentifierConsistencyRule._get_sub_identifier_names(identifier)),
                )
            )
        return contexts

    @staticmethod
    @validate_safely(whats_being_done="running model validation to ensure identifiers have consistent sub-identifiers")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        # build collection of sub-identifier contexts, keyed by identifier name
        identifier_to_sub_identifier_contexts: DefaultDict[str, List[SubIdentifierContext]] = defaultdict(list)
        all_contexts: List[SubIdentifierContext] = list(
            more_itertools.flatten(
                [
                    IdentifierConsistencyRule._get_sub_identifier_context(data_source)
                    for data_source in model.data_sources
                ]
            )
        )
        for context in all_contexts:
            identifier_to_sub_identifier_contexts[context.identifier_reference.element_name].append(context)

        # Filter out anything that has fewer than 2 distinct sub-identifier sets
        invalid_sub_identifier_configurations = dict(
            filter(
                lambda item: len(set([context.sub_identifier_names for context in item[1]])) >= 2,
                identifier_to_sub_identifier_contexts.items(),
            )
        )

        # convert each invalid identifier configuration into a validation warning
        for identifier_name, sub_identifier_contexts in invalid_sub_identifier_configurations.items():
            data_source = sub_identifier_contexts[0].data_source
            issues.append(
                ValidationWarning(
                    context=DataSourceElementContext(
                        file_context=FileContext.from_metadata(metadata=data_source.metadata),
                        data_source_element=DataSourceElementReference(
                            data_source_name=data_source.name, element_name=identifier_name
                        ),
                        element_type=DataSourceElementType.IDENTIFIER,
                    ),
                    message=(
                        f"Identifier '{identifier_name}' does not have consistent sub-identifiers "
                        f"throughout the model: {list(sorted(sub_identifier_contexts, key=lambda x: x.sub_identifier_names))}"
                    ),
                )
            )

        return issues
