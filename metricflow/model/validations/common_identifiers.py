from typing import Dict, List, Set

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.identifier import Identifier
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationIssue,
    ValidationWarning,
    validate_safely,
    ValidationIssueType,
)
from metricflow.specs import IdentifierReference


class CommonIdentifiersRule(ModelValidationRule):
    """Checks that identifiers exist on more than one data source"""

    @staticmethod
    def _map_data_source_identifiers(data_sources: List[DataSource]) -> Dict[IdentifierReference, Set[str]]:
        """Generate mapping of identifier names to the set of data_sources where it is defined"""
        identifiers_to_data_sources: Dict[IdentifierReference, Set[str]] = {}
        for data_source in data_sources or []:
            for identifier in data_source.identifiers or []:
                if identifier.name in identifiers_to_data_sources:
                    identifiers_to_data_sources[identifier.name].add(data_source.name)
                else:
                    identifiers_to_data_sources[identifier.name] = {data_source.name}
        return identifiers_to_data_sources

    @staticmethod
    @validate_safely(whats_being_done="checking identifier exists on more than one data source")
    def _check_identifier(
        identifier: Identifier,
        data_source: DataSource,
        identifiers_to_data_sources: Dict[IdentifierReference, Set[str]],
    ) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []
        # If the identifier is the dict and if the set of data sources minus this data source is empty,
        # then we warn the user that their identifier will be unused in joins
        if (
            identifier.name in identifiers_to_data_sources
            and len(identifiers_to_data_sources[identifier.name].difference({data_source.name})) == 0
        ):
            issues.append(
                ValidationWarning(
                    model_object_reference=ValidationIssue.make_object_reference(
                        data_source_name=data_source.name, identifier_name=identifier.name.element_name
                    ),
                    message=f"Identifier `{identifier.name.element_name}` "
                    f"only found in one data source `{data_source.name}` "
                    f"which means it will be unused in joins.",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation warning if identifiers are only one one data source")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:
        """Issues a warning for any identifier that is associated with only one data_source"""
        issues = []

        identifiers_to_data_sources = CommonIdentifiersRule._map_data_source_identifiers(model.data_sources)
        for data_source in model.data_sources or []:
            for identifier in data_source.identifiers or []:
                issues += CommonIdentifiersRule._check_identifier(
                    identifier=identifier,
                    data_source=data_source,
                    identifiers_to_data_sources=identifiers_to_data_sources,
                )

        return issues
