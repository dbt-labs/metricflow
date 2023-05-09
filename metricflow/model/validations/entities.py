import logging
from datetime import date
from typing import List, MutableSet

from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.elements.entity import EntityType
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import DataSourceReference
from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    FileContext,
    ModelValidationRule,
    ValidationIssue,
    ValidationError,
    validate_safely,
)
from metricflow.model.validations.validator_helpers import ValidationFutureError

logger = logging.getLogger(__name__)


class NaturalEntityConfigurationRule(ModelValidationRule):
    """Ensures that entities marked as EntityType.NATURAL are configured correctly"""

    @staticmethod
    @validate_safely(
        whats_being_done=(
            "checking that each data source has no more than one natural entity, and that "
            "natural entities are used in the appropriate contexts"
        )
    )
    def _validate_data_source_natural_entities(data_source: SemanticModel) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        context = DataSourceContext(
            file_context=FileContext.from_metadata(metadata=data_source.metadata),
            data_source=DataSourceReference(data_source_name=data_source.name),
        )

        natural_entity_names = set(
            [entity.name for entity in data_source.entities if entity.type is EntityType.NATURAL]
        )
        if len(natural_entity_names) > 1:
            error = ValidationError(
                context=context,
                message=f"Data sources can have at most one natural entity, but data source "
                f"`{data_source.name}` has {len(natural_entity_names)} distinct natural entities set! "
                f"{natural_entity_names}.",
            )
            issues.append(error)
        if natural_entity_names and not [dim for dim in data_source.dimensions if dim.validity_params]:
            error = ValidationError(
                context=context,
                message=f"The use of `natural` entities is currently supported only in conjunction with a validity "
                f"window defined in the set of time dimensions associated with the data source. Data source "
                f"`{data_source.name}` uses a natural entity ({natural_entity_names}) but does not define a "
                f"validity window!",
            )
            issues.append(error)

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that entities marked as EntityType.NATURAL are properly configured")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:
        """Validate entities marked as EntityType.NATURAL"""
        issues: List[ValidationIssue] = []
        for data_source in model.data_sources:
            issues += NaturalEntityConfigurationRule._validate_data_source_natural_entities(data_source=data_source)

        return issues


class OnePrimaryEntityPerDataSourceRule(ModelValidationRule):
    """Ensures that each data source has only one primary entity"""

    @staticmethod
    @validate_safely(whats_being_done="checking data source has only one primary entity")
    def _only_one_primary_entity(data_source: SemanticModel) -> List[ValidationIssue]:
        primary_entity_names: MutableSet[str] = set()
        for entity in data_source.entities or []:
            if entity.type == EntityType.PRIMARY:
                primary_entity_names.add(entity.reference.element_name)

        if len(primary_entity_names) > 1:
            return [
                ValidationFutureError(
                    context=DataSourceContext(
                        file_context=FileContext.from_metadata(metadata=data_source.metadata),
                        data_source=DataSourceReference(data_source_name=data_source.name),
                    ),
                    message=f"Data sources can have only one primary entity. The data source"
                    f" `{data_source.name}` has {len(primary_entity_names)}: {', '.join(primary_entity_names)}",
                    error_date=date(2022, 1, 12),  # Wed January 12th 2022
                )
            ]
        return []

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring each data source has only one primary entity")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues = []

        for data_source in model.data_sources:
            issues += OnePrimaryEntityPerDataSourceRule._only_one_primary_entity(data_source=data_source)

        return issues
