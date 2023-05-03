import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import List, MutableSet, Tuple, Sequence, DefaultDict

import more_itertools

from dbt_semantic_interfaces.objects.data_source import DataSource
from dbt_semantic_interfaces.objects.elements.entity import Entity, EntityType, CompositeSubEntity
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import (
    DataSourceElementReference,
    DataSourceReference,
    EntityReference,
)
from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    DataSourceElementContext,
    DataSourceElementType,
    FileContext,
    ModelValidationRule,
    ValidationIssue,
    ValidationError,
    validate_safely,
    ValidationWarning,
)
from metricflow.model.validations.validator_helpers import ValidationFutureError

logger = logging.getLogger(__name__)


class EntityConfigRule(ModelValidationRule):
    """Checks that data source entities are valid"""

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring entities are valid")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues = []
        for data_source in model.data_sources:
            issues += EntityConfigRule._validate_data_source_entities(data_source=data_source)
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the data source's entities are valid")
    def _validate_data_source_entities(data_source: DataSource) -> List[ValidationIssue]:
        """Checks validity of composite entities"""
        issues: List[ValidationIssue] = []
        for entity in data_source.identifiers:
            if entity.entities:
                context = DataSourceElementContext(
                    file_context=FileContext.from_metadata(metadata=data_source.metadata),
                    data_source_element=DataSourceElementReference(
                        data_source_name=data_source.name, element_name=entity.name
                    ),
                    element_type=DataSourceElementType.IDENTIFIER,
                )

                for sub_entity in entity.entities:
                    if sub_entity.ref and (sub_entity.name or sub_entity.expr):
                        logger.warning(f"Entity with error is: {entity}")
                        issues.append(
                            ValidationError(
                                context=context,
                                message=f"Both ref and name/expr set in sub entity of entity "
                                f"({entity.name}), please set one",
                            )
                        )
                    elif sub_entity.ref is not None and sub_entity.ref not in [i.name for i in data_source.identifiers]:
                        issues.append(
                            ValidationError(
                                context=context,
                                message=f"Entity ref must reference an existing entity by name. "
                                f"No entity in this data source has name: {sub_entity.ref}",
                            )
                        )
                    elif not sub_entity.ref and not sub_entity.name:
                        issues.append(
                            ValidationError(
                                context=context,
                                message=f"Must provide either name or ref for sub entity of entity "
                                f"with name: {entity.reference.element_name}",
                            )
                        )

                    if sub_entity.name:
                        for i in data_source.identifiers:
                            if i.name == sub_entity.name and i.expr != sub_entity.expr:
                                issues.append(
                                    ValidationError(
                                        context=context,
                                        message=f"If sub entity has same name ({sub_entity.name}) "
                                        f"as an existing Entity they must have the same expr",
                                    )
                                )
                                break

        return issues


class NaturalEntityConfigurationRule(ModelValidationRule):
    """Ensures that identifiers marked as EntityType.NATURAL are configured correctly"""

    @staticmethod
    @validate_safely(
        whats_being_done=(
            "checking that each data source has no more than one natural identifier, and that "
            "natural identifiers are used in the appropriate contexts"
        )
    )
    def _validate_data_source_natural_entities(data_source: DataSource) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        context = DataSourceContext(
            file_context=FileContext.from_metadata(metadata=data_source.metadata),
            data_source=DataSourceReference(data_source_name=data_source.name),
        )

        natural_entity_names = set(
            [identifier.name for identifier in data_source.identifiers if identifier.type is EntityType.NATURAL]
        )
        if len(natural_entity_names) > 1:
            error = ValidationError(
                context=context,
                message=f"Data sources can have at most one natural identifier, but data source "
                f"`{data_source.name}` has {len(natural_entity_names)} distinct natural identifiers set! "
                f"{natural_entity_names}.",
            )
            issues.append(error)
        if natural_entity_names and not [dim for dim in data_source.dimensions if dim.validity_params]:
            error = ValidationError(
                context=context,
                message=f"The use of `natural` identifiers is currently supported only in conjunction with a validity "
                f"window defined in the set of time dimensions associated with the data source. Data source "
                f"`{data_source.name}` uses a natural identifier ({natural_entity_names}) but does not define a "
                f"validity window!",
            )
            issues.append(error)

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that identifiers marked as EntityType.NATURAL are properly configured")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:
        """Validate identifiers marked as EntityType.NATURAL"""
        issues: List[ValidationIssue] = []
        for data_source in model.data_sources:
            issues += NaturalEntityConfigurationRule._validate_data_source_natural_entities(data_source=data_source)

        return issues


class OnePrimaryEntityPerDataSourceRule(ModelValidationRule):
    """Ensures that each data source has only one primary identifier"""

    @staticmethod
    @validate_safely(whats_being_done="checking data source has only one primary identifier")
    def _only_one_primary_entity(data_source: DataSource) -> List[ValidationIssue]:
        primary_entity_names: MutableSet[str] = set()
        for identifier in data_source.identifiers or []:
            if identifier.type == EntityType.PRIMARY:
                primary_entity_names.add(identifier.reference.element_name)

        if len(primary_entity_names) > 1:
            return [
                ValidationFutureError(
                    context=DataSourceContext(
                        file_context=FileContext.from_metadata(metadata=data_source.metadata),
                        data_source=DataSourceReference(data_source_name=data_source.name),
                    ),
                    message=f"Data sources can have only one primary identifier. The data source"
                    f" `{data_source.name}` has {len(primary_entity_names)}: {', '.join(primary_entity_names)}",
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
            issues += OnePrimaryEntityPerDataSourceRule._only_one_primary_entity(data_source=data_source)

        return issues


@dataclass(frozen=True)
class SubEntityContext:
    """Organizes the context behind entity and their sub-entity."""

    data_source: DataSource
    entity_reference: EntityReference
    sub_entity_names: Tuple[str, ...]


class EntityConsistencyRule(ModelValidationRule):
    """Checks entities with the same name are defined with the same set of sub-entities in all data sources"""

    @staticmethod
    def _get_sub_entity_names(entity: Entity) -> Sequence[str]:
        sub_entity_names = []
        sub_entity: CompositeSubEntity
        for sub_entity in entity.entities or []:
            if sub_entity.name:
                sub_entity_names.append(sub_entity.name)
            elif sub_entity.ref:
                sub_entity_names.append(sub_entity.ref)
        return sub_entity_names

    @staticmethod
    def _get_sub_entity_context(data_source: DataSource) -> Sequence[SubEntityContext]:
        contexts = []
        for entity in data_source.identifiers or []:
            contexts.append(
                SubEntityContext(
                    data_source=data_source,
                    entity_reference=entity.reference,
                    sub_entity_names=tuple(EntityConsistencyRule._get_sub_entity_names(entity)),
                )
            )
        return contexts

    @staticmethod
    @validate_safely(whats_being_done="running model validation to ensure entities have consistent sub-entities")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        # build collection of sub-entity contexts, keyed by entity name
        entity_to_sub_entity_contexts: DefaultDict[str, List[SubEntityContext]] = defaultdict(list)
        all_contexts: List[SubEntityContext] = list(
            tuple(
                more_itertools.flatten(
                    [EntityConsistencyRule._get_sub_entity_context(data_source) for data_source in model.data_sources]
                )
            )
        )
        for context in all_contexts:
            entity_to_sub_entity_contexts[context.entity_reference.element_name].append(context)

        # Filter out anything that has fewer than 2 distinct sub-entity sets
        invalid_sub_entity_configurations = dict(
            filter(
                lambda item: len(set([context.sub_entity_names for context in item[1]])) >= 2,
                entity_to_sub_entity_contexts.items(),
            )
        )

        # convert each invalid entity configuration into a validation warning
        for entity_name, sub_entity_contexts in invalid_sub_entity_configurations.items():
            data_source = sub_entity_contexts[0].data_source
            issues.append(
                ValidationWarning(
                    context=DataSourceElementContext(
                        file_context=FileContext.from_metadata(metadata=data_source.metadata),
                        data_source_element=DataSourceElementReference(
                            data_source_name=data_source.name, element_name=entity_name
                        ),
                        element_type=DataSourceElementType.IDENTIFIER,
                    ),
                    message=(
                        f"Entity '{entity_name}' does not have consistent sub-entities "
                        f"throughout the model: {list(sorted(sub_entity_contexts, key=lambda x: x.sub_entity_names))}"
                    ),
                )
            )

        return issues
