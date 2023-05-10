from typing import Dict, List, Set

from dbt_semantic_interfaces.objects.data_source import DataSource
from dbt_semantic_interfaces.objects.elements.entity import Entity
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import DataSourceElementReference, EntityReference
from metricflow.model.validations.validator_helpers import (
    DataSourceElementContext,
    DataSourceElementType,
    FileContext,
    ModelValidationRule,
    ValidationWarning,
    validate_safely,
    ValidationIssue,
)


class CommonEntitysRule(ModelValidationRule):
    """Checks that entities exist on more than one data source"""

    @staticmethod
    def _map_data_source_entities(data_sources: List[DataSource]) -> Dict[EntityReference, Set[str]]:
        """Generate mapping of entity names to the set of data_sources where it is defined"""
        entities_to_data_sources: Dict[EntityReference, Set[str]] = {}
        for data_source in data_sources or []:
            for entity in data_source.entities or []:
                if entity.reference in entities_to_data_sources:
                    entities_to_data_sources[entity.reference].add(data_source.name)
                else:
                    entities_to_data_sources[entity.reference] = {data_source.name}
        return entities_to_data_sources

    @staticmethod
    @validate_safely(whats_being_done="checking entity exists on more than one data source")
    def _check_entity(
        entity: Entity,
        data_source: DataSource,
        entities_to_data_sources: Dict[EntityReference, Set[str]],
    ) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        # If the entity is the dict and if the set of data sources minus this data source is empty,
        # then we warn the user that their entity will be unused in joins
        if (
            entity.reference in entities_to_data_sources
            and len(entities_to_data_sources[entity.reference].difference({data_source.name})) == 0
        ):
            issues.append(
                ValidationWarning(
                    context=DataSourceElementContext(
                        file_context=FileContext.from_metadata(metadata=data_source.metadata),
                        data_source_element=DataSourceElementReference(
                            data_source_name=data_source.name, element_name=entity.name
                        ),
                        element_type=DataSourceElementType.ENTITY,
                    ),
                    message=f"Entity `{entity.reference.element_name}` "
                    f"only found in one data source `{data_source.name}` "
                    f"which means it will be unused in joins.",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation warning if entities are only one one data source")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:
        """Issues a warning for any entity that is associated with only one data_source"""
        issues = []

        entities_to_data_sources = CommonEntitysRule._map_data_source_entities(model.data_sources)
        for data_source in model.data_sources or []:
            for entity in data_source.entities or []:
                issues += CommonEntitysRule._check_entity(
                    entity=entity,
                    data_source=data_source,
                    entities_to_data_sources=entities_to_data_sources,
                )

        return issues
