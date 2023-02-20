from typing import List

from metricflow.instances import EntityElementReference
from dbt.contracts.graph.nodes import Entity
from dbt.contracts.graph.dimensions import DimensionType
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    EntityElementContext,
    EntityElementType,
    FileContext,
    ModelValidationRule,
    ValidationIssueType,
    validate_safely,
    ValidationError,
)
from metricflow.references import TimeDimensionReference


class AggregationTimeDimensionRule(ModelValidationRule):
    """Checks that the aggregation time dimension for a measure points to a valid time dimension in the entity."""

    @staticmethod
    @validate_safely(whats_being_done="checking aggregation time dimension for entities in the model")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        for entity in model.entities:
            issues.extend(AggregationTimeDimensionRule._validate_entity(entity))

        return issues

    @staticmethod
    def _time_dimension_in_model(time_dimension_reference: TimeDimensionReference, entity: Entity) -> bool:
        for dimension in entity.dimensions:
            if dimension.type == DimensionType.TIME and dimension.name == time_dimension_reference.element_name:
                return True
        return False

    @staticmethod
    @validate_safely(whats_being_done="checking aggregation time dimension for a entity")
    def _validate_entity(entity: Entity) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        for measure in entity.measures:
            measure_context = EntityElementContext(
                file_context=FileContext.from_metadata(metadata=entity.metadata),
                entity_element=EntityElementReference(
                    entity_name=entity.name, element_name=measure.name
                ),
                element_type=EntityElementType.MEASURE,
            )
            agg_time_dimension_reference = measure.checked_agg_time_dimension
            if not AggregationTimeDimensionRule._time_dimension_in_model(
                time_dimension_reference=agg_time_dimension_reference, entity=entity
            ):
                issues.append(
                    ValidationError(
                        context=measure_context,
                        message=f"In entity '{entity.name}', measure '{measure.name}' has the aggregation "
                        f"time dimension set to '{agg_time_dimension_reference.element_name}', "
                        f"which is not a valid time dimension in the entity",
                    )
                )

        return issues
