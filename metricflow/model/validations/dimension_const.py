from typing import Dict, List
from metricflow.instances import EntityElementReference

from dbt.contracts.graph.nodes import Entity
from dbt.contracts.graph.dimensions import Dimension, DimensionType
from metricflow.model.validations.validator_helpers import (
    EntityElementContext,
    EntityElementType,
    FileContext,
    ModelValidationRule,
    DimensionInvariants,
    ValidationIssueType,
    ValidationError,
    validate_safely,
)
from dbt.contracts.graph.manifest import UserConfiguredModel
from dbt.semantic.references import DimensionReference
from metricflow.time.time_granularity import TimeGranularity


class DimensionConsistencyRule(ModelValidationRule):
    """Checks for consistent dimension properties in the entities in a model.

    * Dimensions with the same name should be of the same type.
    * Dimensions with the same name should be either all partitions or not.
    """

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring dimension consistency")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        dimension_to_invariant: Dict[DimensionReference, DimensionInvariants] = {}
        time_dims_to_granularity: Dict[DimensionReference, TimeGranularity] = {}
        issues: List[ValidationIssueType] = []

        for entity in model.entities:
            issues += DimensionConsistencyRule._validate_entity(
                entity=entity, dimension_to_invariant=dimension_to_invariant, update_invariant_dict=True
            )

            for dimension in entity.dimensions:
                issues += DimensionConsistencyRule._validate_dimension(
                    dimension=dimension,
                    time_dims_to_granularity=time_dims_to_granularity,
                    entity=entity,
                )
        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="checking that time dimensions of the same name that are not primary "
        "have the same time granularity specifications"
    )
    def _validate_dimension(
        dimension: Dimension,
        time_dims_to_granularity: Dict[DimensionReference, TimeGranularity],
        entity: Entity,
    ) -> List[ValidationIssueType]:
        """Checks that time dimensions of the same name that aren't primary have the same time granularity specifications

        Args:
            dimension: the dimension to check
            time_dims_to_granularity: a dict from the dimension to the time granularity it should have
            entity: the associated entity. Used for generated issue messages
        Throws: MdoValidationError if there is an inconsistent dimension in the entity.
        """
        issues: List[ValidationIssueType] = []
        context = EntityElementContext(
            file_context=FileContext.from_metadata(metadata=entity.metadata),
            entity_element=EntityElementReference(
                entity_name=entity.name, element_name=dimension.name
            ),
            element_type=EntityElementType.DIMENSION,
        )

        if dimension.type == DimensionType.TIME:
            if dimension.reference not in time_dims_to_granularity and dimension.type_params:
                time_dims_to_granularity[dimension.reference] = dimension.type_params.time_granularity

                # The primary time dimension can be of different time granularities, so don't check for it.
                if (
                    dimension.type_params is not None
                    and not dimension.type_params.is_primary
                    and dimension.type_params.time_granularity != time_dims_to_granularity[dimension.reference]
                ):
                    expected_granularity = time_dims_to_granularity[dimension.reference]
                    issues.append(
                        ValidationError(
                            context=context,
                            message=f"Time granularity must be the same for time dimensions with the same name. "
                            f"Problematic dimension: {dimension.name} in entity with name: "
                            f"`{entity.name}`. Expected granularity is {expected_granularity.name}.",
                        )
                    )

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="checking that the entity has dimensions consistent with the given invariants"
    )
    def _validate_entity(
        entity: Entity,
        dimension_to_invariant: Dict[DimensionReference, DimensionInvariants],
        update_invariant_dict: bool,
    ) -> List[ValidationIssueType]:
        """Checks that the given entity has dimensions consistent with the given invariants.

        Args:
            entity: the entity to check
            dimension_to_invariant: a dict from the dimension name to the properties it should have
            update_invariant_dict: whether to insert an entry into the dict if the given dimension name doesn't exist.
        Throws: MdoValidationError if there is an inconsistent dimension in the entity.
        """
        issues: List[ValidationIssueType] = []

        for dimension in entity.dimensions:
            dimension_invariant = dimension_to_invariant.get(dimension.reference)

            if dimension_invariant is None:
                if update_invariant_dict:
                    dimension_invariant = DimensionInvariants(dimension.type, dimension.is_partition or False)
                    dimension_to_invariant[dimension.reference] = dimension_invariant
                    continue
                # TODO: Can't check for unknown dimensions easily as the name follows <id>__<name> format.
                # e.g. user__created_at
                continue

            # is_partition might not be specified in the configs, so default to False.
            is_partition = dimension.is_partition or False

            context = EntityElementContext(
                file_context=FileContext.from_metadata(metadata=entity.metadata),
                entity_element=EntityElementReference(
                    entity_name=entity.name, element_name=dimension.name
                ),
                element_type=EntityElementType.DIMENSION,
            )

            if dimension_invariant.type != dimension.type:
                issues.append(
                    ValidationError(
                        context=context,
                        message=f"In entity `{entity.name}`, type conflict for dimension `{dimension.name}` "
                        f"- already in model as type `{dimension_invariant.type}` but got `{dimension.type}`",
                    )
                )
            if dimension_invariant.is_partition != is_partition:
                issues.append(
                    ValidationError(
                        context=context,
                        message=f"In entity `{entity.name}, conflicting is_partition attribute for dimension "
                        f"`{dimension.reference}` - already in model"
                        f" with is_partition as `{dimension_invariant.is_partition}` but got "
                        f"`{is_partition}``",
                    )
                )

        return issues
