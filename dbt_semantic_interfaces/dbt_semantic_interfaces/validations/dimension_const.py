from typing import Dict, List

from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.elements.dimension import Dimension, DimensionType
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticModelElementContext,
    SemanticModelElementType,
    FileContext,
    ModelValidationRule,
    DimensionInvariants,
    ValidationIssue,
    ValidationError,
    validate_safely,
)
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import SemanticModelElementReference, DimensionReference
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity


class DimensionConsistencyRule(ModelValidationRule):
    """Checks for consistent dimension properties in the semantic models in a model.

    * Dimensions with the same name should be of the same type.
    * Dimensions with the same name should be either all partitions or not.
    """

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring dimension consistency")
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
        dimension_to_invariant: Dict[DimensionReference, DimensionInvariants] = {}
        time_dims_to_granularity: Dict[DimensionReference, TimeGranularity] = {}
        issues: List[ValidationIssue] = []

        for semantic_model in model.semantic_models:
            issues += DimensionConsistencyRule._validate_semantic_model(
                semantic_model=semantic_model, dimension_to_invariant=dimension_to_invariant, update_invariant_dict=True
            )

            for dimension in semantic_model.dimensions:
                issues += DimensionConsistencyRule._validate_dimension(
                    dimension=dimension,
                    time_dims_to_granularity=time_dims_to_granularity,
                    semantic_model=semantic_model,
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
        semantic_model: SemanticModel,
    ) -> List[ValidationIssue]:
        """Checks that time dimensions of the same name that aren't primary have the same time granularity specifications

        Args:
            dimension: the dimension to check
            time_dims_to_granularity: a dict from the dimension to the time granularity it should have
            semantic_model: the associated semantic model. Used for generated issue messages
        Throws: MdoValidationError if there is an inconsistent dimension in the semantic model.
        """
        issues: List[ValidationIssue] = []
        context = SemanticModelElementContext(
            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
            semantic_model_element=SemanticModelElementReference(
                semantic_model_name=semantic_model.name, element_name=dimension.name
            ),
            element_type=SemanticModelElementType.DIMENSION,
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
                            f"Problematic dimension: {dimension.name} in semantic model with name: "
                            f"`{semantic_model.name}`. Expected granularity is {expected_granularity.name}.",
                        )
                    )

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="checking that the semantic model has dimensions consistent with the given invariants"
    )
    def _validate_semantic_model(
        semantic_model: SemanticModel,
        dimension_to_invariant: Dict[DimensionReference, DimensionInvariants],
        update_invariant_dict: bool,
    ) -> List[ValidationIssue]:
        """Checks that the given semantic model has dimensions consistent with the given invariants.

        Args:
            semantic_model: the semantic model to check
            dimension_to_invariant: a dict from the dimension name to the properties it should have
            update_invariant_dict: whether to insert an entry into the dict if the given dimension name doesn't exist.
        Throws: MdoValidationError if there is an inconsistent dimension in the semantic model.
        """
        issues: List[ValidationIssue] = []

        for dimension in semantic_model.dimensions:
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

            context = SemanticModelElementContext(
                file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                semantic_model_element=SemanticModelElementReference(
                    semantic_model_name=semantic_model.name, element_name=dimension.name
                ),
                element_type=SemanticModelElementType.DIMENSION,
            )

            if dimension_invariant.type != dimension.type:
                issues.append(
                    ValidationError(
                        context=context,
                        message=f"In semantic model `{semantic_model.name}`, type conflict for dimension `{dimension.name}` "
                        f"- already in model as type `{dimension_invariant.type}` but got `{dimension.type}`",
                    )
                )
            if dimension_invariant.is_partition != is_partition:
                issues.append(
                    ValidationError(
                        context=context,
                        message=f"In semantic model `{semantic_model.name}, conflicting is_partition attribute for dimension "
                        f"`{dimension.reference}` - already in model"
                        f" with is_partition as `{dimension_invariant.is_partition}` but got "
                        f"`{is_partition}``",
                    )
                )

        return issues
