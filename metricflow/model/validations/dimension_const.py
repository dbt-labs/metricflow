from typing import Dict, List
from metricflow.instances import DataSourceElementReference

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.validations.validator_helpers import (
    DataSourceElementContext,
    DataSourceElementType,
    FileContext,
    ModelValidationRule,
    DimensionInvariants,
    ValidationIssueType,
    ValidationError,
    validate_safely,
)
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.specs import DimensionReference
from metricflow.time.time_granularity import TimeGranularity


class DimensionConsistencyRule(ModelValidationRule):
    """Checks for consistent dimension properties in the data sources in a model.

    * Dimensions with the same name should be of the same type.
    * Dimensions with the same name should be either all partitions or not.
    """

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring dimension consistency")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        dimension_to_invariant: Dict[DimensionReference, DimensionInvariants] = {}
        time_dims_to_granularity: Dict[DimensionReference, TimeGranularity] = {}
        issues: List[ValidationIssueType] = []

        for data_source in model.data_sources:
            issues += DimensionConsistencyRule._validate_data_source(
                data_source=data_source, dimension_to_invariant=dimension_to_invariant, update_invariant_dict=True
            )

            for dimension in data_source.dimensions:
                issues += DimensionConsistencyRule._validate_dimension(
                    dimension=dimension,
                    time_dims_to_granularity=time_dims_to_granularity,
                    data_source=data_source,
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
        data_source: DataSource,
    ) -> List[ValidationIssueType]:
        """Checks that time dimensions of the same name that aren't primary have the same time granularity specifications

        Args:
            dimension: the dimension to check
            time_dims_to_granularity: a dict from the dimension to the time granularity it should have
            data_source: the associated data source. Used for generated issue messages
        Throws: MdoValidationError if there is an inconsistent dimension in the data source.
        """
        issues: List[ValidationIssueType] = []
        context = DataSourceElementContext(
            file_context=FileContext.from_metadata(metadata=data_source.metadata),
            data_source_element=DataSourceElementReference(
                data_source_name=data_source.name, element_name=dimension.name
            ),
            element_type=DataSourceElementType.DIMENSION,
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
                            f"Problematic dimension: {dimension.name} in data source with name: "
                            f"`{data_source.name}`. Expected granularity is {expected_granularity.name}.",
                        )
                    )

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="checking that the data source has dimensions consistent with the given invariants"
    )
    def _validate_data_source(
        data_source: DataSource,
        dimension_to_invariant: Dict[DimensionReference, DimensionInvariants],
        update_invariant_dict: bool,
    ) -> List[ValidationIssueType]:
        """Checks that the given data source has dimensions consistent with the given invariants.

        Args:
            data_source: the data source to check
            dimension_to_invariant: a dict from the dimension name to the properties it should have
            update_invariant_dict: whether to insert an entry into the dict if the given dimension name doesn't exist.
        Throws: MdoValidationError if there is an inconsistent dimension in the data source.
        """
        issues: List[ValidationIssueType] = []

        for dimension in data_source.dimensions:
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

            context = DataSourceElementContext(
                file_context=FileContext.from_metadata(metadata=data_source.metadata),
                data_source_element=DataSourceElementReference(
                    data_source_name=data_source.name, element_name=dimension.name
                ),
                element_type=DataSourceElementType.DIMENSION,
            )

            if dimension_invariant.type != dimension.type:
                issues.append(
                    ValidationError(
                        context=context,
                        message=f"In data source `{data_source.name}`, type conflict for dimension `{dimension.name}` "
                        f"- already in model as type `{dimension_invariant.type}` but got `{dimension.type}`",
                    )
                )
            if dimension_invariant.is_partition != is_partition:
                issues.append(
                    ValidationError(
                        context=context,
                        message=f"In data source `{data_source.name}, conflicting is_partition attribute for dimension "
                        f"`{dimension.reference}` - already in model"
                        f" with is_partition as `{dimension_invariant.is_partition}` but got "
                        f"`{is_partition}``",
                    )
                )

        return issues
