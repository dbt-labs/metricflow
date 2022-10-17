import logging
from typing import List
from metricflow.instances import DataSourceElementReference, DataSourceReference

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    DataSourceElementContext,
    DataSourceElementType,
    FileContext,
    ModelValidationRule,
    ValidationIssueType,
    ValidationError,
    validate_safely,
)
from metricflow.time.time_constants import SUPPORTED_GRANULARITIES

logger = logging.getLogger(__name__)


class DataSourceTimeDimensionWarningsRule(ModelValidationRule):
    """Checks time dimensions in data sources."""

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring time dimensions are defined properly")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []

        for data_source in model.data_sources:
            issues.extend(DataSourceTimeDimensionWarningsRule._validate_data_source(data_source=data_source))
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking validity of the data source's time dimensions")
    def _validate_data_source(data_source: DataSource) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        primary_time_dimensions = []

        for dim in data_source.dimensions:
            context = DataSourceElementContext(
                file_context=FileContext.from_metadata(metadata=data_source.metadata),
                data_source_element=DataSourceElementReference(
                    data_source_name=data_source.name, element_name=dim.name
                ),
                element_type=DataSourceElementType.DIMENSION,
            )

            if dim.type == DimensionType.TIME:
                if dim.type_params is None:
                    continue
                elif dim.type_params.is_primary:
                    primary_time_dimensions.append(dim)
                elif dim.type_params.time_granularity:
                    if dim.type_params.time_granularity not in SUPPORTED_GRANULARITIES:
                        issues.append(
                            ValidationError(
                                context=context,
                                message=f"Unsupported time granularity in time dimension with name: {dim.name}, "
                                f"Please use {[s.value for s in SUPPORTED_GRANULARITIES]}",
                            )
                        )

        if len(primary_time_dimensions) == 0 and len(data_source.measures) > 0:
            issues.append(
                ValidationError(
                    context=DataSourceContext(
                        file_context=FileContext.from_metadata(metadata=data_source.metadata),
                        data_source=DataSourceReference(data_source_name=data_source.name),
                    ),
                    message=f"No primary time dimension in data source with name ({data_source.name}). Please add one",
                )
            )

        if len(primary_time_dimensions) > 1:
            for primary_time_dimension in primary_time_dimensions:
                issues.append(
                    ValidationError(
                        context=DataSourceContext(
                            file_context=FileContext.from_metadata(metadata=data_source.metadata),
                            data_source=DataSourceReference(data_source_name=data_source.name),
                        ),
                        message=f"In data source {data_source.name}, "
                        f"Primary time dimension with name: {primary_time_dimension.name} "
                        f"is one of many defined as primary.",
                    )
                )

        return issues


class DataSourceValidityWindowRule(ModelValidationRule):
    """Checks validity windows in data sources to ensure they comply with runtime requirements"""

    @staticmethod
    @validate_safely(whats_being_done="checking correctness of the time dimension validity parameters in the model")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:
        """Checks the validity param definitions in every data source in the model"""
        issues: List[ValidationIssueType] = []

        for data_source in model.data_sources:
            issues.extend(DataSourceValidityWindowRule._validate_data_source(data_source=data_source))

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="checking the data source's validity parameters for compatibility with runtime requirements"
    )
    def _validate_data_source(data_source: DataSource) -> List[ValidationIssueType]:
        """Runs assertions on data sources with validity parameters set on one or more time dimensions"""

        issues: List[ValidationIssueType] = []

        validity_param_dims = [dim for dim in data_source.dimensions if dim.validity_params is not None]

        if not validity_param_dims:
            return issues

        context = DataSourceContext(
            file_context=FileContext.from_metadata(metadata=data_source.metadata),
            data_source=DataSourceReference(data_source_name=data_source.name),
        )
        requirements = (
            "Data sources using dimension validity params to define a validity window must have exactly two time "
            "dimensions with validity params specified - one marked `is_start` and the other marked `is_end`."
        )
        num_start_dims = len(
            [dim for dim in validity_param_dims if dim.validity_params and dim.validity_params.is_start]
        )
        num_end_dims = len([dim for dim in validity_param_dims if dim.validity_params and dim.validity_params.is_end])

        if len(validity_param_dims) == 1 and num_start_dims == 1 and num_end_dims == 1:
            # Defining a single point window, such as one might find in a daily snapshot table keyed on date,
            # is not currently supported.
            error = ValidationError(
                context=context,
                message=(
                    f"Data source {data_source.name} has a single validity param dimension that defines its window: "
                    f"{validity_param_dims}. This is not a currently supported configuration! {requirements}"
                ),
            )
            issues.append(error)
        elif len(validity_param_dims) != 2:
            error = ValidationError(
                context=context,
                message=(
                    f"Data source {data_source.name} has {len(validity_param_dims)} validity param dimensions defined:"
                    f"{validity_param_dims}. There must be either zero or two! If you wish to define a validity "
                    f"for this data source, please follow these requirements. {requirements}"
                ),
            )
            issues.append(error)
        elif num_start_dims != 1 or num_end_dims != 1:
            # Validity windows must define both a start and an end, and there should be exactly one
            error = ValidationError(
                context=context,
                message=(
                    f"Data source {data_source.name} has two validity param dimensions defined, but does not have "
                    f"exactly one each marked with is_start and is_end: {validity_param_dims}. {requirements}"
                ),
            )
            issues.append(error)

        return issues
