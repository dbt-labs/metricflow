import logging
from collections import defaultdict
from typing import List, Dict

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationIssue,
    ValidationIssueType,
    ValidationError,
    validate_safely,
)
from metricflow.specs import MeasureReference
from metricflow.time.time_constants import SUPPORTED_GRANULARITIES

logger = logging.getLogger(__name__)


class DataSourceMeasuresUniqueRule(ModelValidationRule):
    """Checks time dimensions in data sources."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring measures exist in only one configured data source"
    )
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []

        measure_names_to_data_sources: Dict[MeasureReference, List] = defaultdict(list)
        for data_source in model.data_sources:
            for measure in data_source.measures:
                if measure.reference in measure_names_to_data_sources:
                    model_object_reference = ValidationIssue.make_object_reference(
                        data_source_name=data_source.name, measure_name=measure.reference.element_name
                    )
                    issues.append(
                        ValidationError(
                            model_object_reference=model_object_reference,
                            message=f"Found measure with name {measure.reference} in multiple data sources with names "
                            f"({measure_names_to_data_sources[measure.reference]})",
                        )
                    )
                measure_names_to_data_sources[measure.reference].append(data_source.name)

        return issues


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
            model_object_reference = ValidationIssue.make_object_reference(
                data_source_name=data_source.name, dimension_name=dim.reference.element_name
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
                                model_object_reference=model_object_reference,
                                message=f"Unsupported time granularity in time dimension with name: {dim.reference}, "
                                f"Please use {[s.value for s in SUPPORTED_GRANULARITIES]}",
                            )
                        )

        if len(primary_time_dimensions) == 0 and len(data_source.measures) > 0:
            issues.append(
                ValidationError(
                    model_object_reference=ValidationIssue.make_object_reference(data_source_name=data_source.name),
                    message=f"No primary time dimension in data source with name ({data_source.name}). Please add one",
                )
            )

        if len(primary_time_dimensions) > 1:
            for primary_time_dimension in primary_time_dimensions:
                model_object_reference = ValidationIssue.make_object_reference(
                    data_source_name=data_source.name, dimension_name=primary_time_dimension.name
                )
                issues.append(
                    ValidationError(
                        model_object_reference=model_object_reference,
                        message=f"In data source {data_source.name}, "
                        f"Primary time dimension with name: {primary_time_dimension.name} "
                        f"is one of many defined as primary.",
                    )
                )

        return issues
