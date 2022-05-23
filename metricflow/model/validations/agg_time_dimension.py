from typing import List

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationIssueType,
    validate_safely,
    ValidationError,
    ValidationIssue,
)
from metricflow.specs import TimeDimensionReference


class AggregationTimeDimensionRule(ModelValidationRule):
    """Checks that the aggregation time dimension for a measure points to a valid time dimension in the data source."""

    @staticmethod
    @validate_safely(whats_being_done="checking aggregation time dimension for data sources in the model")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        for data_source in model.data_sources:
            issues.extend(AggregationTimeDimensionRule._validate_data_source(data_source))

        return issues

    @staticmethod
    def _time_dimension_in_model(time_dimension_reference: TimeDimensionReference, data_source: DataSource) -> bool:
        for dimension in data_source.dimensions:
            if dimension.type == DimensionType.TIME and dimension.name == time_dimension_reference.element_name:
                return True
        return False

    @staticmethod
    @validate_safely(whats_being_done="checking aggregation time dimension for a data source")
    def _validate_data_source(data_source: DataSource) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        for measure in data_source.measures:
            model_object_reference = ValidationIssue.make_object_reference(
                data_source_name=data_source.name, measure_name=measure.name
            )
            if not measure.agg_time_dimension:
                issues.append(
                    ValidationError(
                        model_object_reference=model_object_reference,
                        message=f"In data source '{data_source.name}', measure '{measure.name}' does not have an "
                        f"aggregation time dimension set",
                    )
                )
                continue
            agg_time_dimension_reference = measure.checked_agg_time_dimension
            if not AggregationTimeDimensionRule._time_dimension_in_model(
                time_dimension_reference=agg_time_dimension_reference, data_source=data_source
            ):
                issues.append(
                    ValidationError(
                        model_object_reference=model_object_reference,
                        message=f"In data source '{data_source.name}', measure '{measure.name}' has the aggregation "
                        f"time dimension is set to '{agg_time_dimension_reference.element_name}', "
                        f"which not a valid time dimension in the data source",
                    )
                )

        return issues
