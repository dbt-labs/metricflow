from typing import List

from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.elements.dimension import DimensionType
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import SemanticModelElementReference, TimeDimensionReference
from metricflow.model.validations.validator_helpers import (
    DataSourceElementContext,
    DataSourceElementType,
    FileContext,
    ModelValidationRule,
    ValidationIssue,
    validate_safely,
    ValidationError,
)


class AggregationTimeDimensionRule(ModelValidationRule):
    """Checks that the aggregation time dimension for a measure points to a valid time dimension in the data source."""

    @staticmethod
    @validate_safely(whats_being_done="checking aggregation time dimension for data sources in the model")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        for data_source in model.data_sources:
            issues.extend(AggregationTimeDimensionRule._validate_semantic_model(data_source))

        return issues

    @staticmethod
    def _time_dimension_in_model(time_dimension_reference: TimeDimensionReference, data_source: SemanticModel) -> bool:
        for dimension in data_source.dimensions:
            if dimension.type == DimensionType.TIME and dimension.name == time_dimension_reference.element_name:
                return True
        return False

    @staticmethod
    @validate_safely(whats_being_done="checking aggregation time dimension for a data source")
    def _validate_semantic_model(data_source: SemanticModel) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for measure in data_source.measures:
            measure_context = DataSourceElementContext(
                file_context=FileContext.from_metadata(metadata=data_source.metadata),
                semantic_model_element=SemanticModelElementReference(
                    semantic_model_name=data_source.name, element_name=measure.name
                ),
                element_type=DataSourceElementType.MEASURE,
            )
            agg_time_dimension_reference = measure.checked_agg_time_dimension
            if not AggregationTimeDimensionRule._time_dimension_in_model(
                time_dimension_reference=agg_time_dimension_reference, data_source=data_source
            ):
                issues.append(
                    ValidationError(
                        context=measure_context,
                        message=f"In data source '{data_source.name}', measure '{measure.name}' has the aggregation "
                        f"time dimension set to '{agg_time_dimension_reference.element_name}', "
                        f"which is not a valid time dimension in the data source",
                    )
                )

        return issues
