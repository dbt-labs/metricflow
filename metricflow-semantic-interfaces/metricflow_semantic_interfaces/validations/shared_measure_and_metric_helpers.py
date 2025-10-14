from __future__ import annotations

from typing import List, Literal, Optional, Sequence, Union

from typing_extensions import assert_never

from metricflow_semantic_interfaces.protocols import Metric
from metricflow_semantic_interfaces.protocols.measure import (
    Measure,
    MeasureAggregationParameters,
    NonAdditiveDimensionParameters,
)
from metricflow_semantic_interfaces.protocols.semantic_model import SemanticModel
from metricflow_semantic_interfaces.references import (
    MetricModelReference,
    TimeDimensionReference,
)
from metricflow_semantic_interfaces.type_enums import AggregationType, DimensionType
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    SemanticModelElementContext,
    SemanticModelElementReference,
    SemanticModelElementType,
    ValidationContext,
    ValidationError,
    ValidationIssue,
)


class SharedMeasureAndMetricHelpers:
    """Since Simple Metrics can replace Measures, they share a lot of validation logic."""

    @staticmethod
    def validate_non_additive_dimension(  # noqa: D102
        object: Union[Measure, Metric],
        semantic_model: SemanticModel,
        non_additive_dimension: NonAdditiveDimensionParameters,
        agg_time_dimension_reference: TimeDimensionReference,
        # isinstance doesn't play well with Protocols, so we ask callers to pass this explicitly
        object_type_for_errors: Literal["Measure", "Metric"],
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        agg_time_dimension = next(
            (dim for dim in semantic_model.dimensions if agg_time_dimension_reference.element_name == dim.name),
            None,
        )

        def get_context() -> Union[SemanticModelElementContext, MetricContext]:
            if object_type_for_errors == "Metric":
                return SemanticModelElementContext(
                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                    semantic_model_element=SemanticModelElementReference(
                        semantic_model_name=semantic_model.name, element_name=object.name
                    ),
                    element_type=SemanticModelElementType.MEASURE,
                )
            elif object_type_for_errors == "Measure":
                return MetricContext(
                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                    metric=MetricModelReference(metric_name=object.name),
                )
            assert_never(object_type_for_errors)

        if agg_time_dimension is None:
            # Sanity check, should never hit this
            issues.append(
                ValidationError(
                    context=get_context(),
                    message=(
                        f"{object_type_for_errors} '{object.name}' has a agg_time_dimension of "
                        f"{agg_time_dimension_reference.element_name} "
                        f"that is not defined as a dimension in semantic model '{semantic_model.name}'."
                    ),
                )
            )
            return issues

        # Validates that the non_additive_dimension exists as a time dimension in the semantic model
        matching_dimension = next(
            (dim for dim in semantic_model.dimensions if non_additive_dimension.name == dim.name), None
        )
        if matching_dimension is None:
            issues.append(
                ValidationError(
                    context=get_context(),
                    message=(
                        f"{object_type_for_errors} '{object.name}' has a non_additive_dimension with name "
                        f"'{non_additive_dimension.name}' that is not defined as a dimension in semantic "
                        f"model '{semantic_model.name}'."
                    ),
                )
            )
        if matching_dimension:
            # Check that it's a time dimension
            if matching_dimension.type != DimensionType.TIME:
                issues.append(
                    ValidationError(
                        context=get_context(),
                        message=(
                            f"{object_type_for_errors} '{object.name}' has a non_additive_dimension with name"
                            f"'{non_additive_dimension.name}' "
                            f"that is defined as a categorical dimension which is not supported."
                        ),
                    )
                )

            # Validates that the non_additive_dimension time_granularity
            # is >= agg_time_dimension time_granularity
            if (
                matching_dimension.type_params
                and agg_time_dimension.type_params
                and (matching_dimension.type_params.time_granularity != agg_time_dimension.type_params.time_granularity)
            ):
                issues.append(
                    ValidationError(
                        context=get_context(),
                        message=(
                            f"{object_type_for_errors} '{object.name}' has a non_additive_dimension with name "
                            f"'{non_additive_dimension.name}' that has a base time granularity "
                            f"({matching_dimension.type_params.time_granularity.name}) that is not equal to "
                            f"the {object_type_for_errors.lower()}'s agg_time_dimension {agg_time_dimension.name} "
                            f"with a base granularity of ({agg_time_dimension.type_params.time_granularity.name})."
                        ),
                    )
                )

        # Validates that the window_choice is either MIN/MAX
        if non_additive_dimension.window_choice not in {AggregationType.MIN, AggregationType.MAX}:
            issues.append(
                ValidationError(
                    context=get_context(),
                    message=(
                        f"{object_type_for_errors} '{object.name}' has a non_additive_dimension with an invalid "
                        f"'window_choice' of '{non_additive_dimension.window_choice.value}'. "
                        f"Only choices supported are 'min' or 'max'."
                    ),
                )
            )

        # Validates that all window_groupings are entities
        entities_in_semantic_model = {entity.name for entity in semantic_model.entities}
        window_groupings = set(non_additive_dimension.window_groupings)
        intersected_entities = window_groupings.intersection(entities_in_semantic_model)
        if len(intersected_entities) != len(window_groupings):
            issues.append(
                ValidationError(
                    context=get_context(),
                    message=(
                        f"{object_type_for_errors} '{object.name}' has a non_additive_dimension with an invalid "
                        "'window_groupings'. These entities "
                        f"{window_groupings.difference(intersected_entities)} do not exist in the "
                        "semantic model."
                    ),
                )
            )
        return issues

    @staticmethod
    def validate_expr_for_count_aggregation(  # noqa: D102
        context: ValidationContext,
        object_name: str,
        object_type: Literal["Measure", "Metric"],
        agg_type: AggregationType,
        expr: Optional[str],
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if agg_type != AggregationType.COUNT:
            return []
        if expr is None:
            issues.append(
                ValidationError(
                    context=context,
                    message=(
                        f"{object_type} '{object_name}' uses a COUNT aggregation, which requires an expr to be "
                        "provided. Provide 'expr: 1' if a count of all rows is desired."
                    ),
                )
            )
        if expr and expr.lower().startswith("distinct "):
            # TODO: Expand this to include SUM and potentially AVG agg types as well
            # Note expansion of this guard requires the addition of sum_distinct and avg_distinct agg types
            # or else an adjustment to the error message below.
            issues.append(
                ValidationError(
                    context=context,
                    message=(
                        f"{object_type} '{object_name}' uses a '{agg_type.value}' aggregation with a DISTINCT "
                        f"expr: '{expr}'. This is not supported as it effectively converts an additive "
                        f"{object_type.lower()} into a non-additive one, and this could cause certain queries to "
                        f"return incorrect results. Please use the {agg_type.value}_distinct aggregation type."
                    ),
                )
            )
        return issues

    @staticmethod
    def validate_percentile_arguments(  # noqa: D102
        context: ValidationContext,
        object_name: str,
        object_type: Literal["Measure", "Metric"],
        agg_type: Optional[AggregationType],
        agg_params: Optional[MeasureAggregationParameters],
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if agg_type == AggregationType.PERCENTILE:
            if agg_params is None or agg_params.percentile is None:
                issues.append(
                    ValidationError(
                        context=context,
                        message=(
                            f"{object_type} '{object_name}' uses a PERCENTILE aggregation, which requires "
                            "agg_params.percentile to be provided."
                        ),
                    )
                )
            elif agg_params.percentile <= 0 or agg_params.percentile >= 1:
                issues.append(
                    ValidationError(
                        context=context,
                        message=(
                            f"Percentile aggregation parameter for {object_type.lower()} '{object_name}' is "
                            f"'{agg_params.percentile}', but must be between 0 and 1 (non-inclusive). "
                            "For example, to indicate the 65th percentile value, set 'percentile: 0.65'. "
                            "For percentile values of 0, please use MIN, for percentile values of 1, please "
                            "use MAX."
                        ),
                    )
                )
        elif agg_type == AggregationType.MEDIAN:
            if agg_params:
                if agg_params.percentile is not None and agg_params.percentile != 0.5:
                    issues.append(
                        ValidationError(
                            context=context,
                            message=f"{object_type} '{object_name}' uses a MEDIAN aggregation, while percentile is "
                            f"set to '{agg_params.percentile}', a conflicting value. Please remove "
                            "the parameter or set to '0.5'.",
                        )
                    )
                if agg_params.use_discrete_percentile:
                    issues.append(
                        ValidationError(
                            context=context,
                            message=f"{object_type} '{object_name}' uses a MEDIAN aggregation, while "
                            "use_discrete_percentile is set to true. Please remove the parameter or set "
                            "to False.",
                        )
                    )
        elif agg_params and (
            agg_params.percentile or agg_params.use_discrete_percentile or agg_params.use_approximate_percentile
        ):
            wrong_params = []
            if agg_params.percentile:
                wrong_params.append("percentile")
            if agg_params.use_discrete_percentile:
                wrong_params.append("use_discrete_percentile")
            if agg_params.use_approximate_percentile:
                wrong_params.append("use_approximate_percentile")

            wrong_params_str = ", ".join(wrong_params)
            agg_type_str = agg_type.value if agg_type else "None"

            issues.append(
                ValidationError(
                    context=context,
                    message=(
                        f"{object_type} '{object_name}' with aggregation '{agg_type_str}' uses agg_params "
                        f"({wrong_params_str}) only relevant to Percentile {object_type.lower()}s."
                    ),
                )
            )
        return issues
