from collections import defaultdict
from typing import DefaultDict, Dict, List, Set

from more_itertools import bucket

from dbt_semantic_interfaces.objects.aggregation_type import AggregationType
from dbt_semantic_interfaces.objects.elements.dimension import DimensionType
from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import MetricModelReference, MeasureReference
from metricflow.model.validations.unique_valid_name import UniqueAndValidNameRule
from metricflow.model.validations.validator_helpers import (
    SemanticModelElementContext,
    SemanticModelElementReference,
    SemanticModelElementType,
    FileContext,
    MetricContext,
    ModelValidationRule,
    ValidationIssue,
    ValidationError,
    ValidationWarning,
    validate_safely,
)


class SemanticModelMeasuresUniqueRule(ModelValidationRule):
    """Asserts all measure names are unique across the model."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring measures exist in only one configured semantic model"
    )
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        measure_references_to_semantic_models: Dict[MeasureReference, List] = defaultdict(list)
        for semantic_model in model.semantic_models:
            for measure in semantic_model.measures:
                if measure.reference in measure_references_to_semantic_models:
                    issues.append(
                        ValidationError(
                            context=SemanticModelElementContext(
                                file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                                semantic_model_element=SemanticModelElementReference(
                                    semantic_model_name=semantic_model.name, element_name=measure.name
                                ),
                                element_type=SemanticModelElementType.MEASURE,
                            ),
                            message=f"Found measure with name {measure.name} in multiple semantic models with names "
                            f"({measure_references_to_semantic_models[measure.reference]})",
                        )
                    )
                measure_references_to_semantic_models[measure.reference].append(semantic_model.name)

        return issues


class MeasuresNonAdditiveDimensionRule(ModelValidationRule):
    """Checks that the measure's non_additive_dimensions are properly defined."""

    @staticmethod
    @validate_safely(whats_being_done="ensuring that a measure's non_additive_dimensions is valid")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        for semantic_model in model.semantic_models or []:
            for measure in semantic_model.measures:
                non_additive_dimension = measure.non_additive_dimension
                if non_additive_dimension is None:
                    continue
                agg_time_dimension = next(
                    (
                        dim
                        for dim in semantic_model.dimensions
                        if measure.checked_agg_time_dimension.element_name == dim.name
                    ),
                    None,
                )
                if agg_time_dimension is None:
                    # Sanity check, should never hit this
                    issues.append(
                        ValidationError(
                            context=SemanticModelElementContext(
                                file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                                semantic_model_element=SemanticModelElementReference(
                                    semantic_model_name=semantic_model.name, element_name=measure.name
                                ),
                                element_type=SemanticModelElementType.MEASURE,
                            ),
                            message=(
                                f"Measure '{measure.name}' has a agg_time_dimension of {measure.checked_agg_time_dimension.element_name} "
                                f"that is not defined as a dimension in semantic model '{semantic_model.name}'."
                            ),
                        )
                    )
                    continue

                # Validates that the non_additive_dimension exists as a time dimension in the semantic model
                matching_dimension = next(
                    (dim for dim in semantic_model.dimensions if non_additive_dimension.name == dim.name), None
                )
                if matching_dimension is None:
                    issues.append(
                        ValidationError(
                            context=SemanticModelElementContext(
                                file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                                semantic_model_element=SemanticModelElementReference(
                                    semantic_model_name=semantic_model.name, element_name=measure.name
                                ),
                                element_type=SemanticModelElementType.MEASURE,
                            ),
                            message=(
                                f"Measure '{measure.name}' has a non_additive_dimension with name '{non_additive_dimension.name}' "
                                f"that is not defined as a dimension in semantic model '{semantic_model.name}'."
                            ),
                        )
                    )
                if matching_dimension:
                    # Check that it's a time dimension
                    if matching_dimension.type != DimensionType.TIME:
                        issues.append(
                            ValidationError(
                                context=SemanticModelElementContext(
                                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                                    semantic_model_element=SemanticModelElementReference(
                                        semantic_model_name=semantic_model.name, element_name=measure.name
                                    ),
                                    element_type=SemanticModelElementType.MEASURE,
                                ),
                                message=(
                                    f"Measure '{measure.name}' has a non_additive_dimension with name '{non_additive_dimension.name}' "
                                    f"that is defined as a categorical dimension which is not supported."
                                ),
                            )
                        )

                    # Validates that the non_additive_dimension time_granularity is >= agg_time_dimension time_granularity
                    if (
                        matching_dimension.type_params
                        and agg_time_dimension.type_params
                        and (
                            matching_dimension.type_params.time_granularity
                            != agg_time_dimension.type_params.time_granularity
                        )
                    ):
                        issues.append(
                            ValidationError(
                                context=SemanticModelElementContext(
                                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                                    semantic_model_element=SemanticModelElementReference(
                                        semantic_model_name=semantic_model.name, element_name=measure.name
                                    ),
                                    element_type=SemanticModelElementType.MEASURE,
                                ),
                                message=(
                                    f"Measure '{measure.name}' has a non_additive_dimension with name '{non_additive_dimension.name}' that has "
                                    f"a base time granularity ({matching_dimension.type_params.time_granularity.name}) that is not equal to the measure's "
                                    f"agg_time_dimension {agg_time_dimension.name} with a base granularity of ({agg_time_dimension.type_params.time_granularity.name})."
                                ),
                            )
                        )

                # Validates that the window_choice is either MIN/MAX
                if non_additive_dimension.window_choice not in {AggregationType.MIN, AggregationType.MAX}:
                    issues.append(
                        ValidationError(
                            context=SemanticModelElementContext(
                                file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                                semantic_model_element=SemanticModelElementReference(
                                    semantic_model_name=semantic_model.name, element_name=measure.name
                                ),
                                element_type=SemanticModelElementType.MEASURE,
                            ),
                            message=(
                                f"Measure '{measure.name}' has a non_additive_dimension with an invalid 'window_choice' of '{non_additive_dimension.window_choice.value}'. "
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
                            context=SemanticModelElementContext(
                                file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                                semantic_model_element=SemanticModelElementReference(
                                    semantic_model_name=semantic_model.name, element_name=measure.name
                                ),
                                element_type=SemanticModelElementType.MEASURE,
                            ),
                            message=(
                                f"Measure '{measure.name}' has a non_additive_dimension with an invalid 'window_groupings'. "
                                f"These entities {window_groupings.difference(intersected_entities)} do not exist in the semantic model."
                            ),
                        )
                    )

        return issues


class CountAggregationExprRule(ModelValidationRule):
    """Checks that COUNT measures have an expr provided."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring expr exist for measures with count aggregation"
    )
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        for semantic_model in model.semantic_models:
            for measure in semantic_model.measures:
                context = SemanticModelElementContext(
                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                    semantic_model_element=SemanticModelElementReference(
                        semantic_model_name=semantic_model.name, element_name=measure.name
                    ),
                    element_type=SemanticModelElementType.MEASURE,
                )
                if measure.agg == AggregationType.COUNT and measure.expr is None:
                    issues.append(
                        ValidationError(
                            context=context,
                            message=(
                                f"Measure '{measure.name}' uses a COUNT aggregation, which requires an expr to be provided. "
                                f"Provide 'expr: 1' if a count of all rows is desired."
                            ),
                        )
                    )
                if (
                    measure.agg == AggregationType.COUNT
                    and measure.expr
                    and measure.expr.lower().startswith("distinct ")
                ):
                    # TODO: Expand this to include SUM and potentially AVG agg types as well
                    # Note expansion of this guard requires the addition of sum_distinct and avg_distinct agg types
                    # or else an adjustment to the error message below.
                    issues.append(
                        ValidationError(
                            context=context,
                            message=(
                                f"Measure '{measure.name}' uses a '{measure.agg.value}' aggregation with a DISTINCT expr: "
                                f"'{measure.expr}. This is not supported, as it effectively converts an additive "
                                f"measure into a non-additive one, and this could cause certain queries to return "
                                f"incorrect results. Please use the {measure.agg.value}_distinct aggregation type."
                            ),
                        )
                    )
        return issues


class PercentileAggregationRule(ModelValidationRule):
    """Checks that only PERCENTILE measures have agg_params and valid percentile value provided."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring the agg_params.percentile value exist for measures with percentile aggregation"
    )
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        for semantic_model in model.semantic_models:
            for measure in semantic_model.measures:
                context = SemanticModelElementContext(
                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                    semantic_model_element=SemanticModelElementReference(
                        semantic_model_name=semantic_model.name, element_name=measure.name
                    ),
                    element_type=SemanticModelElementType.MEASURE,
                )
                if measure.agg == AggregationType.PERCENTILE:
                    if measure.agg_params is None or measure.agg_params.percentile is None:
                        issues.append(
                            ValidationError(
                                context=context,
                                message=(
                                    f"Measure '{measure.name}' uses a PERCENTILE aggregation, which requires agg_params.percentile to be provided."
                                ),
                            )
                        )
                    elif measure.agg_params.percentile <= 0 or measure.agg_params.percentile >= 1:
                        issues.append(
                            ValidationError(
                                context=context,
                                message=(
                                    f"Percentile aggregation parameter for measure '{measure.name}' is '{measure.agg_params.percentile}', but"
                                    "must be between 0 and 1 (non-inclusive). For example, to indicate the 65th percentile value, set 'percentile: 0.65'. "
                                    "For percentile values of 0, please use MIN, for percentile values of 1, please use MAX."
                                ),
                            )
                        )
                elif measure.agg == AggregationType.MEDIAN:
                    if measure.agg_params:
                        if measure.agg_params.percentile is not None and measure.agg_params.percentile != 0.5:
                            issues.append(
                                ValidationError(
                                    context=context,
                                    message=f"Measure '{measure.name}' uses a MEDIAN aggregation, while percentile is set to "
                                    f"'{measure.agg_params.percentile}', a conflicting value. Please remove the parameter "
                                    "or set to '0.5'.",
                                )
                            )
                        if measure.agg_params.use_discrete_percentile:
                            issues.append(
                                ValidationError(
                                    context=context,
                                    message=f"Measure '{measure.name}' uses a MEDIAN aggregation, while use_discrete_percentile"
                                    f"is set to true. Please remove the parameter or set to False.",
                                )
                            )
                elif measure.agg_params and (
                    measure.agg_params.percentile
                    or measure.agg_params.use_discrete_percentile
                    or measure.agg_params.use_approximate_percentile
                ):
                    wrong_params = []
                    if measure.agg_params.percentile:
                        wrong_params.append("percentile")
                    if measure.agg_params.use_discrete_percentile:
                        wrong_params.append("use_discrete_percentile")
                    if measure.agg_params.use_approximate_percentile:
                        wrong_params.append("use_approximate_percentile")

                    wrong_params_str = ", ".join(wrong_params)

                    issues.append(
                        ValidationError(
                            context=context,
                            message=(
                                f"Measure '{measure.name}' with aggregation '{measure.agg.value}' uses agg_params "
                                f"({wrong_params_str}) only relevant to Percentile measures."
                            ),
                        )
                    )
        return issues


def _get_measure_names_from_model(model: UserConfiguredModel) -> Set[str]:
    """Return every distinct measure name specified in the model"""
    measure_names = set()
    for semantic_model in model.semantic_models:
        for measure in semantic_model.measures:
            measure_names.add(measure.reference.element_name)

    return measure_names
