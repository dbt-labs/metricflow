from collections import defaultdict
from typing import DefaultDict, Dict, List, Set

from more_itertools import bucket

from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference, MetricModelReference
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.validations.unique_valid_name import UniqueAndValidNameRule
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    ModelValidationRule,
    SemanticModelElementContext,
    SemanticModelElementReference,
    SemanticModelElementType,
    ValidationError,
    ValidationIssue,
    ValidationWarning,
    validate_safely,
)


class SemanticModelMeasuresUniqueRule(ModelValidationRule):
    """Asserts all measure names are unique across the model."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring measures exist in only one configured semantic model"
    )
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
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


class MeasureConstraintAliasesRule(ModelValidationRule):
    """Checks that aliases are configured correctly for constrained measure references.

    These are, currently, only applicable for Metric types, since the MetricInputMeasure is only
    """

    @staticmethod
    @validate_safely(whats_being_done="ensuring measures aliases are set when required")
    def _validate_required_aliases_are_set(metric: Metric, metric_context: MetricContext) -> List[ValidationIssue]:
        """Checks if valid aliases are set on the input measure references where they are required.

        Aliases are required whenever there are 2 or more input measures with the same measure
        reference with different constraints. When this happens, we require aliases for all
        constrained measures for the sake of clarity. Any unconstrained measure does not
        need an alias, since it always relies on the original measure specification.

        At this time aliases are required for ratio metrics, but eventually we could relax that requirement
        if we can find an automatic aliasing scheme for numerator/denominator that we feel comfortable using.
        """
        issues: List[ValidationIssue] = []

        if len(metric.measure_references) == len(set(metric.measure_references)):
            # All measure references are unique, so disambiguation via aliasing is not necessary
            return issues

        # Note: more_itertools.bucket does not produce empty groups
        input_measures_by_name = bucket(metric.input_measures, lambda x: x.name)
        for name in input_measures_by_name:
            input_measures = list(input_measures_by_name[name])

            if len(input_measures) == 1:
                continue

            distinct_input_measures = set(input_measures)
            if len(distinct_input_measures) == 1:
                # Warn whenever multiple identical references exist - we will consolidate these but it might be
                # a meaningful oversight if constraints and aliases are specified
                issues.append(
                    ValidationWarning(
                        context=metric_context,
                        message=(
                            f"Metric {metric.name} has multiple identical input measures specifications for measure "
                            f"{name}. This might be hiding a semantic error. Input measure specification: "
                            f"{input_measures[0]}."
                        ),
                    )
                )
                continue

            constrained_measures_without_aliases = [
                measure for measure in input_measures if measure.filter is not None and measure.alias is None
            ]
            if constrained_measures_without_aliases:
                issues.append(
                    ValidationError(
                        context=metric_context,
                        message=(
                            f"Metric {metric.name} depends on multiple different constrained versions of measure "
                            f"{name}. In such cases, aliases must be provided, but the following input measures have "
                            f"constraints specified without an alias: {constrained_measures_without_aliases}."
                        ),
                    )
                )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking constrained measures are aliased properly")
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:
        """Ensures measures that might need an alias have one set, and that the alias is distinct.

        We do not allow aliases to collide with other alias or measure names, since that could create
        ambiguity at query time or cause issues if users ever restructure their models.
        """
        issues: List[ValidationIssue] = []

        measure_names = _get_measure_names_from_model(model)
        measure_alias_to_metrics: DefaultDict[str, List[str]] = defaultdict(list)
        for metric in model.metrics:
            metric_context = MetricContext(
                file_context=FileContext.from_metadata(metadata=metric.metadata),
                metric=MetricModelReference(metric_name=metric.name),
            )

            issues += MeasureConstraintAliasesRule._validate_required_aliases_are_set(
                metric=metric, metric_context=metric_context
            )

            aliased_measures = [
                input_measure for input_measure in metric.input_measures if input_measure.alias is not None
            ]

            for measure in aliased_measures:
                assert measure.alias, "Type refinement assertion, previous filter should ensure this is true"
                issues += UniqueAndValidNameRule.check_valid_name(measure.alias, metric_context)
                if measure.alias in measure_names:
                    issues.append(
                        ValidationError(
                            context=metric_context,
                            message=(
                                f"Alias `{measure.alias}` for measure `{measure.name}` conflicts with measure names "
                                f"defined elsewhere in the model! This can cause ambiguity for certain types of "
                                f"query. Please choose another alias."
                            ),
                        )
                    )
                if measure.alias in measure_alias_to_metrics:
                    issues.append(
                        ValidationError(
                            context=metric_context,
                            message=(
                                f"Measure alias {measure.alias} conflicts with a measure alias used elsewhere in the "
                                f"model! This can cause ambiguity for certain types of query. Please choose another "
                                f"alias, or, if the measures are constrained in the same way, consider centralizing "
                                f"that definition in a new semantic model. Measure specification: {measure}. Existing "
                                f"metrics with that measure alias used: {measure_alias_to_metrics[measure.alias]}"
                            ),
                        )
                    )

                measure_alias_to_metrics[measure.alias].append(metric.name)

        return issues


class MetricMeasuresRule(ModelValidationRule):
    """Checks that the measures referenced in the metrics exist."""

    @staticmethod
    @validate_safely(whats_being_done="checking all measures referenced by the metric exist")
    def _validate_metric_measure_references(metric: Metric, valid_measure_names: Set[str]) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for measure_reference in metric.measure_references:
            if measure_reference.element_name not in valid_measure_names:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message=(
                            f"Measure {measure_reference.element_name} referenced in metric {metric.name} is not "
                            f"defined in the model!"
                        ),
                    )
                )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring metric measures exist")
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        valid_measure_names = _get_measure_names_from_model(model)

        for metric in model.metrics or []:
            issues += MetricMeasuresRule._validate_metric_measure_references(
                metric=metric, valid_measure_names=valid_measure_names
            )
        return issues


class MeasuresNonAdditiveDimensionRule(ModelValidationRule):
    """Checks that the measure's non_additive_dimensions are properly defined."""

    @staticmethod
    @validate_safely(whats_being_done="ensuring that a measure's non_additive_dimensions is valid")
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
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
                                f"Measure '{measure.name}' has a agg_time_dimension of "
                                f"{measure.checked_agg_time_dimension.element_name} ",
                                f"that is not defined as a dimension in semantic model '{semantic_model.name}'.",
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
                                f"Measure '{measure.name}' has a non_additive_dimension with name "
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
                                context=SemanticModelElementContext(
                                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                                    semantic_model_element=SemanticModelElementReference(
                                        semantic_model_name=semantic_model.name, element_name=measure.name
                                    ),
                                    element_type=SemanticModelElementType.MEASURE,
                                ),
                                message=(
                                    f"Measure '{measure.name}' has a non_additive_dimension with name"
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
                                    f"Measure '{measure.name}' has a non_additive_dimension with name "
                                    f"'{non_additive_dimension.name}' that has a base time granularity "
                                    f"({matching_dimension.type_params.time_granularity.name}) that is not equal to "
                                    f"the measure's agg_time_dimension {agg_time_dimension.name} with a base "
                                    f"granularity of ({agg_time_dimension.type_params.time_granularity.name})."
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
                                f"Measure '{measure.name}' has a non_additive_dimension with an invalid "
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
                            context=SemanticModelElementContext(
                                file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                                semantic_model_element=SemanticModelElementReference(
                                    semantic_model_name=semantic_model.name, element_name=measure.name
                                ),
                                element_type=SemanticModelElementType.MEASURE,
                            ),
                            message=(
                                f"Measure '{measure.name}' has a non_additive_dimension with an invalid "
                                "'window_groupings'. These entities "
                                f"{window_groupings.difference(intersected_entities)} do not exist in the "
                                "semantic model."
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
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
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
                                f"Measure '{measure.name}' uses a COUNT aggregation, which requires an expr to be "
                                "provided. Provide 'expr: 1' if a count of all rows is desired."
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
                                f"Measure '{measure.name}' uses a '{measure.agg.value}' aggregation with a DISTINCT "
                                f"expr: '{measure.expr}. This is not supported as it effectively converts an additive "
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
        whats_being_done="running model validation ensuring the agg_params.percentile value exist for measures with "
        "percentile aggregation"
    )
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
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
                                    f"Measure '{measure.name}' uses a PERCENTILE aggregation, which requires "
                                    "agg_params.percentile to be provided."
                                ),
                            )
                        )
                    elif measure.agg_params.percentile <= 0 or measure.agg_params.percentile >= 1:
                        issues.append(
                            ValidationError(
                                context=context,
                                message=(
                                    f"Percentile aggregation parameter for measure '{measure.name}' is "
                                    f"'{measure.agg_params.percentile}', but must be between 0 and 1 (non-inclusive). "
                                    "For example, to indicate the 65th percentile value, set 'percentile: 0.65'. "
                                    "For percentile values of 0, please use MIN, for percentile values of 1, please "
                                    "use MAX."
                                ),
                            )
                        )
                elif measure.agg == AggregationType.MEDIAN:
                    if measure.agg_params:
                        if measure.agg_params.percentile is not None and measure.agg_params.percentile != 0.5:
                            issues.append(
                                ValidationError(
                                    context=context,
                                    message=f"Measure '{measure.name}' uses a MEDIAN aggregation, while percentile is "
                                    f"set to '{measure.agg_params.percentile}', a conflicting value. Please remove "
                                    "the parameter or set to '0.5'.",
                                )
                            )
                        if measure.agg_params.use_discrete_percentile:
                            issues.append(
                                ValidationError(
                                    context=context,
                                    message=f"Measure '{measure.name}' uses a MEDIAN aggregation, while "
                                    "use_discrete_percentile is set to true. Please remove the parameter or set "
                                    "to False.",
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


def _get_measure_names_from_model(model: SemanticManifest) -> Set[str]:
    """Return every distinct measure name specified in the model."""
    measure_names = set()
    for semantic_model in model.semantic_models:
        for measure in semantic_model.measures:
            measure_names.add(measure.reference.element_name)

    return measure_names
