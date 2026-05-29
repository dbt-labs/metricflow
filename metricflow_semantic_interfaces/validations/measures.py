from __future__ import annotations

from collections import defaultdict
from typing import DefaultDict, Dict, Generic, List, Sequence, Set

from more_itertools import bucket

from metricflow_semantic_interfaces.protocols import (
    Metric,
    SemanticManifest,
    SemanticManifestT,
)
from metricflow_semantic_interfaces.references import MeasureReference, MetricModelReference
from metricflow_semantic_interfaces.validations.shared_measure_and_metric_helpers import (
    SharedMeasureAndMetricHelpers,
)
from metricflow_semantic_interfaces.validations.unique_valid_name import UniqueAndValidNameRule
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    SemanticManifestValidationRule,
    SemanticModelElementContext,
    SemanticModelElementReference,
    SemanticModelElementType,
    ValidationError,
    ValidationIssue,
    ValidationWarning,
    validate_safely,
)


class SemanticModelMeasuresUniqueRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Asserts all measure names are unique across the model."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring measures exist in only one configured semantic model"
    )
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []

        measure_references_to_semantic_models: Dict[MeasureReference, List] = defaultdict(list)
        for semantic_model in semantic_manifest.semantic_models:
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


class MeasureConstraintAliasesRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that aliases are configured correctly for constrained measure references.

    These are, currently, only applicable for PydanticMetric types, since the MetricInputMeasure is only
    """

    @staticmethod
    @validate_safely(whats_being_done="ensuring measures aliases are set when required")
    def _validate_required_aliases_are_set(metric: Metric, metric_context: MetricContext) -> Sequence[ValidationIssue]:
        """Checks if valid aliases are set on the input measure references where they are required.

        Aliases are required whenever there are 2 or more input measures with the same measure
        reference with different constraints. When this happens, we require aliases for all
        constrained measures for the sake of clarity. Any unconstrained measure does not  # type: ignore[misc]
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
                            f"PydanticMetric {metric.name} has multiple identical input measures specifications for "
                            f"measure {name}. This might be hiding a semantic error. Input measure specification: "
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
                            f"PydanticMetric {metric.name} depends on multiple different constrained versions of "
                            f"measure {name}. In such cases, aliases must be provided, but the following input "
                            f"measures have constraints specified without an alias: "
                            f"{constrained_measures_without_aliases}."
                        ),
                    )
                )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking constrained measures are aliased properly")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:
        """Ensures measures that might need an alias have one set, and that the alias is distinct.

        We do not allow aliases to collide with other alias or measure names, since that could create
        ambiguity at query time or cause issues if users ever restructure their models.
        """
        issues: List[ValidationIssue] = []

        measure_names = _get_measure_names_from_semantic_manifest(semantic_manifest)
        measure_alias_to_metrics: DefaultDict[str, List[str]] = defaultdict(list)
        for metric in semantic_manifest.metrics:
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


class MetricMeasuresRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that the measures referenced in the metrics exist."""

    @staticmethod
    @validate_safely(whats_being_done="checking all measures referenced by the metric exist")
    def _validate_metric_measure_references(metric: Metric, valid_measure_names: Set[str]) -> Sequence[ValidationIssue]:
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
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        valid_measure_names = _get_measure_names_from_semantic_manifest(semantic_manifest)

        for metric in semantic_manifest.metrics or []:
            issues += MetricMeasuresRule._validate_metric_measure_references(
                metric=metric, valid_measure_names=valid_measure_names
            )
        return issues


class MeasuresNonAdditiveDimensionRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that the measure's non_additive_dimensions are properly defined."""

    @staticmethod
    @validate_safely(whats_being_done="ensuring that a measure's non_additive_dimensions is valid")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        for semantic_model in semantic_manifest.semantic_models or []:
            for measure in semantic_model.measures:
                non_additive_dimension = measure.non_additive_dimension
                if non_additive_dimension is None:
                    continue
                agg_time_dimension_reference = semantic_model.checked_agg_time_dimension_for_measure(measure.reference)
                issues.extend(
                    SharedMeasureAndMetricHelpers.validate_non_additive_dimension(
                        object=measure,
                        semantic_model=semantic_model,
                        non_additive_dimension=non_additive_dimension,
                        agg_time_dimension_reference=agg_time_dimension_reference,
                        object_type_for_errors="Measure",
                    )
                )

        return issues


class CountAggregationExprRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that COUNT measures have an expr provided."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring expr exist for measures with count aggregation"
    )
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []

        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                context = SemanticModelElementContext(
                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                    semantic_model_element=SemanticModelElementReference(
                        semantic_model_name=semantic_model.name, element_name=measure.name
                    ),
                    element_type=SemanticModelElementType.MEASURE,
                )
                issues.extend(
                    SharedMeasureAndMetricHelpers.validate_expr_for_count_aggregation(
                        context=context,
                        object_name=measure.name,
                        object_type="Measure",
                        agg_type=measure.agg,
                        expr=measure.expr,
                    )
                )
        return issues


class PercentileAggregationRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that only PERCENTILE measures have agg_params and a valid percentile value is provided."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring the agg_params.percentile value exist for measures with "
        "percentile aggregation"
    )
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []

        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                context = SemanticModelElementContext(
                    file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                    semantic_model_element=SemanticModelElementReference(
                        semantic_model_name=semantic_model.name, element_name=measure.name
                    ),
                    element_type=SemanticModelElementType.MEASURE,
                )
                issues.extend(
                    SharedMeasureAndMetricHelpers.validate_percentile_arguments(
                        context=context,
                        object_name=measure.name,
                        object_type="Measure",
                        agg_type=measure.agg,
                        agg_params=measure.agg_params,
                    )
                )
        return issues


def _get_measure_names_from_semantic_manifest(semantic_manifest: SemanticManifest) -> Set[str]:
    """Return every distinct measure name specified in the model."""
    measure_names = set()
    for semantic_model in semantic_manifest.semantic_models:
        for measure in semantic_model.measures:
            measure_names.add(measure.reference.element_name)

    return measure_names
