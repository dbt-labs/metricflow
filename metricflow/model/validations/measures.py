from collections import defaultdict
from typing import DefaultDict, Dict, List, Set

from more_itertools import bucket

from metricflow.instances import MetricModelReference
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.unique_valid_name import UniqueAndValidNameRule
from metricflow.model.validations.validator_helpers import (
    DataSourceElementContext,
    DataSourceElementReference,
    DataSourceElementType,
    FileContext,
    MetricContext,
    ModelValidationRule,
    ValidationIssueType,
    ValidationError,
    ValidationWarning,
    validate_safely,
)
from metricflow.references import MeasureReference


class DataSourceMeasuresUniqueRule(ModelValidationRule):
    """Asserts all measure names are unique across the model."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring measures exist in only one configured data source"
    )
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []

        measure_references_to_data_sources: Dict[MeasureReference, List] = defaultdict(list)
        for data_source in model.data_sources:
            for measure in data_source.measures:
                if measure.reference in measure_references_to_data_sources:
                    issues.append(
                        ValidationError(
                            context=DataSourceElementContext(
                                file_context=FileContext.from_metadata(metadata=data_source.metadata),
                                data_source_element=DataSourceElementReference(
                                    data_source_name=data_source.name, element_name=measure.name
                                ),
                                element_type=DataSourceElementType.MEASURE,
                            ),
                            message=f"Found measure with name {measure.name} in multiple data sources with names "
                            f"({measure_references_to_data_sources[measure.reference]})",
                        )
                    )
                measure_references_to_data_sources[measure.reference].append(data_source.name)

        return issues


class MeasureConstraintAliasesRule(ModelValidationRule):
    """Checks that aliases are configured correctly for constrained measure references

    These are, currently, only applicable for Metric types, since the MetricInputMeasure is only
    """

    @staticmethod
    @validate_safely(whats_being_done="ensuring measures aliases are set when required")
    def _validate_required_aliases_are_set(metric: Metric, metric_context: MetricContext) -> List[ValidationIssueType]:
        """Checks if valid aliases are set on the input measure references where they are required

        Aliases are required whenever there are 2 or more input measures with the same measure
        reference with different constraints. When this happens, we require aliases for all
        constrained measures for the sake of clarity. Any unconstrained measure does not
        need an alias, since it always relies on the original measure specification.

        At this time aliases are required for ratio metrics, but eventually we could relax that requirement
        if we can find an automatic aliasing scheme for numerator/denominator that we feel comfortable using.
        """
        issues: List[ValidationIssueType] = []

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
                measure for measure in input_measures if measure.constraint is not None and measure.alias is None
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
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:
        """Ensures measures that might need an alias have one set, and that the alias is distinct

        We do not allow aliases to collide with other alias or measure names, since that could create
        ambiguity at query time or cause issues if users ever restructure their models.
        """
        issues: List[ValidationIssueType] = []

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
                                f"that definition in a new data source. Measure specification: {measure}. Existing "
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
    def _validate_metric_measure_references(metric: Metric, valid_measure_names: Set[str]) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

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
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        valid_measure_names = _get_measure_names_from_model(model)

        for metric in model.metrics or []:
            issues += MetricMeasuresRule._validate_metric_measure_references(
                metric=metric, valid_measure_names=valid_measure_names
            )
        return issues


def _get_measure_names_from_model(model: UserConfiguredModel) -> Set[str]:
    """Return every distinct measure name specified in the model"""
    measure_names = set()
    for data_source in model.data_sources:
        for measure in data_source.measures:
            measure_names.add(measure.reference.element_name)

    return measure_names
