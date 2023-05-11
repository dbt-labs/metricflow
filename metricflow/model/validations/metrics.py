import traceback
from collections import defaultdict
from typing import DefaultDict, List, Set

from more_itertools import bucket

from dbt_semantic_interfaces.errors import ParsingException
from dbt_semantic_interfaces.objects.metric import Metric, MetricType, MetricTimeWindow
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import MetricModelReference
from metricflow.model.validations.unique_valid_name import UniqueAndValidNameRule
from metricflow.model.validations.validator_helpers import (
    FileContext,
    MetricContext,
    ModelValidationRule,
    ValidationIssue,
    ValidationError,
    ValidationWarning,
    validate_safely,
)


class CumulativeMetricRule(ModelValidationRule):
    """Checks that cumulative sum metrics are configured properly"""

    @staticmethod
    @validate_safely(whats_being_done="checking that the params of metric are valid if it is a cumulative sum metric")
    def _validate_cumulative_sum_metric_params(metric: Metric) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if metric.type == MetricType.CUMULATIVE:
            if metric.type_params.window and metric.type_params.grain_to_date:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message="Both window and grain_to_date set for cumulative metric. Please set one or the other",
                    )
                )

            if metric.type_params.window:
                try:
                    MetricTimeWindow.parse(metric.type_params.window.to_string())
                except ParsingException as e:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message="".join(traceback.format_exception_only(etype=type(e), value=e)),
                            extra_detail="".join(traceback.format_tb(e.__traceback__)),
                        )
                    )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring cumulative sum metrics are valid")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        for metric in model.metrics or []:
            issues += CumulativeMetricRule._validate_cumulative_sum_metric_params(metric=metric)

        return issues


class DerivedMetricRule(ModelValidationRule):
    """Checks that derived metrics are configured properly"""

    @staticmethod
    @validate_safely(whats_being_done="checking that the alias set are not unique and distinct")
    def _validate_alias_collision(metric: Metric) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if metric.type == MetricType.DERIVED:
            metric_context = MetricContext(
                file_context=FileContext.from_metadata(metadata=metric.metadata),
                metric=MetricModelReference(metric_name=metric.name),
            )
            used_names = {input_metric.name for input_metric in metric.input_metrics}
            for input_metric in metric.input_metrics:
                if input_metric.alias:
                    issues += UniqueAndValidNameRule.check_valid_name(input_metric.alias, metric_context)
                    if input_metric.alias in used_names:
                        issues.append(
                            ValidationError(
                                context=metric_context,
                                message=f"Alias '{input_metric.alias}' for input metric: '{input_metric.name}' is already being used. Please choose another alias.",
                            )
                        )
                        used_names.add(input_metric.alias)
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the input metrics exist")
    def _validate_input_metrics_exist(model: UserConfiguredModel) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        all_metrics = {m.name for m in model.metrics}
        for metric in model.metrics:
            if metric.type == MetricType.DERIVED:
                for input_metric in metric.input_metrics:
                    if input_metric.name not in all_metrics:
                        issues.append(
                            ValidationError(
                                context=MetricContext(
                                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                                    metric=MetricModelReference(metric_name=metric.name),
                                ),
                                message=f"For metric: {metric.name}, input metric: '{input_metric.name}' does not exist as a configured metric in the model.",
                            )
                        )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that input metric time offset params are valid")
    def _validate_time_offset_params(metric: Metric) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for input_metric in metric.input_metrics or []:
            if input_metric.offset_window and input_metric.offset_to_grain:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message=f"Both offset_window and offset_to_grain set for derived metric '{metric.name}' on input metric '{input_metric.name}'. Please set one or the other.",
                    )
                )

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring derived metrics properties are configured properly"
    )
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        issues += DerivedMetricRule._validate_input_metrics_exist(model=model)
        for metric in model.metrics or []:
            issues += DerivedMetricRule._validate_alias_collision(metric=metric)
            issues += DerivedMetricRule._validate_time_offset_params(metric=metric)
        return issues


class MetricConstraintAliasesRule(ModelValidationRule):
    """Checks that aliases are configured correctly for constrained metric references """

    @staticmethod
    @validate_safely(whats_being_done="ensuring metric aliases are set when required")
    def _validate_required_aliases_are_set(metric: Metric, metric_context: MetricContext) -> List[ValidationIssue]:
        """Checks if valid aliases are set on the input metric references where they are required

        Aliases are required whenever there are 2 or more input metrics with the same metric
        reference with different constraints. When this happens, we require aliases for all
        constrained metrics for the sake of clarity. Any unconstrained metric does not
        need an alias, since it always relies on the original metric specification.
        """
        issues: List[ValidationIssue] = []

        if len(metric.input_metrics) == len(set(metric.input_metrics)):
            # All measure references are unique, so disambiguation via aliasing is not necessary
            return issues

        # Note: more_itertools.bucket does not produce empty groups
        input_metrics_by_name = bucket(metric.input_metrics, lambda x: x.name)
        for name in input_metrics_by_name:
            input_metrics = list(input_metrics_by_name[name])

            if len(input_metrics) == 1:
                continue

            distinct_input_metrics = set(input_metrics)
            if len(distinct_input_metrics) == 1:
                # Warn whenever multiple identical references exist - we will consolidate these but it might be
                # a meaningful oversight if constraints and aliases are specified
                issues.append(
                    ValidationWarning(
                        context=metric_context,
                        message=(
                            f"Metric {metric.name} has multiple identical input metrics specifications for metric "
                            f"{name}. This might be hiding a semantic error. Input metric specification: "
                            f"{input_metrics[0]}."
                        ),
                    )
                )
                continue

            constrained_metrics_without_aliases = [
                metric for metric in input_metrics if metric.constraint is not None and metric.alias is None
            ]
            if constrained_metrics_without_aliases:
                issues.append(
                    ValidationError(
                        context=metric_context,
                        message=(
                            f"Metric {metric.name} depends on multiple different constrained versions of metric "
                            f"{name}. In such cases, aliases must be provided, but the following input metrics have "
                            f"constraints specified without an alias: {constrained_metrics_without_aliases}."
                        ),
                    )
                )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking constrained metrics are aliased properly")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:
        """Ensures metrics that might need an alias have one set, and that the alias is distinct

        We do not allow aliases to collide with other alias or metric names, since that could create
        ambiguity at query time or cause issues if users ever restructure their models.
        """
        issues: List[ValidationIssue] = []

        metric_names = _get_metric_names_from_model(model)
        metric_alias_to_metrics: DefaultDict[str, List[str]] = defaultdict(list)
        for metric in model.metrics:
            metric_context = MetricContext(
                file_context=FileContext.from_metadata(metadata=metric.metadata),
                metric=MetricModelReference(metric_name=metric.name),
            )

            issues += MetricConstraintAliasesRule._validate_required_aliases_are_set(
                metric=metric, metric_context=metric_context
            )

            aliased_metrics = [
                input_metric for input_metric in metric.input_metrics if input_metric.alias is not None
            ]

            for metric in aliased_metrics:
                assert metric.alias, "Type refinement assertion, previous filter should ensure this is true"
                issues += UniqueAndValidNameRule.check_valid_name(metric.alias, metric_context)
                if metric.alias in metric_names:
                    issues.append(
                        ValidationError(
                            context=metric_context,
                            message=(
                                f"Alias `{metric.alias}` for metric `{metric.name}` conflicts with metric names "
                                f"defined elsewhere in the semantic manifest! This can cause ambiguity for certain types of "
                                f"query. Please choose another alias."
                            ),
                        )
                    )
                if metric.alias in metric_alias_to_metrics:
                    issues.append(
                        ValidationError(
                            context=metric_context,
                            message=(
                                f"Metric alias {metric.alias} conflicts with a metric alias used elsewhere in the "
                                f"semantic manifest! This can cause ambiguity for certain types of query. Please choose another "
                                f"alias, or, if the metrics are constrained in the same way, consider centralizing "
                                f"that definition in a new semantic model. Metric specification: {metric}. Existing "
                                f"metrics with that metric alias used: {metric_alias_to_metrics[metric.alias]}"
                            ),
                        )
                    )

                metric_alias_to_metrics[metric.alias].append(metric.name)

        return issues
    

def _get_metric_names_from_model(model: UserConfiguredModel) -> Set[str]:
    """Return every distinct measure name specified in the model"""
    metric_names = set()
    for metric in model.metrics:
        for metric_reference in metric.input_metrics:
            metric_names.add(metric_reference.as_reference.element_name)

    return metric_names