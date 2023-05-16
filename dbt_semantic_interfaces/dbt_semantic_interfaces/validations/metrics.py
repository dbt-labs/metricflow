import traceback
from typing import List

from dbt_semantic_interfaces.errors import ParsingException
from dbt_semantic_interfaces.objects.metric import Metric, MetricTimeWindow, MetricType
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import MetricModelReference
from dbt_semantic_interfaces.validations.unique_valid_name import UniqueAndValidNameRule
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    ModelValidationRule,
    ValidationError,
    ValidationIssue,
    validate_safely,
)


class CumulativeMetricRule(ModelValidationRule):
    """Checks that cumulative sum metrics are configured properly."""

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
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        for metric in model.metrics or []:
            issues += CumulativeMetricRule._validate_cumulative_sum_metric_params(metric=metric)

        return issues


class DerivedMetricRule(ModelValidationRule):
    """Checks that derived metrics are configured properly."""

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
                                message=f"Alias '{input_metric.alias}' for input metric: '{input_metric.name}' is "
                                "already being used. Please choose another alias.",
                            )
                        )
                        used_names.add(input_metric.alias)
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the input metrics exist")
    def _validate_input_metrics_exist(model: SemanticManifest) -> List[ValidationIssue]:
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
                                message=f"For metric: {metric.name}, input metric: '{input_metric.name}' does not "
                                "exist as a configured metric in the model.",
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
                        message=f"Both offset_window and offset_to_grain set for derived metric '{metric.name}' on "
                        f"input metric '{input_metric.name}'. Please set one or the other.",
                    )
                )

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring derived metrics properties are configured properly"
    )
    def validate_model(model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []

        issues += DerivedMetricRule._validate_input_metrics_exist(model=model)
        for metric in model.metrics or []:
            issues += DerivedMetricRule._validate_alias_collision(metric=metric)
            issues += DerivedMetricRule._validate_time_offset_params(metric=metric)
        return issues
