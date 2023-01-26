import traceback
from typing import List

from metricflow.aggregation_properties import AggregationType
from metricflow.errors.errors import ParsingException
from metricflow.instances import MetricModelReference
from metricflow.model.objects.metric import Metric, MetricType, MetricTimeWindow
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.unique_valid_name import UniqueAndValidNameRule
from metricflow.model.validations.validator_helpers import (
    FileContext,
    MetricContext,
    ModelValidationRule,
    ValidationIssueType,
    ValidationError,
    validate_safely,
)
from metricflow.references import MeasureReference


class CumulativeMetricRule(ModelValidationRule):
    """Checks that cumulative sum metrics are configured properly"""

    @staticmethod
    @validate_safely(whats_being_done="checking that the params of metric are valid if it is a cumulative sum metric")
    def _validate_cumulative_sum_metric_params(metric: Metric) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

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
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []

        for metric in model.metrics or []:
            issues += CumulativeMetricRule._validate_cumulative_sum_metric_params(metric=metric)

        return issues


class DerivedMetricRule(ModelValidationRule):
    """Checks that derived metrics are configured properly"""

    @staticmethod
    @validate_safely(whats_being_done="checking that the alias set are not unique and distinct")
    def _validate_alias_collision(metric: Metric) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

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
    def _validate_input_metrics_exist(model: UserConfiguredModel) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

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
    def _validate_time_offset_params(metric: Metric) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

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
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []

        issues += DerivedMetricRule._validate_input_metrics_exist(model=model)
        for metric in model.metrics or []:
            issues += DerivedMetricRule._validate_alias_collision(metric=metric)
            issues += DerivedMetricRule._validate_time_offset_params(metric=metric)
        return issues


class ConversionMetricRule(ModelValidationRule):
    """Checks that conversion metrics are configured properly"""

    @staticmethod
    @validate_safely(whats_being_done="checking that the params of metric are valid if it is a conversion metric")
    def _validate_type_params(metric: Metric) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        if metric.type == MetricType.CONVERSION:
            if metric.type_params.window:
                try:
                    _, _ = MetricTimeWindow.parse(metric.type_params.window.to_string())
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
    @validate_safely(whats_being_done="checks that the entity exists in the base/conversion data source")
    def _validate_entity_exists(model: UserConfiguredModel) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []
        for metric in model.metrics or []:
            if metric.type == MetricType.CONVERSION:
                if metric.type_params.entity is None:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"Entity must be set for conversion metrics, but not found in metric: {metric.name}.",
                        )
                    )
                    continue

                base_data_source = None
                conversion_data_source = None
                for data_source in model.data_sources:
                    data_source_measures = {measure.reference for measure in data_source.measures}
                    if metric.type_params.base_measure_reference in data_source_measures:
                        base_data_source = data_source
                    if metric.type_params.conversion_measure_reference in data_source_measures:
                        conversion_data_source = data_source

                assert (
                    base_data_source
                ), f"Unable to find data_source for measure: {metric.type_params.base_measure_reference.element_name}"
                assert (
                    conversion_data_source
                ), f"Unable to find data_source for measure: {metric.type_params.conversion_measure_reference.element_name}"

                if metric.type_params.entity not in {i.entity for i in base_data_source.identifiers}:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"Entity: {metric.type_params.entity} not found in base_data_source: {base_data_source.name}.",
                        )
                    )
                if metric.type_params.entity not in {i.entity for i in conversion_data_source.identifiers}:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"Entity: {metric.type_params.entity} not found in conversion_data_source: {conversion_data_source.name}.",
                        )
                    )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checks that the measures exist and are valid in the base/conversion data source")
    def _validate_measures(metric: Metric, model: UserConfiguredModel) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []
        if metric.type == MetricType.CONVERSION:

            def _validate_measure(measure_reference: MeasureReference) -> None:
                measure = None
                for data_source in model.data_sources:
                    for mmeasure in data_source.measures:
                        if mmeasure.reference == measure_reference:
                            measure = mmeasure
                            break

                if measure is None:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"For metric: {metric.name} unable to find measure: {measure_reference.element_name}",
                        )
                    )
                    return
                if (
                    measure.agg != AggregationType.COUNT
                    and measure.agg != AggregationType.COUNT_DISTINCT
                    and (measure.agg != AggregationType.SUM or measure.expr != "1")
                ):
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"For conversion metrics, the measure must be COUNT/SUM(1)/COUNT_DISTINCT. Measure: {measure.name} is agg type: {measure.agg}",
                        )
                    )

            _validate_measure(metric.type_params.base_measure_reference)
            _validate_measure(metric.type_params.conversion_measure_reference)
        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring conversion metrics are valid")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        issues += ConversionMetricRule._validate_entity_exists(model=model)
        for metric in model.metrics or []:
            issues += ConversionMetricRule._validate_measures(metric=metric, model=model)
            issues += ConversionMetricRule._validate_type_params(metric=metric)

        return issues
