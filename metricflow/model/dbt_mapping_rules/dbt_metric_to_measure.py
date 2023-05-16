import traceback
from typing import Dict, List, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import (
    DbtMappingRule,
    MappedObjects,
    assert_essential_metric_properties,
    get_and_assert_calc_method_mapping,
)
from dbt_semantic_interfaces.objects.metric import MetricType
from dbt_semantic_interfaces.validations.validator_helpers import (
    ModelValidationResults,
    ValidationIssue,
    ValidationError,
)

CALC_METHOD_TO_MEASURE_TYPE: Dict[str, AggregationType] = {
    "count": AggregationType.COUNT,
    "count_distinct": AggregationType.COUNT_DISTINCT,
    "sum": AggregationType.SUM,
    "average": AggregationType.AVERAGE,
    "min": AggregationType.MIN,
    "max": AggregationType.MAX,
    # "derived": AggregationType.DERIVED # Derived DBT metrics don't create measures
}


class DbtToMeasureName(DbtMappingRule):
    """Rule for mapping non-derived dbt metric names to semantic model measure names"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                metric_type = get_and_assert_calc_method_mapping(dbt_metric=metric)
                if metric_type != MetricType.DERIVED:
                    objects.measures[metric.model.name][metric.name]["name"] = metric.name

            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMeasureAgg(DbtMappingRule):
    """Rule for mapping non-derived dbt metric calculation method to semantic model measure agg"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                metric_type = get_and_assert_calc_method_mapping(dbt_metric=metric)
                if metric_type != MetricType.DERIVED:
                    objects.measures[metric.model.name][metric.name]["agg"] = CALC_METHOD_TO_MEASURE_TYPE[
                        metric.calculation_method
                    ]

            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMeasureExpr(DbtMappingRule):
    """Rule for mapping non-derived dbt metric expression to semantic model measure expression"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                metric_type = get_and_assert_calc_method_mapping(dbt_metric=metric)
                if metric_type != MetricType.DERIVED:
                    assert metric.expression, f"Expected an `expression` for `{metric.name}` metric, got `None`"
                    objects.measures[metric.model.name][metric.name]["expr"] = metric.expression

            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMeasureAggTimeDimension(DbtMappingRule):
    """Rule for mapping non-derived dbt metric timestamp to semantic model measure agg_time_dimension"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                metric_type = get_and_assert_calc_method_mapping(dbt_metric=metric)
                if metric_type != MetricType.DERIVED:
                    assert metric.timestamp, f"Expected a `timestamp` for `{metric.name}` metric, got `None`"
                    objects.measures[metric.model.name][metric.name]["agg_time_dimension"] = metric.timestamp

            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)
