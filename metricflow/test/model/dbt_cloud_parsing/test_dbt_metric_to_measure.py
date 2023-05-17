from typing import Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_mapping_rules.dbt_metric_to_measure import (
    DbtToMeasureName,
    DbtToMeasureAgg,
    DbtToMeasureAggTimeDimension,
    DbtToMeasureExpr,
)
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import (
    MappedObjects,
    DbtMappingRule,
    get_and_assert_calc_method_mapping,
)
from metricflow.model.dbt_converter import DbtConverter
from dbt_semantic_interfaces.objects.metric import MetricType


def test_dbt_metric_to_measure_rules_skip_derived_metrics(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    derived_metrics = tuple(dbt_metric for dbt_metric in dbt_metrics if dbt_metric.calculation_method == "derived")
    rules: Tuple[DbtMappingRule, ...] = (
        DbtToMeasureName(),
        DbtToMeasureAgg(),
        DbtToMeasureAggTimeDimension(),
        DbtToMeasureExpr(),
    )
    converter = DbtConverter(rules=rules)
    result = converter._map_dbt_to_metricflow(dbt_metrics=derived_metrics)
    assert len(result.mapped_objects.dimensions.keys()) == 0, "Derived dbt metrics created measures, and they shouldn't"


def test_dbt_to_measure_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToMeasureName().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMeasureName raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.measures[dbt_metric.model.name].get(dbt_metric.name) is not None
            ), f"DbtToMeasureName didn't create the measure `{dbt_metric.name}` for semantic model `{dbt_metric.model.name}`"
            assert (
                objects.measures[dbt_metric.model.name][dbt_metric.name]["name"] == dbt_metric.name
            ), f"DbtToMeasureName, created a measure named `{objects.measures[dbt_metric.model.name][dbt_metric.name]['name']}` expected `{dbt_metric.name}`"


def test_dbt_to_measure_name_requires_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    dbt_metrics[0].name = None
    issues = DbtToMeasureName().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtToMeasureName didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_to_measure_agg(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToMeasureAgg().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMeasure raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.measures[dbt_metric.model.name].get(dbt_metric.name) is not None
            ), f"DbtToMeasureAgg didn't create the measure `{dbt_metric.name}` for semantic model `{dbt_metric.model.name}`"
            assert (
                objects.measures[dbt_metric.model.name][dbt_metric.name].get("agg") is not None
            ), f"DbtToMeasureAgg, created a measure `{dbt_metric.name}` with no `agg` specified"


def test_dbt_to_measure_agg_requires_calculation_method(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    dbt_metrics[0].calculation_method = None
    issues = DbtToMeasureAgg().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtToMeasureAgg didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_to_measure_agg_time_dimension(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToMeasureAggTimeDimension().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMeasure raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.measures[dbt_metric.model.name].get(dbt_metric.name) is not None
            ), f"DbtToMeasureAggTimeDimension didn't create the measure `{dbt_metric.name}` for semantic model `{dbt_metric.model.name}`"
            assert (
                objects.measures[dbt_metric.model.name][dbt_metric.name].get("agg_time_dimension") is not None
            ), f"DbtToMeasureAggTimeDimension, created a measure `{dbt_metric.name}` with no `agg_time_dimension` specified"


def test_dbt_to_measure_agg_time_dimension_requires_timestamp(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    dbt_metrics[0].timestamp = None
    issues = DbtToMeasureAggTimeDimension().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtToMeasureAggTimeDimension didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_to_measure_expr(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToMeasureExpr().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMeasureExpr raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.measures[dbt_metric.model.name].get(dbt_metric.name) is not None
            ), f"DbtToMeasureExpr didn't create the measure `{dbt_metric.name}` for semantic model `{dbt_metric.model.name}`"
            assert (
                objects.measures[dbt_metric.model.name][dbt_metric.name].get("expr") is not None
            ), f"DbtToMeasureExpr, created a measure `{dbt_metric.name}` with no `expr` specified"


def test_dbt_to_measure_expr_requires_expression(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    dbt_metrics[0].expression = None
    issues = DbtToMeasureExpr().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtToMeasureExpr didn't raise blocking issues when it should have: {issues.to_pretty_json()}"
