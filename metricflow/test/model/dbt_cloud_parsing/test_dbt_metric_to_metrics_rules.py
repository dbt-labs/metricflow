from typing import Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_mapping_rules.dbt_metric_to_metrics_rules import (
    DbtToMetricName,
    DbtToMetricType,
    DbtToMetricDescription,
    DbtToMetricConstraint,
    DbtToDerivedMetricTypeParams,
    DbtToMeasureProxyMetricTypeParams,
)
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import MappedObjects, get_and_assert_calc_method_mapping
from metricflow.model.objects.metric import MetricType


def test_dbt_to_metric_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToMetricName().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMetricName raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        assert (
            objects.metrics.get(dbt_metric.name) is not None
        ), f"DbtToMetricName didn't create the metric `{dbt_metric.name}`"
        assert (
            objects.metrics[dbt_metric.name]["name"] == dbt_metric.name
        ), f"DbtToMetricName, created a metric named `{objects.metrics[dbt_metric.name]['name']}` expected `{dbt_metric.name}`"


def test_dbt_to_metric_name_requires_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    dbt_metrics[0].name = None
    issues = DbtToMetricName().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtToMetricName didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_to_metric_type(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToMetricType().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMetricType raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        assert (
            objects.metrics.get(dbt_metric.name) is not None
        ), f"DbtToMetricType didn't create the metric `{dbt_metric.name}`"
        assert (
            objects.metrics[dbt_metric.name].get("type") is not None
        ), f"DbtToMetricType created a metric `{dbt_metric.name}` with no `type` specified"


def test_dbt_to_metric_type_requires_calculation_method(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    dbt_metrics[0].calculation_method = None
    issues = DbtToMetricType().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtToMetricType didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_to_metric_description(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToMetricDescription().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMetricDescription raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        # descriptions are optional
        if dbt_metric.description is not None:
            assert (
                objects.metrics.get(dbt_metric.name) is not None
            ), f"DbtToMetricDescription didn't create the metric `{dbt_metric.name}`"
            assert (
                objects.metrics[dbt_metric.name].get("description") is not None
            ), f"DbtToMetricDescription created a metric `{dbt_metric.name}` with no `description` specified"


def test_dbt_to_metric_description_doesnt_require_description(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    dbt_metrics[0].description = None
    issues = DbtToMetricDescription().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMetricDescription raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    assert (
        objects.metrics.get(dbt_metrics[0].name) is None
    ), f"DbtToMetricDescription created a description for metric `{dbt_metrics[0].name}` when it shouldn't have"


def test_dbt_to_metric_constraint(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToMetricConstraint().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMetricConstraint raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        # Filters are optional
        if dbt_metric.filters:
            assert (
                objects.metrics.get(dbt_metric.name) is not None
            ), f"DbtToMetricConstraint didn't create the metric `{dbt_metric.name}`"
            assert (
                objects.metrics[dbt_metric.name].get("constraint") is not None
            ), f"DbtToMetricConstraint created a metric `{dbt_metric.name}` with no `constraint` specified"


def test_dbt_to_metric_constraint_doesnt_require_filters(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    dbt_metrics[0].filters = None
    issues = DbtToMetricConstraint().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToMetricConstraint raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    assert (
        objects.metrics.get(dbt_metrics[0].name) is None
    ), f"DbtToMetricConstraint created a constraint for metric `{dbt_metrics[0].name}` when it shouldn't have"


def test_dbt_to_derived_metric_type_params(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToDerivedMetricTypeParams().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToDerivedMetricTypeParams raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type == MetricType.DERIVED:
            assert (
                objects.metrics.get(dbt_metric.name) is not None
            ), f"DbtToDerivedMetricTypeParams didn't create the metric `{dbt_metric.name}`"
            assert (
                objects.metrics[dbt_metric.name].get("type_params") is not None
            ), f"DbtToDerivedMetricTypeParams created a metric `{dbt_metric.name}` with no `type_params` specified"
        else:
            # This rule shouldn't created anything for non derived metrics
            assert (
                objects.metrics.get(dbt_metric.name) is None
            ), f"DbtToDerivedMetricTypeParams created a metric type params for the non derived metirc `{dbt_metric.name}`"


def test_dbt_to_derived_metric_type_params_requires_expression(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type == MetricType.DERIVED:
            dbt_metric.expression = None
    issues = DbtToDerivedMetricTypeParams().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtToDerivedMetricTypeParams didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_to_derived_metric_type_params_requires_depends_on(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type == MetricType.DERIVED:
            dbt_metric.depends_on = None
    issues = DbtToDerivedMetricTypeParams().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtToDerivedMetricTypeParams didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_to_measure_proxy_metric_type_params(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtToMeasureProxyMetricTypeParams().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtToDerivedMetricTypeParams raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.metrics.get(dbt_metric.name) is not None
            ), f"DbtToMeasureProxyMetricTypeParams didn't create the metric `{dbt_metric.name}`"
            assert (
                objects.metrics[dbt_metric.name].get("type_params") is not None
            ), f"DbtToMeasureProxyMetricTypeParams created a metric `{dbt_metric.name}` with no `type_params` specified"
        else:
            # This rule shouldn't created anything for derived metrics
            assert (
                objects.metrics.get(dbt_metric.name) is None
            ), f"DbtToMeasureProxyMetricTypeParams created a metric type params for the non derived metirc `{dbt_metric.name}`"


def test_dbt_to_measure_proxy_metric_type_params_requires_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            dbt_metric.name = None
    issues = DbtToMeasureProxyMetricTypeParams().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtToMeasureProxyMetricTypeParams didn't raise blocking issues when it should have: {issues.to_pretty_json()}"
