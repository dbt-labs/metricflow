from typing import Any, Dict, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_converter import DbtConverter
from metricflow.model.dbt_mapping_rules.dbt_metric_to_dimensions_rules import (
    DbtDimensionsToDimensions,
    DbtTimestampToDimension,
    DbtFiltersToDimensions,
)
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import (
    MappedObjects,
    DbtMappingRule,
    get_and_assert_calc_method_mapping,
)
from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.objects.metric import MetricType


def test_dbt_metric_to_dimensions_rules_skip_derived_metrics(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    derived_metrics = tuple(dbt_metric for dbt_metric in dbt_metrics if dbt_metric.calculation_method == "derived")
    rules: Tuple[DbtMappingRule, ...] = (
        DbtDimensionsToDimensions(),
        DbtTimestampToDimension(),
        DbtFiltersToDimensions(),
    )
    converter = DbtConverter(rules=rules)
    result = converter._map_dbt_to_metricflow(dbt_metrics=derived_metrics)
    assert (
        len(result.mapped_objects.dimensions.keys()) == 0
    ), "Derived dbt metrics created dimensions, and they shouldn't"


def test_dbt_dimensions_to_dimensions(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtDimensionsToDimensions().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtDimensionsToDimensions raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            for dimension in dbt_metric.dimensions:
                assert (
                    objects.dimensions[dbt_metric.model.name].get(dimension) is not None
                ), f"Failed to build dimension `{dimension}` for data source `{dbt_metric.model.name}` of dbt metric `{dbt_metric.name}`"


def test_dbt_dimensions_to_dimensions_no_issues_when_no_dimensions(  # noqa: D
    dbt_metrics: Tuple[MetricNode, ...]
) -> None:
    objects = MappedObjects()
    dbt_metrics[0].dimensions = None
    issues = DbtDimensionsToDimensions().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtDimensionsToDimensions raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"


def test_dbt_timestamp_to_dimension(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtTimestampToDimension().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtTimestampToDimension raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            # each dbt metric timestamp creates 1 dimension, and a dbt metric must have exactly one timestamp
            assert (
                len(objects.dimensions[dbt_metric.model.name].keys()) == 1
            ), f"A dbt metric timestamp should only create 1 dimension, but {len(objects.dimensions[dbt_metric.model.name].keys())} were created"
            assert (
                objects.dimensions[dbt_metric.model.name][dbt_metric.timestamp]["type"] == DimensionType.TIME
            ), f"A TIME type dimension should be created from a dbt metric timestamp, but a `{objects.dimensions[dbt_metric.model.name][dbt_metric.timestamp]['type']}` was created"


def test_dbt_timestamp_to_dimension_missing_timestamp_issue(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    dbt_metrics[0].timestamp = None
    issues = DbtTimestampToDimension().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtTimestampToDimension didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_filters_to_dimensions(  # type: ignore[misc]  # noqa: D
    num_domesticated_cats_metric: Dict[str, Any],
    num_cats_with_max_age_close_to_average_age_metric: Dict[str, Any],
) -> None:
    objects = MappedObjects()
    simple_filter_metric = MetricNode(json_data=num_domesticated_cats_metric)
    issues = DbtFiltersToDimensions().run(dbt_metrics=(simple_filter_metric,), objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtFiltersToDimension raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    assert len(objects.dimensions[simple_filter_metric.model.name].keys()) == 1

    objects = MappedObjects()
    complex_filter_metric = MetricNode(json_data=num_cats_with_max_age_close_to_average_age_metric)
    issues = DbtFiltersToDimensions().run(dbt_metrics=(complex_filter_metric,), objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtFiltersToDimension raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    assert len(objects.dimensions[complex_filter_metric.model.name].keys()) == 2


def test_dbt_filters_to_dimensions_no_filters_okay(  # type: ignore[misc]  # noqa: D
    num_domesticated_cats_metric: Dict[str, Any]
) -> None:
    objects = MappedObjects()
    none_filter_metric = MetricNode(json_data=num_domesticated_cats_metric)
    none_filter_metric.filters = None
    issues = DbtFiltersToDimensions().run(dbt_metrics=(none_filter_metric,), objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtFiltersToDimension raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    assert (
        objects.dimensions.get(none_filter_metric.model.name) is None
    ), f"No filters should have been created, but {len(objects.dimensions[none_filter_metric.model.name].keys())} were created"
