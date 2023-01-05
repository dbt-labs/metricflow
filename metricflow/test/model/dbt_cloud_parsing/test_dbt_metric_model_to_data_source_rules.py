from typing import Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_converter import DbtConverter
from metricflow.model.dbt_mapping_rules.dbt_metric_model_to_data_source_rules import (
    DbtMapToDataSourceName,
    DbtMapToDataSourceDescription,
    DbtMapDataSourceSqlTable,
)
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import (
    MappedObjects,
    DbtMappingRule,
    get_and_assert_calc_method_mapping,
)
from metricflow.model.objects.metric import MetricType


def test_dbt_metric_model_to_data_source_rules_skip_derived_metrics(  # noqa: D
    dbt_metrics: Tuple[MetricNode, ...]
) -> None:
    derived_metrics = tuple(dbt_metric for dbt_metric in dbt_metrics if dbt_metric.calculation_method == "derived")
    rules: Tuple[DbtMappingRule, ...] = (
        DbtMapToDataSourceName(),
        DbtMapToDataSourceDescription(),
        DbtMapDataSourceSqlTable(),
    )
    converter = DbtConverter(rules=rules)
    result = converter._map_dbt_to_metricflow(dbt_metrics=derived_metrics)
    assert (
        len(result.mapped_objects.data_sources.keys()) == 0
    ), "Derived dbt metrics created data sources, and they shouldn't"


def test_dbt_map_to_data_source_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtMapToDataSourceName().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtMapToDataSourceName raised blocking issues when it shouldn't: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.data_sources[dbt_metric.model.name].get("name") is not None
            ), "Expected data source to have name, got `None`"


def test_dbt_map_to_data_source_name_missing_model_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    # Remove a model name from a metric
    dbt_metrics[0].model.name = None

    issues = DbtMapToDataSourceName().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtMapToDataSourceName didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_map_to_data_source_description(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtMapToDataSourceDescription().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtMapToDataSourceDescription raised blocking issues when it shouldn't: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.data_sources[dbt_metric.model.name]["description"] == dbt_metric.model.description
            ), f"Got description `{objects.data_sources[dbt_metric.model.name]['description']}`, but expected `{dbt_metric.model.description}`"


def test_dbt_map_to_data_source_description_can_be_optional(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    # Remove a model description from a metric
    dbt_metrics[0].model.description = None
    issues = DbtMapToDataSourceDescription().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtMapToDataSourceDescription raised blocking issues when it shouldn't: {issues.to_pretty_json()}"


def test_dbt_map_data_source_sql_table(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()

    issues = DbtMapDataSourceSqlTable().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtMapToDataSourceSqlTable raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.data_sources[dbt_metric.model.name]["sql_table"]
                == f"{dbt_metric.model.database}.{dbt_metric.model.schema}.{dbt_metric.model.name}"
            )


def test_dbt_map_data_source_sql_table_issues_when_missing_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    # remove model name
    dbt_metrics[0].model.name = None

    issues = DbtMapDataSourceSqlTable().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtMapToDataSourceSqlTable didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_map_data_source_sql_table_issues_when_missing_schema(  # noqa: D
    dbt_metrics: Tuple[MetricNode, ...]
) -> None:
    objects = MappedObjects()
    # remove model schema
    dbt_metrics[0].model.schema = None

    issues = DbtMapDataSourceSqlTable().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtMapToDataSourceSqlTable didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_map_data_source_sql_table_issues_when_missing_database(  # noqa: D
    dbt_metrics: Tuple[MetricNode, ...]
) -> None:
    objects = MappedObjects()
    # remove model database
    dbt_metrics[0].model.database = None

    issues = DbtMapDataSourceSqlTable().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtMapToDataSourceSqlTable didn't raise blocking issues when it should have: {issues.to_pretty_json()}"
