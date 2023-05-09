from typing import Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_converter import DbtConverter
from metricflow.model.dbt_mapping_rules.dbt_metric_model_to_semantic_model_rules import (
    DbtMapToSemanticModelName,
    DbtMapToSemanticModelDescription,
    DbtMapSemanticModelNodeRelation,
)
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import (
    MappedObjects,
    DbtMappingRule,
    get_and_assert_calc_method_mapping,
)
from dbt_semantic_interfaces.objects.metric import MetricType


def test_dbt_metric_model_to_semantic_model_rules_skip_derived_metrics(  # noqa: D
    dbt_metrics: Tuple[MetricNode, ...]
) -> None:
    derived_metrics = tuple(dbt_metric for dbt_metric in dbt_metrics if dbt_metric.calculation_method == "derived")
    rules: Tuple[DbtMappingRule, ...] = (
        DbtMapToSemanticModelName(),
        DbtMapToSemanticModelDescription(),
        DbtMapSemanticModelNodeRelation(),
    )
    converter = DbtConverter(rules=rules)
    result = converter._map_dbt_to_metricflow(dbt_metrics=derived_metrics)
    assert (
        len(result.mapped_objects.semantic_models.keys()) == 0
    ), "Derived dbt metrics created semantic models, and they shouldn't"


def test_dbt_map_to_semantic_model_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtMapToSemanticModelName().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtMapToSemanticModelName raised blocking issues when it shouldn't: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.semantic_models[dbt_metric.model.name].get("name") is not None
            ), "Expected semantic model to have name, got `None`"


def test_dbt_map_to_semantic_model_name_missing_model_name(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    # Remove a model name from a metric
    dbt_metrics[0].model.name = None

    issues = DbtMapToSemanticModelName().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtMapToSemanticModelName didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_map_to_semantic_model_description(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    issues = DbtMapToSemanticModelDescription().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtMapToSemanticModelDescription raised blocking issues when it shouldn't: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.semantic_models[dbt_metric.model.name]["description"] == dbt_metric.model.description
            ), f"Got description `{objects.semantic_models[dbt_metric.model.name]['description']}`, but expected `{dbt_metric.model.description}`"


def test_dbt_map_to_semantic_model_description_can_be_optional(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()
    # Remove a model description from a metric
    dbt_metrics[0].model.description = None
    issues = DbtMapToSemanticModelDescription().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtMapToSemanticModelDescription raised blocking issues when it shouldn't: {issues.to_pretty_json()}"


def test_dbt_map_semantic_model_sql_table(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    objects = MappedObjects()

    issues = DbtMapSemanticModelNodeRelation().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        not issues.has_blocking_issues
    ), f"DbtMapToSemanticModelSqlTable raised blocking issues when it shouldn't have: {issues.to_pretty_json()}"
    for dbt_metric in dbt_metrics:
        metric_type = get_and_assert_calc_method_mapping(dbt_metric=dbt_metric)
        if metric_type != MetricType.DERIVED:
            assert (
                objects.semantic_models[dbt_metric.model.name]["node_relation"].relation_name
                == f"{dbt_metric.model.database}.{dbt_metric.model.schema}.{dbt_metric.model.name}"
            )


def test_dbt_map_semantic_model_sql_table_issues_when_missing_name(  # noqa: D
    dbt_metrics: Tuple[MetricNode, ...]
) -> None:
    objects = MappedObjects()
    # remove model name
    dbt_metrics[0].model.name = None

    issues = DbtMapSemanticModelNodeRelation().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtMapToSemanticModelSqlTable didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_map_semantic_model_sql_table_issues_when_missing_schema(  # noqa: D
    dbt_metrics: Tuple[MetricNode, ...]
) -> None:
    objects = MappedObjects()
    # remove model schema
    dbt_metrics[0].model.schema = None

    issues = DbtMapSemanticModelNodeRelation().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtMapToSemanticModelSqlTable didn't raise blocking issues when it should have: {issues.to_pretty_json()}"


def test_dbt_map_semantic_model_sql_table_issues_when_missing_database(  # noqa: D
    dbt_metrics: Tuple[MetricNode, ...]
) -> None:
    objects = MappedObjects()
    # remove model database
    dbt_metrics[0].model.database = None

    issues = DbtMapSemanticModelNodeRelation().run(dbt_metrics=dbt_metrics, objects=objects)
    assert (
        issues.has_blocking_issues
    ), f"DbtMapToSemanticModelSqlTable didn't raise blocking issues when it should have: {issues.to_pretty_json()}"
