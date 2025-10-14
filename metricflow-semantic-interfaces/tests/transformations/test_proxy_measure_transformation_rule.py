from __future__ import annotations

import textwrap
from typing import List, Optional

import pytest
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInputMeasure,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.parsing.dir_to_model import (
    SemanticManifestBuildResult,
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.type_enums.metric_type import MetricType

from tests.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)


def test_measure_with_create_metric_generates_metric_with_equivalent_agg_params() -> None:
    """Test that a measure with create_metric: true generates a metric with equivalent agg params."""
    primary_entity_name = "example_entity"
    measure_name = "my_sum_measure"
    measure_agg = "percentile"
    measure_expr = "this_expr"
    measure_agg_time_dimension = "ds"
    semantic_model_name = "proxy_measure_test_model"
    measure_non_additive_dimension_time_dimension = "ds"
    measure_non_additive_dimension = "is_instant"
    measure_non_additive_dimension_window_choice = "min"
    measure_non_additive_dimension_window_grouping = primary_entity_name
    measure_description = "A beautiful description for a beautiful measure!"
    measure_label = "Read me, human!  I'm a label!"

    yaml_contents = textwrap.dedent(
        f"""\
        semantic_model:
          name: {semantic_model_name}
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: {primary_entity_name}
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: {measure_name}
              description: {measure_description}
              label: {measure_label}
              agg: {measure_agg}
              agg: percentile
              agg_params:
                percentile: 0.99
              agg_time_dimension: {measure_agg_time_dimension}
              expr: {measure_expr}
              non_additive_dimension:
                name: {measure_non_additive_dimension}
                window_choice: {measure_non_additive_dimension_window_choice}
                window_groupings:
                  - {measure_non_additive_dimension_window_grouping}
              config:
                meta:
                  text_tag_1: whoot
                  text_tag_2: beepboop
              create_metric: true
          dimensions:
            - name: {measure_agg_time_dimension}
              type: time
              type_params:
                time_granularity: day
            - name: {measure_non_additive_dimension_time_dimension}
              type: time
              type_params:
                time_granularity: day
            - name: {measure_non_additive_dimension}
              type: time
              type_params:
                time_granularity: day
        """
    )
    yaml_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model_build_result: SemanticManifestBuildResult = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, yaml_file],
        apply_transformations=True,
        raise_issues_as_exceptions=True,
    )

    # Sanity check - there should exactly 1 measure and 1 metric
    measures: List[PydanticMeasure] = []
    manifest: PydanticSemanticManifest = model_build_result.semantic_manifest
    for sm in manifest.semantic_models:
        measures.extend(sm.measures)
    metrics: List[PydanticMetric] = manifest.metrics
    assert len(measures) == 1
    assert len(metrics) == 1

    # Get the metric we expect with correct metadata
    metric = metrics[0]
    assert metric.name == measure_name, "Metric was constructed with the wrong name."
    assert metric.type == MetricType.SIMPLE, "Constructed metric MUST be a simple metric."
    assert metric.description == measure_description, "Metric was constructed with the wrong description."
    assert metric.label == measure_label, "Metric was constructed with the wrong label."
    assert metric.config is not None, "Metric should have a config, but it is missing."
    meta = metric.config.meta
    assert meta is not None, "Metric should have a 'meta' value in its config."
    assert meta.get("text_tag_1") == "whoot", "Metric was constructed with the wrong config values."
    assert meta.get("text_tag_2") == "beepboop", "Metric was constructed with the wrong config values."

    # Metric fields that specific to metrics that replace measures
    metric_agg_params: Optional[PydanticMetricAggregationParams] = metric.type_params.metric_aggregation_params
    assert metric_agg_params is not None, "Metric aggregation params were not populated but they should have been."
    assert (
        metric_agg_params.semantic_model == semantic_model_name
    ), "Metric was constructed with the wrong semantic model name."
    assert metric_agg_params.agg.value == measure_agg, "Metric was constructed with the wrong aggregation type."
    assert (
        metric_agg_params.agg_time_dimension == measure_agg_time_dimension
    ), "Metric was constructed with the wrong agg time dimension."
    non_additive_dimension = metric_agg_params.non_additive_dimension
    assert non_additive_dimension is not None, "Metric should have been constructed with a non-additive dimension."
    assert (
        non_additive_dimension.name == measure_non_additive_dimension
    ), "Metric was constructed with the wrong non-additive dimension name."
    assert (
        non_additive_dimension.window_choice.value == measure_non_additive_dimension_window_choice
    ), "Metric was constructed with the wrong non-additive dimension window choice."
    assert non_additive_dimension.window_groupings == [
        measure_non_additive_dimension_window_grouping
    ], "Metric was constructed with the wrong non-additive dimension window groupings."

    assert metric.type_params.expr == measure_expr, "Metric was constructed with the wrong expr."

    # Finally, make sure it still references the measure
    assert metric.type_params.measure == PydanticMetricInputMeasure(
        name=measure_name
    ), "Metric was constructed with the wrong measure."


@pytest.mark.parametrize(
    "measure_expr,expected_expr",
    [
        ("this_expr", "this_expr"),
        (None, "my_sum_measure"),
    ],
)
def test_proxy_measure_expr_population(measure_expr: Optional[str], expected_expr: str) -> None:
    """Proxy measure should set expr from measure.expr or fall back to the measure name."""
    primary_entity_name = "example_entity"
    measure_name = "my_sum_measure"
    measure_agg = "sum"
    measure_agg_time_dimension = "ds"
    semantic_model_name = "proxy_measure_expr_test_model"

    expr_line = f"\n              expr: {measure_expr}" if measure_expr is not None else ""

    yaml_contents = textwrap.dedent(
        f"""\
        semantic_model:
          name: {semantic_model_name}
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: {primary_entity_name}
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: {measure_name}
              agg: {measure_agg}{expr_line}
              agg_time_dimension: {measure_agg_time_dimension}
              create_metric: true
          dimensions:
            - name: {measure_agg_time_dimension}
              type: time
              type_params:
                time_granularity: day
        """
    )
    yaml_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model_build_result: SemanticManifestBuildResult = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, yaml_file],
        apply_transformations=True,
        raise_issues_as_exceptions=True,
    )

    manifest: PydanticSemanticManifest = model_build_result.semantic_manifest
    metrics: List[PydanticMetric] = manifest.metrics
    assert len(metrics) == 1, "Expected exactly one metric after transformation"
    metric = metrics[0]

    # expr should match expected
    assert metric.type_params.expr == expected_expr
    # Always should be true for proxy-created metrics
    assert metric.type_params.join_to_timespine is False
    assert metric.type_params.fill_nulls_with is None
    assert metric.filter is None
    # measure pointer should be present
    assert metric.type_params.measure == PydanticMetricInputMeasure(name=measure_name)


def test_proxy_measure_defaults_and_agg_params() -> None:
    """Proxy-created metric should have correct agg params, defaults, and is_private False."""
    primary_entity_name = "example_entity"
    measure_name = "my_percentile_measure"
    measure_agg = "percentile"
    measure_agg_time_dimension = "event_time"
    semantic_model_name = "proxy_measure_defaults_test_model"

    yaml_contents = textwrap.dedent(
        f"""\
        semantic_model:
          name: {semantic_model_name}
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: {primary_entity_name}
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: {measure_name}
              agg: {measure_agg}
              agg_params:
                percentile: 0.5
              agg_time_dimension: {measure_agg_time_dimension}
              expr: amount
              create_metric: true
          dimensions:
            - name: {measure_agg_time_dimension}
              type: time
              type_params:
                time_granularity: day
        """
    )
    yaml_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model_build_result: SemanticManifestBuildResult = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, yaml_file],
        apply_transformations=True,
        raise_issues_as_exceptions=True,
    )

    manifest: PydanticSemanticManifest = model_build_result.semantic_manifest
    metrics: List[PydanticMetric] = manifest.metrics
    assert len(metrics) == 1, "Expected exactly one metric after transformation"
    metric = metrics[0]

    # Aggregation params copied from measure
    metric_agg_params: Optional[PydanticMetricAggregationParams] = metric.type_params.metric_aggregation_params
    assert metric_agg_params is not None
    assert metric_agg_params.semantic_model == semantic_model_name
    assert metric_agg_params.agg.value == measure_agg
    assert metric_agg_params.agg_time_dimension == measure_agg_time_dimension

    # expr copied from measure
    assert metric.type_params.expr == "amount"

    # Defaults for proxy-created metrics
    assert (
        metric.type_params.join_to_timespine is False
    ), "Created metrics should always have a join_to_timespine set to False"
    assert metric.type_params.fill_nulls_with is None, "Created metrics should never have a value for fill_nulls_with"
    assert metric.filter is None, "Proxy-created metrics should never have a filter"
    assert metric.type_params.is_private is False
    # measure pointer should be present
    assert metric.type_params.measure == PydanticMetricInputMeasure(
        name=measure_name
    ), "Created metrics should always have a measure input"
    assert metric.type_params.input_measures == [
        PydanticMetricInputMeasure(name=measure_name)
    ], "Created metrics should have measure_inputs populated"
