from __future__ import annotations

import textwrap

from metricflow_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.test_utils import base_semantic_manifest_file

from tests.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)


def test_cumulative_metric_with_custom_grain_to_date() -> None:  # noqa: D103
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: booking_source
          node_relation:
            schema_name: some_schema
            alias: some_alias
          primary_entity: booking

          entities:
            - name: user
              type: foreign
              expr: user_id
          dimensions:
            - name: is_instant
              type: categorical
          measures:
            - name: bookings
              expr: "1"
              agg: sum
        ---
        metric:
          name: "bookings"
          description: "bookings metric"
          label: "Bookings"
          type: simple
          type_params:
            measure:
              name: bookings
        ---
        metric:
          name: "test_cumulative_metric_with_custom_grain_to_date"
          description: "Test cumulative grain to date with custom granularity"
          type: cumulative
          type_params:
            measure:
              name: bookings
              fill_nulls_with: 15
            cumulative_type_params:
              grain_to_date: martian_week
        """
    )
    metric_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, metric_file]
    )
    assert not model.issues.has_blocking_issues
    semantic_manifest = model.semantic_manifest
    # 2 explicit ones and one that is created for the measure input for the
    # cumulative metric's params
    assert len(semantic_manifest.metrics) == 3

    metric = next(
        (m for m in semantic_manifest.metrics if m.name == "test_cumulative_metric_with_custom_grain_to_date"), None
    )
    assert metric is not None, "Can't find metric"
    assert (
        metric.type_params.cumulative_type_params
        and metric.type_params.cumulative_type_params.grain_to_date == "martian_week"
    )


def test_cumulative_metric_with_custom_window() -> None:  # noqa: D103
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: booking_source
          node_relation:
            schema_name: some_schema
            alias: some_alias
          primary_entity: booking

          entities:
            - name: user
              type: foreign
              expr: user_id
          dimensions:
            - name: is_instant
              type: categorical
          measures:
            - name: bookings
              expr: "1"
              agg: sum
        ---
        metric:
          name: "bookings"
          description: "bookings metric"
          label: "Bookings"
          type: simple
          type_params:
            measure:
              name: bookings
        ---
        metric:
          name: "test_cumulative_metric_with_custom_window"
          description: "Test cumulative window with custom granularity"
          type: cumulative
          time_granularity: martian_week
          type_params:
            measure:
              name: bookings
            cumulative_type_params:
              window: 5 martian_weeks
        """
    )
    metric_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, metric_file]
    )
    assert not model.issues.has_blocking_issues
    semantic_manifest = model.semantic_manifest
    # 2 explicit metrics.  The cumulative metric's input metric should be deduplicated
    # so it will match.
    assert len(semantic_manifest.metrics) == 2

    metric = next((m for m in semantic_manifest.metrics if m.name == "test_cumulative_metric_with_custom_window"), None)
    assert metric is not None, "Can't find metric"
    # Should have gotten rid of the trailing 's' from the transformations
    assert (
        metric.type_params.cumulative_type_params
        and metric.type_params.cumulative_type_params.window
        and metric.type_params.cumulative_type_params.window.window_string == "5 martian_week"
    )


def test_conversion_metric_with_custom_grain_window() -> None:  # noqa: D103
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: booking_source
          node_relation:
            schema_name: some_schema
            alias: some_alias
          primary_entity: booking

          entities:
            - name: user
              type: foreign
              expr: user_id
          dimensions:
            - name: is_instant
              type: categorical
          measures:
            - name: bookings
              expr: "1"
              agg: sum
        ---
        metric:
          name: "bookings"
          description: "bookings metric"
          label: "Bookings"
          type: simple
          type_params:
            measure:
              name: bookings
        ---
        metric:
          name: "test_conversion_metric_with_custom_grain_window"
          description: "Test conversion metric window with custom granularity"
          type: conversion
          type_params:
            conversion_type_params:
              base_measure: bookings
              conversion_measure: bookings
              window: 7 martian_weeks
              entity: user
              calculation: conversion_rate
        """
    )
    metric_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, metric_file]
    )
    assert not model.issues.has_blocking_issues
    semantic_manifest = model.semantic_manifest
    # 2 explicitly created metrics.  The conversion measure -> metric transformation
    # should not need to create a new metric since the existing one already matches.
    assert len(semantic_manifest.metrics) == 2

    metric = next(
        (m for m in semantic_manifest.metrics if m.name == "test_conversion_metric_with_custom_grain_window"), None
    )
    assert metric is not None, "Can't find metric"
    # Should have gotten rid of the trailing 's' from the transformations
    assert (
        metric.type_params.conversion_type_params
        and metric.type_params.conversion_type_params.window
        and metric.type_params.conversion_type_params.window.window_string == "7 martian_week"
    )


def test_input_metric_custom_grains() -> None:  # noqa: D103
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: booking_source
          node_relation:
            schema_name: some_schema
            alias: some_alias
          primary_entity: booking

          entities:
            - name: user
              type: foreign
              expr: user_id
          dimensions:
            - name: is_instant
              type: categorical
          measures:
            - name: bookings
              expr: "1"
              agg: sum
        ---
        metric:
          name: "bookings"
          description: "bookings metric"
          label: "Bookings"
          type: simple
          type_params:
            measure:
              name: bookings
        ---
        metric:
          name: "test_input_metric_custom_grains"
          description: "Test custom granularity support in input metrics"
          type: derived
          type_params:
            expr: bookings_offset_to_grain - bookings_offset_window
            metrics:
              - name: bookings
                offset_to_grain: martian_week
                alias: bookings_offset_to_grain
              - name: bookings
                offset_window: 14 martian_weeks
                alias: bookings_offset_window
        """
    )

    metric_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), metric_file]
    )
    assert not model.issues.has_blocking_issues
    semantic_manifest = model.semantic_manifest
    assert len(semantic_manifest.metrics) == 3

    metric = next((m for m in semantic_manifest.metrics if m.name == "test_input_metric_custom_grains"), None)
    assert metric is not None, "Can't find metric"
    # Should have gotten rid of the trailing 's' from the transformations
    assert metric.type_params.metrics and {
        m.offset_to_grain or (m.offset_window.window_string if m.offset_window else None)
        for m in metric.type_params.metrics
    } == {
        "martian_week",
        "14 martian_week",
    }
