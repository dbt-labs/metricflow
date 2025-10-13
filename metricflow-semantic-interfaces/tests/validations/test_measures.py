from __future__ import annotations

import re
import textwrap

import pytest
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.transformations.pydantic_rule_set import (
    PydanticSemanticManifestTransformRuleSet,
)
from metricflow_semantic_interfaces.transformations.semantic_manifest_transformer import (
    PydanticSemanticManifestTransformer,
)
from metricflow_semantic_interfaces.validations.measures import (
    CountAggregationExprRule,
    MeasuresNonAdditiveDimensionRule,
    MetricMeasuresRule,
    SemanticModelMeasuresUniqueRule,
)
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)

from tests.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)


def test_metric_missing_measure() -> None:
    """Tests the basic MetricMeasuresRule, which asserts all measure inputs to a metric exist in the model."""
    metric_name = "invalid_measure_metric_do_not_add_to_model"
    measure_name = "this_measure_cannot_exist_or_else_it_breaks_tests"

    yaml_contents = textwrap.dedent(
        f"""\
        ---
        metric:
          name: "{metric_name}"
          description: "Metric with invalid measure"
          type: simple
          type_params:
            measure:
              name: {measure_name}
        """
    )
    metric_missing_measure_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, metric_missing_measure_file]
    )

    with pytest.raises(
        SemanticManifestValidationException,
        match=f"Measure {measure_name} referenced in metric {metric_name} is not defined in the model!",
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([MetricMeasuresRule()]).checked_validations(
            semantic_manifest=model.semantic_manifest
        )


def test_measures_only_exist_in_one_semantic_model() -> None:  # noqa: D103
    yaml_contents_1 = textwrap.dedent(
        """\
        semantic_model:
          name: sample_semantic_model
          node_relation:
            schema_name: some_schema
            alias: source_table
          defaults:
            agg_time_dimension: ds
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: num_sample_rows
              agg: sum
              expr: 1
              create_metric: true
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
        """
    )
    base_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents_1)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_file]
    )
    model_issues = SemanticManifestValidator[PydanticSemanticManifest]().validate_semantic_manifest(
        model.semantic_manifest
    )
    duplicate_measure_message = "Found measure with name .* in multiple semantic models with names"
    found_issue = False

    if model_issues is not None:
        for issue in model_issues.all_issues:
            if re.search(duplicate_measure_message, issue.message):
                found_issue = True

    assert found_issue is False

    yaml_contents_2 = textwrap.dedent(
        """\
        semantic_model:
          name: sample_semantic_model_2
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: example_entity_2
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: num_sample_rows
              agg: sum
              expr: 1
              create_metric: true
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
        """
    )
    dup_measure_file = YamlConfigFile(filepath="inline_for_test_2", contents=yaml_contents_2)
    dup_model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [
            EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
            base_file,
            dup_measure_file,
        ]
    )
    model_issues = SemanticManifestValidator[PydanticSemanticManifest](
        [SemanticModelMeasuresUniqueRule()]
    ).validate_semantic_manifest(dup_model.semantic_manifest)

    if model_issues is not None:
        for issue in model_issues.all_issues:
            if re.search(duplicate_measure_message, issue.message):
                found_issue = True

    assert found_issue is True


def test_invalid_non_additive_dimension_properties() -> None:
    """Tests validator for invalid cases of non_additive_dimension properties."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: sample_semantic_model_2
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: bad_measure
              agg: sum
              agg_time_dimension: ds
              non_additive_dimension:
                name: doesntexist
                window_choice: min
                window_groupings:
                  - wherethisentity
            - name: bad_measure2
              agg: sum
              agg_time_dimension: ds
              non_additive_dimension:
                name: is_instant
                window_choice: min
            - name: bad_measure3
              agg: sum
              agg_time_dimension: ds
              non_additive_dimension:
                name: weekly_time
                window_choice: min
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
            - name: weekly_time
              type: time
              type_params:
                time_granularity: week
            - name: is_instant
              type: categorical
        """
    )
    invalid_dim_file = YamlConfigFile(filepath="inline_for_test_2", contents=yaml_contents)
    model_build_result = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, invalid_dim_file],
        apply_transformations=False,
        raise_issues_as_exceptions=False,
    )
    transformed_model = PydanticSemanticManifestTransformer.transform(
        model=model_build_result.semantic_manifest,
        ordered_rule_sequences=(PydanticSemanticManifestTransformRuleSet().primary_rules,),
    )

    model_issues = SemanticManifestValidator[PydanticSemanticManifest](
        [MeasuresNonAdditiveDimensionRule()]
    ).validate_semantic_manifest(transformed_model)
    expected_error_substring_1 = "that is not defined as a dimension in semantic model 'sample_semantic_model_2'."
    expected_error_substring_2 = "has a non_additive_dimension with an invalid 'window_groupings'"
    expected_error_substring_3 = "that is defined as a categorical dimension which is not supported."
    expected_error_substring_4 = "that is not equal to the measure's agg_time_dimension"
    missing_error_strings = set()
    for expected_str in [
        expected_error_substring_1,
        expected_error_substring_2,
        expected_error_substring_3,
        expected_error_substring_4,
    ]:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in model_issues.errors):
            missing_error_strings.add(expected_str)
    assert len(missing_error_strings) == 0, (
        f"Failed to match one or more expected errors: {missing_error_strings} in "
        f"{set([x.as_readable_str() for x in model_issues.errors])}"
    )


def test_count_measure_missing_expr() -> None:
    """Tests that all measures with COUNT agg should have expr provided."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: sample_semantic_model_2
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: num_sample_rows
              agg: sum
              expr: 1
              create_metric: true
            - name: bad_measure
              agg: count
              agg_time_dimension: ds
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
        """
    )
    missing_expr_file = YamlConfigFile(filepath="inline_for_test_2", contents=yaml_contents)
    model_build_result = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, missing_expr_file], apply_transformations=False
    )
    transformed_model = PydanticSemanticManifestTransformer.transform(
        model=model_build_result.semantic_manifest,
        ordered_rule_sequences=(PydanticSemanticManifestTransformRuleSet().primary_rules,),
    )

    model_issues = SemanticManifestValidator[PydanticSemanticManifest](
        [CountAggregationExprRule()]
    ).validate_semantic_manifest(transformed_model)
    expected_error_substring = (
        "Measure 'bad_measure' uses a COUNT aggregation, which requires an expr to be provided. "
        "Provide 'expr: 1' if a count of all rows is desired."
    )
    assert len(model_issues.errors) == 1

    actual_error = model_issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"


def test_count_measure_with_distinct_expr() -> None:
    """Tests that measures with COUNT agg can NOT use the DISTINCT keyword."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: sample_semantic_model_2
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: num_sample_rows
              agg: sum
              expr: 1
              create_metric: true
            - name: distinct_count
              agg: count
              agg_time_dimension: ds
              expr: DISTINCT listing
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
        """
    )
    distinct_count_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model_build_result = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, distinct_count_file], apply_transformations=False
    )
    transformed_model = PydanticSemanticManifestTransformer.transform(
        model=model_build_result.semantic_manifest,
        ordered_rule_sequences=(PydanticSemanticManifestTransformRuleSet().primary_rules,),
    )

    model_issues = SemanticManifestValidator[PydanticSemanticManifest](
        [CountAggregationExprRule()]
    ).validate_semantic_manifest(transformed_model)
    expected_error_substring = "Measure 'distinct_count' uses a 'count' aggregation with a DISTINCT expr"

    assert len(model_issues.errors) == 1
    actual_error = model_issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"


def test_percentile_measure_missing_agg_params() -> None:
    """Tests that only measures with PERCENTILE agg should have percentile and discrete provided."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: sample_semantic_model
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: bad_measure_1
              agg: percentile
              agg_time_dimension: ds
            - name: bad_measure_2
              agg: percentile
              agg_time_dimension: ds
              agg_params: {}
            - name: bad_measure_3
              agg: sum
              agg_time_dimension: ds
              agg_params:
                percentile: 0.3
          dimensions:
            - name: is_instant
              type: categorical
            - name: ds
              type: time
              type_params:
                time_granularity: day
        """
    )
    missing_agg_params_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [
            EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
            missing_agg_params_file,
        ]
    )

    model_issues = SemanticManifestValidator[PydanticSemanticManifest]().validate_semantic_manifest(
        model.semantic_manifest
    )
    expected_error_substring_1 = (
        "Measure 'bad_measure_1' uses a PERCENTILE aggregation, which requires agg_params.percentile to be provided."
    )
    expected_error_substring_2 = (
        "Measure 'bad_measure_2' uses a PERCENTILE aggregation, which requires agg_params.percentile to be provided."
    )

    expected_error_substring_3 = (
        "Measure 'bad_measure_3' with aggregation 'sum' uses agg_params (percentile) only relevant to Percentile "
        "measures."
    )

    missing_error_strings = set()
    for expected_str in [expected_error_substring_1, expected_error_substring_2, expected_error_substring_3]:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in model_issues.errors):
            missing_error_strings.add(expected_str)
    assert len(missing_error_strings) == 0, (
        f"Failed to match one or more expected errors: {missing_error_strings} in "
        f"{set([x.as_readable_str() for x in model_issues.errors])}"
    )


def test_percentile_measure_bad_percentile_values() -> None:
    """Tests that all measures with PERCENTILE agg should have the correct percentile value range."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: sample_semantic_model
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: bad_measure_1
              agg: percentile
              agg_time_dimension: ds
              agg_params:
                percentile: 1
            - name: bad_measure_2
              agg: percentile
              agg_time_dimension: ds
              agg_params:
                percentile: -2.0
          dimensions:
            - name: is_instant
              type: categorical
            - name: ds
              type: time
              type_params:
                time_granularity: day
        """
    )
    bad_percentile_values_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [
            EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
            bad_percentile_values_file,
        ]
    )

    model_issues = SemanticManifestValidator[PydanticSemanticManifest]().validate_semantic_manifest(
        model.semantic_manifest
    )
    expected_error_substring_1 = (
        "Percentile aggregation parameter for measure 'bad_measure_1' is '1.0', but "
        + "must be between 0 and 1 (non-inclusive). For example, to indicate the 65th percentile value, set "
        + "'percentile: 0.65'. For percentile values of 0, please use MIN, for percentile values of 1, please use MAX."
    )
    expected_error_substring_2 = (
        "Percentile aggregation parameter for measure 'bad_measure_2' is '-2.0', but "
        + "must be between 0 and 1 (non-inclusive). For example, to indicate the 65th percentile value, set "
        + "'percentile: 0.65'. For percentile values of 0, please use MIN, for percentile values of 1, please use MAX."
    )

    missing_error_strings = set()
    for expected_str in [
        expected_error_substring_1,
        expected_error_substring_2,
    ]:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in model_issues.errors):
            missing_error_strings.add(expected_str)
    assert len(missing_error_strings) == 0, (
        f"Failed to match one or more expected errors: {missing_error_strings} in "
        f"{set([x.as_readable_str() for x in model_issues.errors])}"
    )
