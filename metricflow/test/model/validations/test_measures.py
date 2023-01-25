import re
import textwrap

import pytest
from metricflow.model.objects.common import YamlConfigFile
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.model_validator import ModelValidator
from metricflow.model.parsing.dir_to_model import parse_yaml_files_to_validation_ready_model
from metricflow.model.validations.measures import (
    CountAggregationExprRule,
    DataSourceMeasuresUniqueRule,
    MeasureConstraintAliasesRule,
    MeasuresNonAdditiveDimensionRule,
    MetricMeasuresRule,
)
from metricflow.model.validations.validator_helpers import ModelValidationException


def test_metric_missing_measure() -> None:
    """Tests the basic MetricMeasuresRule, which asserts all measure inputs to a metric exist in the model"""
    metric_name = "invalid_measure_metric_do_not_add_to_model"
    measure_name = "this_measure_cannot_exist_or_else_it_breaks_tests"

    yaml_contents = textwrap.dedent(
        f"""\
        ---
        metric:
          name: "{metric_name}"
          description: "Metric with invalid measure"
          type: expr
          type_params:
            measures:
              - {measure_name}
        """
    )
    metric_missing_measure_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([metric_missing_measure_file])

    with pytest.raises(
        ModelValidationException,
        match=f"Measure {measure_name} referenced in metric {metric_name} is not defined in the model!",
    ):
        ModelValidator([MetricMeasuresRule()]).checked_validations(model=model.model)


def test_measures_only_exist_in_one_data_source() -> None:  # noqa: D
    yaml_contents_1 = textwrap.dedent(
        """\
        data_source:
          name: sample_data_source
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
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
                is_primary: true
        """
    )
    base_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents_1)
    model = parse_yaml_files_to_validation_ready_model([base_file])
    build = ModelValidator().validate_model(model.model)
    duplicate_measure_message = "Found measure with name .* in multiple data sources with names"
    found_issue = False

    if build.issues is not None:
        for issue in build.issues.all_issues:
            if re.search(duplicate_measure_message, issue.message):
                found_issue = True

    assert found_issue is False

    yaml_contents_2 = textwrap.dedent(
        """\
        data_source:
          name: sample_data_source_2
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier_2
              type: primary
              role: test_role
              entity: other_identifier
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
                is_primary: true
        """
    )
    dup_measure_file = YamlConfigFile(filepath="inline_for_test_2", contents=yaml_contents_2)
    dup_model = parse_yaml_files_to_validation_ready_model([base_file, dup_measure_file])
    build = ModelValidator([DataSourceMeasuresUniqueRule()]).validate_model(dup_model.model)

    if build.issues is not None:
        for issue in build.issues.all_issues:
            if re.search(duplicate_measure_message, issue.message):
                found_issue = True

    assert found_issue is True


def test_measure_alias_is_set_when_required() -> None:
    """Tests to ensure that an appropriate error appears when a required alias is missing"""
    measure_name = "num_sample_rows"
    yaml_contents = textwrap.dedent(
        f"""\
        data_source:
          name: sample_data_source
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
              expr: example_id
          measures:
            - name: {measure_name}
              agg: sum
              expr: 1
              create_metric: true
          dimensions:
            - name: is_instant
              type: categorical
            - name: ds
              type: time
              type_params:
                time_granularity: day
                is_primary: true
        ---
        metric:
          name: "metric1"
          type: expr
          type_params:
            measures:
              - name: {measure_name}
              - name: {measure_name}
                constraint: is_instant
        """
    )
    missing_alias_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([missing_alias_file])

    build = ModelValidator([MeasureConstraintAliasesRule()]).validate_model(model.model)

    assert len(build.issues.errors) == 1
    expected_error_substring = f"depends on multiple different constrained versions of measure {measure_name}"
    actual_error = build.issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"


def test_invalid_measure_alias_name() -> None:
    """Tests measures with aliases that don't pass the unique and valid name rule"""
    invalid_alias = "_can't_do_this_"

    yaml_contents = textwrap.dedent(
        f"""\
        data_source:
          name: sample_data_source
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
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
                is_primary: true
        ---
        metric:
          name: "metric1"
          type: expr
          type_params:
            measures:
              - name: num_sample_rows
                alias: {invalid_alias}
        """
    )
    invalid_alias_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([invalid_alias_file])

    build = ModelValidator([MeasureConstraintAliasesRule()]).validate_model(model.model)

    assert len(build.issues.errors) == 1
    expected_error_substring = f"Invalid name `{invalid_alias}` - names should only consist of"
    actual_error = build.issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"


def test_measure_alias_measure_name_conflict() -> None:
    """Tests measures with aliases that already exist as measure names"""

    invalid_alias = "average_sample_rows"
    measure_name = "num_sample_rows"
    yaml_contents = textwrap.dedent(
        f"""\
        data_source:
          name: sample_data_source
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
              expr: example_id
          measures:
            - name: {measure_name}
              agg: sum
              expr: 1
            - name: {invalid_alias}
              agg: average
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
                is_primary: true
        ---
        metric:
          name: "metric1"
          type: expr
          type_params:
            measures:
              - name: {measure_name}
                alias: {invalid_alias}
        """
    )
    invalid_alias_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([invalid_alias_file])

    build = ModelValidator([MeasureConstraintAliasesRule()]).validate_model(model.model)

    assert len(build.issues.errors) == 1
    expected_error_substring = (
        f"Alias `{invalid_alias}` for measure `{measure_name}` conflicts with measure names "
        f"defined elsewhere in the model!"
    )
    actual_error = build.issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"


def test_reused_measure_alias() -> None:
    """Tests measures with aliases that have been used as measure aliases elsewhere in the model"""

    invalid_alias = "duplicate_alias"

    yaml_contents = textwrap.dedent(
        f"""\
        data_source:
          name: sample_data_source
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
              expr: example_id
          measures:
            - name: num_sample_rows
              agg: sum
              expr: 1
              create_metric: true
            - name: average_sample_rows
              agg: average
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
                is_primary: true
        ---
        metric:
          name: "metric1"
          type: expr
          type_params:
            measures:
              - name: num_sample_rows
                alias: {invalid_alias}
        ---
        metric:
          name: "metric2"
          type: expr
          type_params:
            measures:
              - name: average_sample_rows
                alias: {invalid_alias}
        """
    )
    invalid_alias_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([invalid_alias_file])

    build = ModelValidator([MeasureConstraintAliasesRule()]).validate_model(model.model)

    assert len(build.issues.errors) == 1
    expected_error_substring = (
        f"Measure alias {invalid_alias} conflicts with a measure alias used elsewhere in the model!"
    )
    actual_error = build.issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"


def test_reused_measure_alias_within_metric() -> None:
    """Tests measures with aliases that have been used as measure aliases in the same metric spec

    This covers a boundary case in the logic where alias checking must always include aliases from the current
    metric under consideration, instead of simply checking against the previous seen values
    """
    invalid_alias = "duplicate_alias"
    yaml_contents = textwrap.dedent(
        f"""\
        data_source:
          name: sample_data_source
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
              expr: example_id
          measures:
            - name: num_sample_rows
              agg: sum
              expr: 1
              create_metric: true
            - name: average_sample_rows
              agg: average
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
                is_primary: true
        ---
        metric:
          name: "metric1"
          type: expr
          type_params:
            measures:
              - name: num_sample_rows
                alias: {invalid_alias}
              - name: average_sample_rows
                alias: {invalid_alias}
        """
    )
    invalid_alias_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([invalid_alias_file])

    build = ModelValidator([MeasureConstraintAliasesRule()]).validate_model(model.model)

    assert len(build.issues.errors) == 1
    expected_error_substring = (
        f"Measure alias {invalid_alias} conflicts with a measure alias used elsewhere in the model!"
    )
    actual_error = build.issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"


def test_invalid_non_additive_dimension_properties() -> None:
    """Tests validator for invalid cases of non_additive_dimension properties."""

    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: sample_data_source_2
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
              expr: example_id
          measures:
            - name: bad_measure
              agg: sum
              agg_time_dimension: ds
              non_additive_dimension:
                name: doesntexist
                window_choice: min
                window_groupings:
                  - wherethisidentifier
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
                is_primary: true
            - name: weekly_time
              type: time
              type_params:
                time_granularity: week
            - name: is_instant
              type: categorical
        """
    )
    invalid_dim_file = YamlConfigFile(filepath="inline_for_test_2", contents=yaml_contents)
    model_build_result = parse_yaml_files_to_validation_ready_model(
        [invalid_dim_file], apply_transformations=False, raise_issues_as_exceptions=False
    )
    transformed_model = ModelTransformer.transform(
        model=model_build_result.model, ordered_rule_sequences=(ModelTransformer.PRIMARY_RULES,)
    )

    build = ModelValidator([MeasuresNonAdditiveDimensionRule()]).validate_model(transformed_model)
    expected_error_substring_1 = "that is not defined as a dimension in data source 'sample_data_source_2'."
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
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build.issues.errors):
            missing_error_strings.add(expected_str)
    assert (
        len(missing_error_strings) == 0
    ), f"Failed to match one or more expected errors: {missing_error_strings} in {set([x.as_readable_str() for x in build.issues.errors])}"


def test_count_measure_missing_expr() -> None:
    """Tests that all measures with COUNT agg should have expr provided."""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: sample_data_source_2
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
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
                is_primary: true
        """
    )
    missing_expr_file = YamlConfigFile(filepath="inline_for_test_2", contents=yaml_contents)
    model_build_result = parse_yaml_files_to_validation_ready_model([missing_expr_file], apply_transformations=False)
    transformed_model = ModelTransformer.transform(
        model=model_build_result.model, ordered_rule_sequences=(ModelTransformer.PRIMARY_RULES,)
    )

    build = ModelValidator([CountAggregationExprRule()]).validate_model(transformed_model)
    expected_error_substring = (
        "Measure 'bad_measure' uses a COUNT aggregation, which requires an expr to be provided. "
        "Provide 'expr: 1' if a count of all rows is desired."
    )
    assert len(build.issues.errors) == 1

    actual_error = build.issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"


def test_count_measure_with_distinct_expr() -> None:
    """Tests that measures with COUNT agg can NOT use the DISTINCT keyword."""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: sample_data_source_2
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
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
                is_primary: true
        """
    )
    distinct_count_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model_build_result = parse_yaml_files_to_validation_ready_model([distinct_count_file], apply_transformations=False)
    transformed_model = ModelTransformer.transform(
        model=model_build_result.model, ordered_rule_sequences=(ModelTransformer.PRIMARY_RULES,)
    )

    build = ModelValidator([CountAggregationExprRule()]).validate_model(transformed_model)
    expected_error_substring = "Measure 'distinct_count' uses a 'count' aggregation with a DISTINCT expr"

    assert len(build.issues.errors) == 1
    actual_error = build.issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"


def test_percentile_measure_missing_agg_params() -> None:
    """Tests that only measures with PERCENTILE agg should have percentile and discrete provided."""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: sample_data_source
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
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
                is_primary: true
        """
    )
    missing_agg_params_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([missing_agg_params_file])

    build = ModelValidator().validate_model(model.model)
    expected_error_substring_1 = (
        "Measure 'bad_measure_1' uses a PERCENTILE aggregation, which requires agg_params.percentile to be provided."
    )
    expected_error_substring_2 = (
        "Measure 'bad_measure_2' uses a PERCENTILE aggregation, which requires agg_params.percentile to be provided."
    )

    expected_error_substring_3 = "Measure 'bad_measure_3' with aggregation 'sum' uses agg_params (percentile) only relevant to Percentile measures."

    missing_error_strings = set()
    for expected_str in [expected_error_substring_1, expected_error_substring_2, expected_error_substring_3]:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build.issues.errors):
            missing_error_strings.add(expected_str)
    assert (
        len(missing_error_strings) == 0
    ), f"Failed to match one or more expected errors: {missing_error_strings} in {set([x.as_readable_str() for x in build.issues.errors])}"


def test_percentile_measure_bad_percentile_values() -> None:
    """Tests that all measures with PERCENTILE agg should have the correct percentile value range."""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: sample_data_source
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
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
                is_primary: true
        """
    )
    bad_percentile_values_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([bad_percentile_values_file])

    build = ModelValidator().validate_model(model.model)
    expected_error_substring_1 = (
        "Percentile aggregation parameter for measure 'bad_measure_1' is '1.0', but"
        + "must be between 0 and 1 (non-inclusive). For example, to indicate the 65th percentile value, set 'percentile: 0.65'. "
        + "For percentile values of 0, please use MIN, for percentile values of 1, please use MAX."
    )
    expected_error_substring_2 = (
        "Percentile aggregation parameter for measure 'bad_measure_2' is '-2.0', but"
        + "must be between 0 and 1 (non-inclusive). For example, to indicate the 65th percentile value, set 'percentile: 0.65'. "
        + "For percentile values of 0, please use MIN, for percentile values of 1, please use MAX."
    )

    missing_error_strings = set()
    for expected_str in [
        expected_error_substring_1,
        expected_error_substring_2,
    ]:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build.issues.errors):
            missing_error_strings.add(expected_str)
    assert (
        len(missing_error_strings) == 0
    ), f"Failed to match one or more expected errors: {missing_error_strings} in {set([x.as_readable_str() for x in build.issues.errors])}"
