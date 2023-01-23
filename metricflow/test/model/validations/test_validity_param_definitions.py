import pytest
import textwrap

from metricflow.model.objects.common import YamlConfigFile
from metricflow.model.parsing.dir_to_model import parse_yaml_files_to_validation_ready_model
from metricflow.model.model_validator import ModelValidator
from metricflow.model.validations.data_sources import DataSourceValidityWindowRule
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.test.model.validations.helpers import base_model_file


def test_validity_window_configuration() -> None:
    """Tests to ensure a data source with a properly configured validity window passes validation"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_key
              type: natural
          dimensions:
            - name: country
              type: categorical
            - name: window_start
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
            - name: window_end
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: true
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    validation_result = ModelValidator().validate_model(model.model)

    assert not validation_result.issues.has_blocking_issues, (
        f"Found blocking issues validating model with validity window properly configured: "
        f"{[x.as_readable_str() for x in validation_result.issues.errors]}"
    )


def test_validity_window_must_have_a_start() -> None:
    """Tests validation asserting a validity window end has a corresponding start"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_key
              type: natural
          dimensions:
            - name: country
              type: categorical
            - name: window_end
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: true
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    with pytest.raises(ModelValidationException, match="has 1 dimensions defined with validity params"):
        ModelValidator([DataSourceValidityWindowRule()]).checked_validations(model.model)


def test_validity_window_must_have_an_end() -> None:
    """Tests validation asserting a validity window start has a corresponding end"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_key
              type: natural
          dimensions:
            - name: country
              type: categorical
            - name: window_start
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    with pytest.raises(ModelValidationException, match="has 1 dimensions defined with validity params"):
        ModelValidator([DataSourceValidityWindowRule()]).checked_validations(model.model)


def test_validity_window_uses_two_dimensions() -> None:
    """Tests validation asserting validity window endpoints are defined in separate dimensions

    Note: This test should be removed when support for single column validity window joins is added
    """
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_key
              type: natural
          dimensions:
            - name: country
              type: categorical
            - name: window
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
                  is_end: true
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    with pytest.raises(ModelValidationException, match="single validity param dimension that defines its window"):
        ModelValidator([DataSourceValidityWindowRule()]).checked_validations(model.model)


def test_two_dimension_validity_windows_must_not_overload_start_and_end() -> None:
    """Tests validation asserting that a validity window does not set is_start and is_end on one dimension"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_key
              type: natural
          dimensions:
            - name: country
              type: categorical
            - name: window_start
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
            - name: window_end
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
                  is_end: true
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    with pytest.raises(ModelValidationException, match="does not have exactly one each"):
        ModelValidator([DataSourceValidityWindowRule()]).checked_validations(model.model)


def test_multiple_validity_windows_are_invalid() -> None:
    """Tests validation asserting that no more than 1 validity window can exist in a data source"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_key
              type: natural
          dimensions:
            - name: country
              type: categorical
            - name: window_start_one
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
            - name: window_end_one
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: true
            - name: window_start_two
              type: time
              type_params:
                time_granularity: week
                validity_params:
                  is_start: true
            - name: window_end_two
              type: time
              type_params:
                time_granularity: week
                validity_params:
                  is_end: true
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    with pytest.raises(ModelValidationException, match="has 4 dimensions defined with validity params"):
        ModelValidator([DataSourceValidityWindowRule()]).checked_validations(model.model)


def test_empty_validity_windows_are_invalid() -> None:
    """Tests validation asserting that validity windows cannot be specified if they are empty"""

    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_key
              type: natural
          dimensions:
            - name: country
              type: categorical
            - name: window_start
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: false
            - name: window_end
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: false
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    with pytest.raises(ModelValidationException, match="does not have exactly one each"):
        ModelValidator([DataSourceValidityWindowRule()]).checked_validations(model.model)


def test_measures_are_prevented() -> None:
    """Tests validation asserting that measures are not allowed in a data source with validity windows

    This block is temporary while we sort out the proper syntax for defining a measure in SCD-style data sources
    and implement whatever additional functionality is needed for measures which are semi-additive to the window.
    """

    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_key
              type: natural
          dimensions:
            - name: country
              type: categorical
            - name: window_start
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
                is_primary: true
            - name: window_end
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: true
          measures:
            - name: num_countries
              agg: count_distinct
              expr: country
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    with pytest.raises(ModelValidationException, match="has both measures and validity param dimensions defined"):
        ModelValidator([DataSourceValidityWindowRule()]).checked_validations(model.model)


def test_validity_window_must_have_a_natural_key() -> None:
    """Tests validation asserting that data sources with validity windows use an identifier with type NATURAL"""

    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_key
              type: unique
          dimensions:
            - name: country
              type: categorical
            - name: window_start
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
            - name: window_end
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: true
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    with pytest.raises(ModelValidationException, match="does not have an identifier with type `natural` set"):
        ModelValidator([DataSourceValidityWindowRule()]).checked_validations(model.model)


def test_validity_window_does_not_use_primary_key() -> None:
    """Tests validation asserting that data sources with validity windows do not use primary keys

    This is useful because we currently do not support joins against SCD-style data sources without using the
    validity window filter, and so enabling a primary key would be confusing. Subsequent changes may add support
    for this in which case we should of course remove this validation requirement.
    """

    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_data_source
          sql_table: some_schema.scd_table
          identifiers:
            - name: scd_primary_key
              type: primary
            - name: scd_key
              type: natural
          dimensions:
            - name: country
              type: categorical
            - name: window_start
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
            - name: window_end
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: true
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), validity_window_file])

    with pytest.raises(ModelValidationException, match="has one or more identifiers designated as `primary`"):
        ModelValidator([DataSourceValidityWindowRule()]).checked_validations(model.model)
