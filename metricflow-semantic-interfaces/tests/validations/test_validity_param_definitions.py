from __future__ import annotations

import textwrap

import pytest
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.test_utils import base_semantic_manifest_file
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.semantic_models import (
    SemanticModelValidityWindowRule,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)

from tests.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)


def test_validity_window_configuration() -> None:
    """Tests to ensure a semantic model with a properly configured validity window passes validation."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          primary_entity: some_primary_entity
          entities:
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
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    model_issues = SemanticManifestValidator[PydanticSemanticManifest]().validate_semantic_manifest(
        model.semantic_manifest
    )

    assert not model_issues.has_blocking_issues, (
        f"Found blocking issues validating model with validity window properly configured: "
        f"{[x.as_readable_str() for x in model_issues.errors]}"
    )


def test_validity_window_must_have_a_start() -> None:
    """Tests validation asserting a validity window end has a corresponding start."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          entities:
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
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    with pytest.raises(SemanticManifestValidationException, match="has 1 dimensions defined with validity params"):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelValidityWindowRule()]).checked_validations(
            model.semantic_manifest
        )


def test_validity_window_must_have_an_end() -> None:
    """Tests validation asserting a validity window start has a corresponding end."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          entities:
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
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    with pytest.raises(SemanticManifestValidationException, match="has 1 dimensions defined with validity params"):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelValidityWindowRule()]).checked_validations(
            model.semantic_manifest
        )


def test_validity_window_uses_two_dimensions() -> None:
    """Tests validation asserting validity window endpoints are defined in separate dimensions.

    Note: This test should be removed when support for single column validity window joins is added
    """
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          entities:
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
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    with pytest.raises(
        SemanticManifestValidationException, match="single validity param dimension that defines its window"
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelValidityWindowRule()]).checked_validations(
            model.semantic_manifest
        )


def test_two_dimension_validity_windows_must_not_overload_start_and_end() -> None:
    """Tests validation asserting that a validity window does not set is_start and is_end on one dimension."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          entities:
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
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    with pytest.raises(SemanticManifestValidationException, match="does not have exactly one each"):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelValidityWindowRule()]).checked_validations(
            model.semantic_manifest
        )


def test_multiple_validity_windows_are_invalid() -> None:
    """Tests validation asserting that no more than 1 validity window can exist in a semantic model."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          entities:
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
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    with pytest.raises(SemanticManifestValidationException, match="has 4 dimensions defined with validity params"):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelValidityWindowRule()]).checked_validations(
            model.semantic_manifest
        )


def test_empty_validity_windows_are_invalid() -> None:
    """Tests validation asserting that validity windows cannot be specified if they are empty."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          entities:
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
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    with pytest.raises(SemanticManifestValidationException, match="does not have exactly one each"):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelValidityWindowRule()]).checked_validations(
            model.semantic_manifest
        )


def test_measures_are_prevented() -> None:
    """Tests validation asserting that measures are not allowed in a semantic model with validity windows.

    This block is temporary while we sort out the proper syntax for defining a measure in SCD-style semantic models
    and implement whatever additional functionality is needed for measures which are semi-additive to the window.
    """
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          entities:
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
          measures:
            - name: num_countries
              agg: count_distinct
              agg_time_dimension: window_start
              expr: country
        """
    )
    validity_window_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    with pytest.raises(
        SemanticManifestValidationException, match="has both measures and validity param dimensions defined"
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelValidityWindowRule()]).checked_validations(
            model.semantic_manifest
        )


def test_validity_window_must_have_a_natural_key() -> None:
    """Tests validation asserting that semantic models with validity windows use an entity with type NATURAL."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          entities:
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
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    with pytest.raises(SemanticManifestValidationException, match="does not have an entity with type `natural` set"):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelValidityWindowRule()]).checked_validations(
            model.semantic_manifest
        )


def test_validity_window_does_not_use_primary_key() -> None:
    """Tests validation asserting that semantic models with validity windows do not use primary keys.

    This is useful because we currently do not support joins against SCD-style semantic models without using the
    validity window filter, and so enabling a primary key would be confusing. Subsequent changes may add support
    for this in which case we should of course remove this validation requirement.
    """
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_semantic_model
          node_relation:
            schema_name: some_schema
            alias: scd_table
          entities:
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
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), validity_window_file]
    )

    with pytest.raises(SemanticManifestValidationException, match="has one or more entities designated as `primary`"):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelValidityWindowRule()]).checked_validations(
            model.semantic_manifest
        )
