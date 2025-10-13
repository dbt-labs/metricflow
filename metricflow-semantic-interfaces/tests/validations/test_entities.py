from __future__ import annotations

import copy
import re
import textwrap

import pytest
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.test_utils import (
    base_semantic_manifest_file,
    find_semantic_model_with,
)
from metricflow_semantic_interfaces.type_enums import EntityType
from metricflow_semantic_interfaces.validations.entities import NaturalEntityConfigurationRule
from metricflow_semantic_interfaces.validations.primary_entity import PrimaryEntityRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)

from tests.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)


def test_semantic_model_cant_have_more_than_one_primary_entity(
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:  # noqa: D103
    """Add an additional primary entity to a semantic model and assert that it cannot have two."""
    model = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    def func(semantic_model: PydanticSemanticModel) -> bool:
        return len(semantic_model.entities) > 1

    multiple_entity_semantic_model, _ = find_semantic_model_with(model, func)

    entity_references = set()
    for entity in multiple_entity_semantic_model.entities:
        entity.type = EntityType.PRIMARY
        entity_references.add(entity.reference)

    model_issues = SemanticManifestValidator[PydanticSemanticManifest](
        [PrimaryEntityRule[PydanticSemanticManifest]()]
    ).validate_semantic_manifest(model)

    expected_issue_message = (
        f"Semantic models can have only one primary entity. The semantic model"
        f" `{multiple_entity_semantic_model.name}` has {len(entity_references)}"
    )

    assert len(
        tuple(
            issue for issue in model_issues.all_issues if re.search(expected_issue_message, issue.message) is not None
        )
    )


def test_multiple_natural_entities() -> None:
    """Test validation enforcing that a single semantic model cannot have more than one natural entity."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: too_many_natural_entities
          node_relation:
            schema_name: some_schema
            alias: natural_entity_table
          entities:
            - name: natural_key_one
              type: natural
            - name: natural_key_two
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
    natural_entity_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), natural_entity_file]
    )

    with pytest.raises(SemanticManifestValidationException, match="can have at most one natural entity"):
        SemanticManifestValidator[PydanticSemanticManifest]([NaturalEntityConfigurationRule()]).checked_validations(
            model.semantic_manifest
        )


def test_natural_entity_used_in_wrong_context() -> None:
    """Test validation enforcing that a single semantic model cannot have more than one natural entity."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: random_natural_entity
          node_relation:
            schema_name: some_schema
            alias: random_natural_entity_table
          entities:
            - name: natural_key
              type: natural
          dimensions:
            - name: country
              type: categorical
        """
    )
    natural_entity_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), natural_entity_file]
    )

    with pytest.raises(
        SemanticManifestValidationException, match="use of `natural` entities is currently supported only in"
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([NaturalEntityConfigurationRule()]).checked_validations(
            model.semantic_manifest
        )
