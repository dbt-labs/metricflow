import copy
import re
import textwrap
from typing import Callable

import pytest

from dbt_semantic_interfaces.model_validator import ModelValidator
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.elements.entity import EntityType
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_validation_ready_model
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.validations.entities import (
    NaturalEntityConfigurationRule,
    OnePrimaryEntityPerSemanticModelRule,
)
from dbt_semantic_interfaces.validations.validator_helpers import ModelValidationException
from dbt_semantic_interfaces.test_utils import base_semantic_manifest_file
from dbt_semantic_interfaces.test_utils import find_semantic_model_with


def test_semantic_model_cant_have_more_than_one_primary_entity(
    simple_model__with_primary_transforms: SemanticManifest,
) -> None:  # noqa: D
    """Add an additional primary entity to a semantic model and assert that it cannot have two"""
    model = copy.deepcopy(simple_model__with_primary_transforms)
    func: Callable[[SemanticModel], bool] = lambda semantic_model: len(semantic_model.entities) > 1

    multiple_entity_semantic_model, _ = find_semantic_model_with(model, func)

    entity_references = set()
    for entity in multiple_entity_semantic_model.entities:
        entity.type = EntityType.PRIMARY
        entity_references.add(entity.reference)

    model_issues = ModelValidator([OnePrimaryEntityPerSemanticModelRule()]).validate_model(model)

    future_issue = (
        f"Semantic models can have only one primary entity. The semantic model"
        f" `{multiple_entity_semantic_model.name}` has {len(entity_references)}"
    )

    found_future_issue = False

    if model_issues is not None:
        for issue in model_issues.all_issues:
            if re.search(future_issue, issue.message):
                found_future_issue = True

    assert found_future_issue


def test_multiple_natural_entities() -> None:
    """Test validation enforcing that a single semantic model cannot have more than one natural entity"""
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
    model = parse_yaml_files_to_validation_ready_model([base_semantic_manifest_file(), natural_entity_file])

    with pytest.raises(ModelValidationException, match="can have at most one natural entity"):
        ModelValidator([NaturalEntityConfigurationRule()]).checked_validations(model.model)


def test_natural_entity_used_in_wrong_context() -> None:
    """Test validation enforcing that a single semantic model cannot have more than one natural entity"""
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
    model = parse_yaml_files_to_validation_ready_model([base_semantic_manifest_file(), natural_entity_file])

    with pytest.raises(ModelValidationException, match="use of `natural` entities is currently supported only in"):
        ModelValidator([NaturalEntityConfigurationRule()]).checked_validations(model.model)
