import copy
import re
import textwrap
from typing import Callable

import pytest

from dbt_semantic_interfaces.model_validator import ModelValidator
from dbt_semantic_interfaces.objects.data_source import DataSource
from dbt_semantic_interfaces.objects.elements.entity import EntityType
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_validation_ready_model
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.validations.entities import (
    NaturalEntityConfigurationRule,
    OnePrimaryEntityPerDataSourceRule,
)
from dbt_semantic_interfaces.validations.validator_helpers import ModelValidationException
from metricflow.test.model.validations.helpers import base_model_file
from metricflow.test.test_utils import find_data_source_with


def test_data_source_cant_have_more_than_one_primary_identifier(
    simple_model__with_primary_transforms: UserConfiguredModel,
) -> None:  # noqa: D
    """Add an additional primary identifier to a data source and assert that it cannot have two"""
    model = copy.deepcopy(simple_model__with_primary_transforms)
    func: Callable[[DataSource], bool] = lambda data_source: len(data_source.identifiers) > 1

    multiple_identifier_data_source, _ = find_data_source_with(model, func)

    entity_references = set()
    for identifier in multiple_identifier_data_source.identifiers:
        identifier.type = EntityType.PRIMARY
        entity_references.add(identifier.reference)

    model_issues = ModelValidator([OnePrimaryEntityPerDataSourceRule()]).validate_model(model)

    future_issue = (
        f"Data sources can have only one primary entity. The data source"
        f" `{multiple_identifier_data_source.name}` has {len(entity_references)}"
    )

    found_future_issue = False

    if model_issues is not None:
        for issue in model_issues.all_issues:
            if re.search(future_issue, issue.message):
                found_future_issue = True

    assert found_future_issue


def test_multiple_natural_identifiers() -> None:
    """Test validation enforcing that a single data source cannot have more than one natural entity"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: too_many_natural_identifiers
          sql_table: some_schema.natural_identifier_table
          identifiers:
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
    natural_identifier_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), natural_identifier_file])

    with pytest.raises(ModelValidationException, match="can have at most one natural entity"):
        ModelValidator([NaturalEntityConfigurationRule()]).checked_validations(model.model)


def test_natural_identifier_used_in_wrong_context() -> None:
    """Test validation enforcing that a single data source cannot have more than one natural entity"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: random_natural_identifier
          sql_table: some_schema.random_natural_identifier_table
          identifiers:
            - name: natural_key
              type: natural
          dimensions:
            - name: country
              type: categorical
        """
    )
    natural_identifier_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([base_model_file(), natural_identifier_file])

    with pytest.raises(ModelValidationException, match="use of `natural` entities is currently supported only in"):
        ModelValidator([NaturalEntityConfigurationRule()]).checked_validations(model.model)
