from __future__ import annotations

import logging
import textwrap

import pytest

from metricflow_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.validations.hierarchies import SemanticModelHierarchiesRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator
from metricflow_semantic_interfaces.validations.validator_helpers import SemanticManifestValidationException
from tests_metricflow_semantic_interfaces.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)

logger = logging.getLogger(__name__)

_EMPLOYEES_YAML = textwrap.dedent(
    """\
    semantic_model:
      name: employees
      node_relation:
        schema_name: some_schema
        alias: employees
      entities:
        - name: employee
          type: primary
        - name: store
          type: foreign
        - name: branch
          type: foreign
      dimensions:
        - name: department_code
          type: categorical
        - name: parent_department_code
          type: categorical
        - name: employee_code
          type: categorical
        - name: manager_code
          type: categorical
        - name: hired_at
          type: time
          type_params:
            time_granularity: day
    """
)

_STORES_AND_REGIONS_YAML = textwrap.dedent(
    """\
    semantic_model:
      name: stores
      node_relation:
        schema_name: some_schema
        alias: stores
      entities:
        - name: store
          type: primary
        - name: branch
          type: unique
        - name: region
          type: foreign
      dimensions:
        - name: store_name
          type: categorical
        - name: city
          type: categorical
        - name: opening_period
          type: categorical
    ---
    semantic_model:
      name: store_history
      node_relation:
        schema_name: some_schema
        alias: store_history
      entities:
        - name: store
          type: natural
      dimensions:
        - name: opening_period
          type: time
          type_params:
            time_granularity: day
        - name: previous_city
          type: categorical
    ---
    semantic_model:
      name: regions
      node_relation:
        schema_name: some_schema
        alias: regions
      entities:
        - name: region
          type: primary
      dimensions:
        - name: country
          type: categorical
        - name: region_name
          type: categorical
    """
)


def _semantic_manifest(hierarchies_yaml: str) -> PydanticSemanticManifest:
    employees_file = YamlConfigFile(
        filepath="employees.yaml",
        contents=_EMPLOYEES_YAML + textwrap.indent(textwrap.dedent(hierarchies_yaml), "  "),
    )
    stores_and_regions_file = YamlConfigFile(filepath="stores_and_regions.yaml", contents=_STORES_AND_REGIONS_YAML)
    return parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, employees_file, stores_and_regions_file]
    ).semantic_manifest


def _validate(semantic_manifest: PydanticSemanticManifest) -> None:
    SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelHierarchiesRule()]).checked_validations(
        semantic_manifest
    )


def test_hierarchies_valid() -> None:  # noqa: D103
    semantic_manifest = _semantic_manifest(
        """\
        hierarchies:
          - name: geography
            levels:
              - store__region__country
              - store__region__region_name
              - store__city
              - store__store_name
          - name: organization
            levels:
              - dimension: department_code
                parent: parent_department_code
              - employee__employee_code
          - name: reporting_line
            levels:
              - dimension: employee_code
                parent: employee__manager_code
        """
    )

    _validate(semantic_manifest)


@pytest.mark.parametrize(
    ("hierarchies_yaml", "error_pattern"),
    [
        (
            """\
            hierarchies:
              - name: geography
                levels: [store__city, store__district]
            """,
            "Level `store__district` in hierarchy `geography` does not resolve to a dimension reachable from "
            "semantic model `employees`",
        ),
        (
            """\
            hierarchies:
              - name: geography
                levels: [region__country]
            """,
            "Level `region__country` in hierarchy `geography` does not resolve",
        ),
        (
            """\
            hierarchies:
              - name: organization
                levels: [store__department_code]
            """,
            "Level `store__department_code` in hierarchy `organization` does not resolve",
        ),
        (
            """\
            hierarchies:
              - name: geography
                levels: [city]
            """,
            "Level `city` in hierarchy `geography` does not resolve",
        ),
        (
            """\
            hierarchies:
              - name: tenure
                levels: [department_code, hired_at]
            """,
            "Level `hired_at` in hierarchy `tenure` is a time dimension",
        ),
        (
            """\
            hierarchies:
              - name: store_lifecycle
                levels: [store__city, store__opening_period]
            """,
            "Level `store__opening_period` in hierarchy `store_lifecycle` is a time dimension",
        ),
        (
            """\
            hierarchies:
              - name: geography
                levels: [store__city]
              - name: geography
                levels: [store__store_name]
            """,
            "more than one hierarchy named `geography`",
        ),
        (
            """\
            hierarchies:
              - name: organization
                levels: [department_code, employee__department_code]
            """,
            "lists the level `employee__department_code` more than once",
        ),
        (
            """\
            hierarchies:
              - name: reporting_line
                levels:
                  - dimension: employee_code
                    parent: supervisor_code
            """,
            "Parent `supervisor_code` of level `employee_code` in hierarchy `reporting_line` does not resolve",
        ),
        (
            """\
            hierarchies:
              - name: reporting_line
                levels:
                  - dimension: employee_code
                    parent: store__store_name
            """,
            "Parent `store__store_name` of level `employee_code` in hierarchy `reporting_line` does not come from the "
            "same rows",
        ),
        (
            """\
            hierarchies:
              - name: relocation
                levels:
                  - dimension: store__city
                    parent: store__previous_city
            """,
            "Parent `store__previous_city` of level `store__city` in hierarchy `relocation` does not come from the "
            "same rows",
        ),
        (
            """\
            hierarchies:
              - name: store_tree
                levels:
                  - dimension: store__store_name
                    parent: branch__city
            """,
            "Parent `branch__city` of level `store__store_name` in hierarchy `store_tree` does not come from the "
            "same rows",
        ),
        (
            """\
            hierarchies:
              - name: reporting_line
                levels:
                  - dimension: employee_code
                    parent: employee__employee_code
            """,
            "Level `employee_code` in hierarchy `reporting_line` names itself as its parent",
        ),
    ],
)
def test_hierarchies_invalid(hierarchies_yaml: str, error_pattern: str) -> None:  # noqa: D103
    semantic_manifest = _semantic_manifest(hierarchies_yaml)

    with pytest.raises(SemanticManifestValidationException, match=error_pattern):
        _validate(semantic_manifest)


def test_hierarchy_without_levels_invalid() -> None:
    """Test the empty-levels check for manifests built without the YAML schema, which already rejects it."""
    semantic_manifest = _semantic_manifest(
        """\
        hierarchies:
          - name: geography
            levels: [store__city]
        """
    )
    employees = next(model for model in semantic_manifest.semantic_models if model.name == "employees")
    employees.hierarchies[0].levels = []

    with pytest.raises(
        SemanticManifestValidationException,
        match="Hierarchy `geography` in semantic model `employees` has no levels",
    ):
        _validate(semantic_manifest)


def test_hierarchy_with_invalid_name() -> None:
    """Test the name check for manifests built without the YAML schema, which already rejects it."""
    semantic_manifest = _semantic_manifest(
        """\
        hierarchies:
          - name: geography
            levels: [store__city]
        """
    )
    employees = next(model for model in semantic_manifest.semantic_models if model.name == "employees")
    employees.hierarchies[0].name = "Geography Tree"

    with pytest.raises(SemanticManifestValidationException, match="Invalid name `Geography Tree`"):
        _validate(semantic_manifest)


def _shorthand_semantic_manifest(
    countries_hierarchies: str = "", offices_hierarchies: str = ""
) -> PydanticSemanticManifest:
    yaml_contents = (
        textwrap.dedent(
            """\
            semantic_model:
              name: countries
              node_relation:
                schema_name: some_schema
                alias: countries
              primary_entity: country
              dimensions:
                - name: continent
                  type: categorical
                - name: country_name
                  type: categorical
            """
        )
        + textwrap.indent(textwrap.dedent(countries_hierarchies), "  ")
        + textwrap.dedent(
            """\
            ---
            semantic_model:
              name: offices
              node_relation:
                schema_name: some_schema
                alias: offices
              entities:
                - name: office
                  type: primary
                - name: country
                  type: foreign
              dimensions:
                - name: office_name
                  type: categorical
            """
        )
        + textwrap.indent(textwrap.dedent(offices_hierarchies), "  ")
        + textwrap.dedent(
            """\
            ---
            semantic_model:
              name: country_facts
              node_relation:
                schema_name: some_schema
                alias: country_facts
              entities:
                - name: country
                  type: primary
              dimensions:
                - name: currency
                  type: categorical
            """
        )
    )
    return parse_yaml_files_to_validation_ready_semantic_manifest(
        [
            EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
            YamlConfigFile(filepath="shorthand.yaml", contents=yaml_contents),
        ]
    ).semantic_manifest


def test_hierarchy_with_primary_entity_shorthand_valid() -> None:  # noqa: D103
    semantic_manifest = _shorthand_semantic_manifest(
        countries_hierarchies="""\
        hierarchies:
          - name: world
            levels: [country__continent, country__country_name]
        """
    )

    _validate(semantic_manifest)


def test_hierarchy_cannot_join_through_primary_entity_shorthand() -> None:
    """Test that a `primary_entity` shorthand is not a join key, as in MetricFlow's group-by resolution."""
    semantic_manifest = _shorthand_semantic_manifest(
        offices_hierarchies="""\
        hierarchies:
          - name: office_geography
            levels: [country__continent, office_name]
        """
    )

    with pytest.raises(SemanticManifestValidationException, match="Level `country__continent` .* does not resolve"):
        _validate(semantic_manifest)


def test_hierarchy_cannot_join_out_of_primary_entity_shorthand() -> None:
    """Test that a model with only a `primary_entity` shorthand cannot reach another model's dimensions."""
    semantic_manifest = _shorthand_semantic_manifest(
        countries_hierarchies="""\
        hierarchies:
          - name: world
            levels: [country__continent, country__currency]
        """
    )

    with pytest.raises(SemanticManifestValidationException, match="Level `country__currency` .* does not resolve"):
        _validate(semantic_manifest)


def test_hierarchy_with_local_dimension_through_another_entity_is_duplicate() -> None:
    """Test that `branch__city` on a model with a `branch` unique entity is the same level as `city`."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: stores
          node_relation:
            schema_name: some_schema
            alias: stores
          entities:
            - name: store
              type: primary
            - name: branch
              type: unique
          dimensions:
            - name: city
              type: categorical
          hierarchies:
            - name: geography
              levels: [city, branch__city]
        """
    )
    semantic_manifest = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, YamlConfigFile(filepath="stores.yaml", contents=yaml_contents)]
    ).semantic_manifest

    with pytest.raises(SemanticManifestValidationException, match="lists the level `branch__city` more than once"):
        _validate(semantic_manifest)
