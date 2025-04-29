from __future__ import annotations

import textwrap
from typing import List

import pytest
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_validation_ready_semantic_manifest
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)

BOOKINGS_YAML = textwrap.dedent(
    """\
    semantic_model:
      name: bookings_source

      node_relation:
        schema_name: some_schema
        alias: bookings_source_table

      defaults:
        agg_time_dimension: ds

      measures:
        - name: bookings
          expr: "1"
          agg: sum
          create_metric: true

      dimensions:
        - name: is_instant
          type: categorical
        - name: ds
          type: time
          type_params:
            time_granularity: day

      primary_entity: booking

      entities:
        - name: listing
          type: foreign
          expr: listing_id
    """
)


@pytest.fixture(scope="session")
def bookings_query_parser() -> MetricFlowQueryParser:  # noqa
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    return query_parser_from_yaml([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file])


def query_parser_from_yaml(yaml_contents: List[YamlConfigFile]) -> MetricFlowQueryParser:
    """Given yaml files, return a query parser using default source nodes, resolvers and time spine source."""
    semantic_manifest = parse_yaml_files_to_validation_ready_semantic_manifest(
        yaml_contents, apply_transformations=True
    ).semantic_manifest
    semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
    SemanticManifestValidator[SemanticManifest]().checked_validations(semantic_manifest)
    return MetricFlowQueryParser(
        semantic_manifest_lookup=semantic_manifest_lookup,
    )
