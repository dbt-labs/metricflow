from __future__ import annotations

import textwrap
from typing import List

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_validation_ready_semantic_manifest
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)

from tests_metricflow_semantics.query.parser_helpers import QueryParserTester

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


def create_manifest_from_yaml_files(yaml_files: List[YamlConfigFile]) -> SemanticManifest:  # noqa: D103
    semantic_manifest = parse_yaml_files_to_validation_ready_semantic_manifest(
        yaml_files, apply_transformations=True
    ).semantic_manifest
    SemanticManifestValidator().checked_validations(semantic_manifest)
    return semantic_manifest


def query_parser_from_yaml(yaml_contents: List[YamlConfigFile]) -> MetricFlowQueryParser:
    """Given YAML files, return a query parser using default source nodes, resolvers and time spine source."""
    semantic_manifest_lookup = SemanticManifestLookup(create_manifest_from_yaml_files(yaml_contents))
    return MetricFlowQueryParser(
        semantic_manifest_lookup=semantic_manifest_lookup,
    )


@pytest.fixture(scope="module")
def scd_query_parser(scd_semantic_manifest: SemanticManifest) -> MetricFlowQueryParser:  # noqa: D103
    return MetricFlowQueryParser(SemanticManifestLookup(scd_semantic_manifest))


@pytest.fixture
def parser_tester_for_scd_manifest(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, scd_semantic_manifest: SemanticManifest
) -> QueryParserTester:
    return QueryParserTester(request, mf_test_configuration, scd_semantic_manifest)


@pytest.fixture
def parser_tester_for_bookings_manifest(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration
) -> QueryParserTester:
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    return QueryParserTester(
        request,
        mf_test_configuration,
        create_manifest_from_yaml_files([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file]),
    )
