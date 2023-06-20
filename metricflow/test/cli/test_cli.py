from __future__ import annotations

import os
import pathlib
import shutil
import textwrap
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_directory_of_yaml_files_to_semantic_manifest,
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.test_utils import base_semantic_manifest_file
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

from metricflow.cli.main import (
    dimension_values,
    dimensions,
    entities,
    health_checks,
    metrics,
    query,
    tutorial,
    validate_configs,
)
from metricflow.cli.tutorial import (
    COUNTRIES_TABLE,
    CUSTOMERS_TABLE,
    TIME_SPINE_TABLE,
    TRANSACTIONS_TABLE,
)
from metricflow.test.fixtures.cli_fixtures import MetricFlowCliRunner


def test_query(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(query, args=["--metrics", "bookings", "--group-bys", "ds"])
    assert "bookings" in resp.output
    assert resp.exit_code == 0


def test_list_dimensions(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(dimensions, args=["--metrics", "bookings"])

    assert "ds" in resp.output
    assert resp.exit_code == 0


def test_list_metrics(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(metrics)

    assert "bookings_per_listing: ds" in resp.output
    assert resp.exit_code == 0


def test_get_dimension_values(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(dimension_values, args=["--metric", "bookings", "--dimension", "is_instant"])

    actual_output_lines = sorted(resp.output.split("\n"))
    assert ["", "• False", "• True"] == actual_output_lines
    assert resp.exit_code == 0


@contextmanager
def create_directory(directory_path: str) -> Iterator[None]:
    """Creates a temporary directory (errors if it exists) and removes it."""
    path = Path(directory_path)
    path.mkdir(parents=True)
    yield
    shutil.rmtree(path)


def test_validate_configs(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: bad_semantic_model
          node_relation:
            schema_name: some_schema
            alias: some_table
          defaults:
            agg_time_dimension: ds
          dimensions:
            - name: country
              type: categorical
        """
    )
    bad_semantic_model = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    manifest = parse_yaml_files_to_validation_ready_semantic_manifest(
        [base_semantic_manifest_file(), bad_semantic_model]
    ).semantic_manifest

    target_directory = Path().absolute() / "target"
    with create_directory(target_directory.as_posix()):
        manifest_file = target_directory / "semantic_manifest.json"
        manifest_file.write_text(manifest.json())

        resp = cli_runner.run(validate_configs)

        assert "ERROR" in resp.output
        assert resp.exit_code == 0


def test_health_checks(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(health_checks)

    assert "SELECT 1: Success!" in resp.output
    assert resp.exit_code == 0


def test_tutorial(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    cli_context = cli_runner.cli_context

    resp = cli_runner.run(tutorial, args=["-m"])
    assert "Please run the following steps" in resp.output

    pathlib.Path(cli_context.config.file_path).touch()
    resp = cli_runner.run(tutorial, args=["--skip-dw"])
    assert (
        "Attempting to generate model configs to your local filesystem in" in resp.output
    ), f"Unexpected output: {resp.output}"

    table_names = cli_context.sql_client.list_tables(schema_name=cli_context.mf_system_schema)
    assert CUSTOMERS_TABLE in table_names
    assert TRANSACTIONS_TABLE in table_names
    assert COUNTRIES_TABLE in table_names
    assert TIME_SPINE_TABLE in table_names


def test_build_tutorial_model(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    TOP_LEVEL_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    cli_sample_template_mapping = {
        "customers_table": CUSTOMERS_TABLE,
        "transactions_table": TRANSACTIONS_TABLE,
        "countries_table": COUNTRIES_TABLE,
        "system_schema": cli_runner.cli_context.mf_system_schema,
    }
    model_build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(TOP_LEVEL_PATH, "cli/sample_models"), template_mapping=cli_sample_template_mapping
    )
    assert model_build_result.issues.has_blocking_issues is False

    SemanticManifestValidator[SemanticManifest]().checked_validations(model_build_result.semantic_manifest)


def test_list_entities(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(entities, args=["--metrics", "bookings"])

    assert "guest" in resp.output
    assert "host" in resp.output
