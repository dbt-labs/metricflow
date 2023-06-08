from __future__ import annotations

import os
import pathlib
from datetime import datetime
from unittest.mock import MagicMock, patch

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import parse_directory_of_yaml_files_to_semantic_manifest
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationResults,
    ValidationError,
    ValidationFutureError,
    ValidationWarning,
)

from metricflow.cli.cli_context import CLIContext
from metricflow.cli.main import (
    dimension_values,
    dimensions,
    health_checks,
    metrics,
    query,
    tutorial,
    validate_configs,
)
from metricflow.cli.tutorial import (
    COUNTRIES_TABLE,
    CUSTOMERS_TABLE,
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


def test_validate_configs(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    # Mock build result for `model_build_result_from_config`
    mocked_parsing_result = MagicMock(issues=SemanticManifestValidationResults())
    # Mock validation errors in validate_model function
    issues = (
        ValidationWarning(context=None, message="warning_message"),  # type: ignore
        ValidationFutureError(context=None, message="future_error_message", error_date=datetime.now()),  # type: ignore
        ValidationError(context=None, message="error_message"),  # type: ignore
    )
    mocked_validate_model = SemanticManifestValidationResults.from_issues_sequence(issues)
    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch.object(SemanticManifestValidator, "validate_semantic_manifest", return_value=mocked_validate_model):
            resp = cli_runner.run(validate_configs)

    assert "error_message" in resp.output
    assert resp.exit_code == 0


def test_future_errors_and_warnings_conditionally_show_up(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    # Mock build result for `model_build_result_from_config`
    mocked_parsing_result = MagicMock(issues=SemanticManifestValidationResults())
    # Mock validation errors in validate_model function
    issues = (
        ValidationWarning(context=None, message="warning_message"),  # type: ignore
        ValidationFutureError(context=None, message="future_error_message", error_date=datetime.now()),  # type: ignore
        ValidationError(context=None, message="error_message"),  # type: ignore
    )
    mocked_validate_model = SemanticManifestValidationResults.from_issues_sequence(issues)
    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch.object(SemanticManifestValidator, "validate_semantic_manifest", return_value=mocked_validate_model):
            resp = cli_runner.run(validate_configs)

    assert "warning_message" not in resp.output
    assert "future_error_message" not in resp.output
    assert resp.exit_code == 0

    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch.object(SemanticManifestValidator, "validate_semantic_manifest", return_value=mocked_validate_model):
            resp = cli_runner.run(validate_configs, ["--show-all"])

    assert "warning_message" in resp.output
    assert "future_error_message" in resp.output
    assert resp.exit_code == 0


def test_validate_configs_data_warehouse_validations(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    # Mock build result for `model_build_result_from_config`
    mocked_parsing_result = MagicMock(issues=SemanticManifestValidationResults())
    dw_validation_issues = [
        ValidationError(context=None, message="Data Warehouse Error"),  # type: ignore
    ]
    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch.object(
            SemanticManifestValidator, "validate_semantic_manifest", return_value=SemanticManifestValidationResults()
        ):
            with patch.object(CLIContext, "sql_client", return_value=None):  # type: ignore
                with patch(
                    "metricflow.cli.main._run_dw_validations",
                    return_value=SemanticManifestValidationResults(errors=dw_validation_issues),
                ):
                    resp = cli_runner.run(validate_configs)

    assert "Data Warehouse Error" in resp.output
    assert resp.exit_code == 0


def test_validate_configs_skip_data_warehouse_validations(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    # Mock build result for `model_build_result_from_config`
    mocked_parsing_result = MagicMock(issues=SemanticManifestValidationResults())
    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch.object(
            SemanticManifestValidator,
            "validate_semantic_manifest",
            return_value=MagicMock(issues=SemanticManifestValidationResults()),
        ):
            resp = cli_runner.run(validate_configs, args=["--skip-dw"])

    assert "Data Warehouse Error" not in resp.output
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
    assert "Attempting to generate model configs to your local filesystem in" in resp.output

    table_names = cli_context.sql_client.list_tables(schema_name=cli_context.mf_system_schema)
    assert CUSTOMERS_TABLE in table_names
    assert TRANSACTIONS_TABLE in table_names
    assert COUNTRIES_TABLE in table_names


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

    SemanticManifestValidator[PydanticSemanticManifest]().checked_validations(model_build_result.semantic_manifest)
