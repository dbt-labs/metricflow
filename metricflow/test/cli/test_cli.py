from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationResults,
    ValidationError,
    ValidationFutureError,
    ValidationWarning,
)

from metricflow.cli.cli_context import CLIContext
from metricflow.cli.main import (
    get_dimension_values,
    health_checks,
    list_dimensions,
    list_metrics,
    query,
    validate_configs,
    version,
)
from metricflow.test.fixtures.cli_fixtures import MetricFlowCliRunner


def test_query(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(query, args=["--metrics", "bookings", "--dimensions", "ds"])
    assert "bookings" in resp.output
    assert resp.exit_code == 0


def test_list_dimensions(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(list_dimensions, args=["--metric-names", "bookings"])

    assert "ds" in resp.output
    assert resp.exit_code == 0


def test_list_metrics(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(list_metrics)

    assert "bookings_per_listing: ds" in resp.output
    assert resp.exit_code == 0


def test_get_dimension_values(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(get_dimension_values, args=["--metric-name", "bookings", "--dimension-name", "is_instant"])

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


def test_version(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(version)

    assert resp.output
    assert resp.exit_code == 0


def test_health_checks(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(health_checks)

    assert "SELECT 1: Success!" in resp.output
    assert resp.exit_code == 0
