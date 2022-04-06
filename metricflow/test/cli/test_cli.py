from datetime import datetime
from unittest.mock import patch, MagicMock

from metricflow.cli.main import (
    drop_materialization,
    get_dimension_values,
    list_dimensions,
    list_materializations,
    list_metrics,
    materialize,
    query,
    validate_configs,
)
from metricflow.model.model_validator import ModelValidator
from metricflow.model.validations.validator_helpers import (
    ValidationError,
    ValidationFatal,
    ValidationFutureError,
    ValidationWarning,
)
from metricflow.test.fixtures.cli_fixtures import MetricFlowCliRunner


def test_query(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(query, args=["--metrics", "metric1", "--dimensions", "ds"])

    assert "metric1" in resp.output
    assert resp.exit_code == 0


def test_list_dimensions(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(list_dimensions, args=["--metric-names", "metric1"])

    assert "dim1" in resp.output
    assert resp.exit_code == 0


def test_list_metrics(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(list_metrics)

    assert "metric1: dim1, dim2, dim3" in resp.output
    assert resp.exit_code == 0


def test_get_dimension_values(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(get_dimension_values, args=["--metric-name", "metric1", "--dimension-name", "ds"])

    assert "dim_val1" in resp.output
    assert resp.exit_code == 0


def test_list_materializations(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(list_materializations)

    assert "materialization: details related to materialization" in resp.output
    assert resp.exit_code == 0


def test_materialize(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(materialize, args=["--materialization-name", "test", "--start-time", "2020-01-02"])

    assert "test.table" in resp.output
    assert resp.exit_code == 0


def test_drop_materialization(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(drop_materialization, args=["--materialization-name", "test"])

    assert resp.exit_code == 0


def test_validate_configs(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    # Mock validate_model function
    mocked_build_result = MagicMock(
        issues=(
            ValidationWarning(None, "warning"),  # type: ignore
            ValidationFutureError(None, "future_error", datetime.now()),  # type: ignore
            ValidationError(None, "error"),  # type: ignore
            ValidationFatal(None, "fatal"),  # type: ignore
        )
    )
    with patch.object(ModelValidator, "validate_model", return_value=mocked_build_result):
        resp = cli_runner.run(validate_configs)

    assert "future_error" in resp.output
    assert resp.exit_code == 0
