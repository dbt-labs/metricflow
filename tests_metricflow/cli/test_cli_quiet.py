from __future__ import annotations

import logging

import pytest
from _pytest.capture import CaptureFixture
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from dbt_metricflow.cli.main import query
from tests_metricflow.cli.cli_test_helpers import run_and_check_cli_command
from tests_metricflow.fixtures.cli_fixtures import MetricFlowCliRunner

logger = logging.getLogger(__name__)


@pytest.mark.duckdb_only
def test_query(
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    capsys: CaptureFixture,
    cli_runner: MetricFlowCliRunner,
) -> None:
    """Test that the `--quiet` flag only shows the table when running a query."""
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=["--metrics", "bookings", "--group-by", "metric_time", "--order", "metric_time", "--quiet"],
    )


@pytest.mark.duckdb_only
def test_explain(
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    capsys: CaptureFixture,
    cli_runner: MetricFlowCliRunner,
) -> None:
    """Test that the `--quiet` flag only shows the SQL when explaining a query."""
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=["--metrics", "bookings", "--group-by", "metric_time", "--order", "metric_time", "--explain", "--quiet"],
    )
