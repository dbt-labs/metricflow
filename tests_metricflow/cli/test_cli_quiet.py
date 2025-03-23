from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow.cli.cli_test_helpers import run_and_check_cli_command
from tests_metricflow.cli.isolated_cli_command_interface import IsolatedCliCommandEnum
from tests_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner

logger = logging.getLogger(__name__)


@pytest.mark.slow
def test_query(
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Test that the `--quiet` flag only shows the table when running a query."""
    run_and_check_cli_command(
        request=request,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=["--metrics", "transactions", "--group-by", "metric_time", "--order", "metric_time", "--quiet"],
    )


@pytest.mark.slow
def test_explain(
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Test that the `--quiet` flag only shows the SQL when explaining a query."""
    run_and_check_cli_command(
        request=request,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--metrics",
            "transactions",
            "--group-by",
            "metric_time",
            "--order",
            "metric_time",
            "--explain",
            "--quiet",
        ],
    )
