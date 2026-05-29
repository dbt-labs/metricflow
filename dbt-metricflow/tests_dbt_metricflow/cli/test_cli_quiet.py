from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_dbt_metricflow.cli.cli_test_helpers import run_and_check_cli_command
from tests_dbt_metricflow.cli.isolated_cli_command_interface import IsolatedCliCommandEnum
from tests_dbt_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner

logger = logging.getLogger(__name__)


def test_query(
    cli_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Test that the `--quiet` flag only shows the table when running a query."""
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=["--metrics", "transactions", "--group-by", "metric_time", "--order", "metric_time", "--quiet"],
    )


def test_explain(
    cli_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Test that the `--quiet` flag only shows the SQL when explaining a query."""
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
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
