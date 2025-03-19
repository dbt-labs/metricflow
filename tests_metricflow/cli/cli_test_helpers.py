from __future__ import annotations

from typing import Optional, Sequence

import click
from _pytest.capture import CaptureFixture
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.cli_fixtures import MetricFlowCliRunner
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal


def run_and_check_cli_command(
    request: FixtureRequest,
    capsys: CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
    command: click.BaseCommand,
    args: Sequence[str],
    sql_client: Optional[SqlClient] = None,
    expected_exit_code: int = 0,
    expectation_description: Optional[str] = None,
) -> None:
    """Helper to run a CLI command and check that the output matches the stored snapshot."""
    # Needed to resolve `ValueError: I/O operation on closed file` when running CLI tests individually.
    # See: https://github.com/pallets/click/issues/824
    with capsys.disabled():
        result = cli_runner.run(command, args=args)
    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="result",
        snapshot_str=result.stdout,
        sql_engine=sql_client.sql_engine_type if sql_client else None,
        expectation_description=expectation_description,
    )
    assert result.exit_code == expected_exit_code
