from __future__ import annotations

import logging

import pytest
from _pytest.capture import CaptureFixture
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from dbt_metricflow.cli.main import query
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.cli.conftest import run_and_check_cli_command
from tests_metricflow.fixtures.cli_fixtures import MetricFlowCliRunner

logger = logging.getLogger(__name__)


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_query(
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    capsys: CaptureFixture,
    cli_runner: MetricFlowCliRunner,
    sql_client: SqlClient,
) -> None:
    """Test that the `--quiet` flag only shows the table when running a query."""
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=["--metrics", "bookings", "--group-by", "metric_time", "--order", "metric_time", "--quiet"],
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_explain(
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    capsys: CaptureFixture,
    cli_runner: MetricFlowCliRunner,
    sql_client: SqlClient,
) -> None:
    """Test that the `--quiet` flag only shows the SQL when explaining a query."""
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=["--metrics", "bookings", "--group-by", "metric_time", "--order", "metric_time", "--explain", "--quiet"],
        sql_client=sql_client,
    )
