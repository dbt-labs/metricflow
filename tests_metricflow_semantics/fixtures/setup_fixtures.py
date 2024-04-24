from __future__ import annotations

import logging

import _pytest.config
import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    DISPLAY_SNAPSHOTS_CLI_FLAG,
    OVERWRITE_SNAPSHOTS_CLI_FLAG,
    add_display_snapshots_cli_flag,
    add_overwrite_snapshots_cli_flag,
)

logger = logging.getLogger(__name__)


def pytest_addoption(parser: _pytest.config.argparsing.Parser) -> None:
    """Add options for running pytest through the CLI."""
    add_overwrite_snapshots_cli_flag(parser)
    add_display_snapshots_cli_flag(parser)


@pytest.fixture(scope="session")
def mf_test_configuration(  # noqa: D103
    request: FixtureRequest,
) -> MetricFlowTestConfiguration:
    return MetricFlowTestConfiguration(
        sql_engine_url="N/A",
        sql_engine_password="N/A",
        mf_system_schema="N/A",
        mf_source_schema="N/A",
        display_snapshots=bool(request.config.getoption(DISPLAY_SNAPSHOTS_CLI_FLAG, default=False)),
        display_graphs=False,
        overwrite_snapshots=bool(request.config.getoption(OVERWRITE_SNAPSHOTS_CLI_FLAG, default=False)),
        use_persistent_source_schema=False,
    )
