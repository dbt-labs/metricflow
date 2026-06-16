# These imports are required to properly set up pytest fixtures.
from __future__ import annotations

import _pytest.config
import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.snapshot_helpers import (
    DISPLAY_SNAPSHOTS_CLI_FLAG,
    OVERWRITE_SNAPSHOTS_CLI_FLAG,
    SnapshotConfiguration,
    add_display_snapshots_cli_flag,
    add_overwrite_snapshots_cli_flag,
)

from tests_metricflow_semantic_interfaces import TESTS_METRICFLOW_SEMANTIC_INTERFACES_ANCHOR
from tests_metricflow_semantic_interfaces.fixtures.semantic_manifest_fixtures import *  # noqa: F401, F403


def pytest_addoption(parser: _pytest.config.argparsing.Parser) -> None:
    """Add options for running pytest through the CLI."""
    add_overwrite_snapshots_cli_flag(parser)
    add_display_snapshots_cli_flag(parser)


@pytest.fixture(scope="session")
def snapshot_configuration(request: FixtureRequest) -> SnapshotConfiguration:
    """Snapshot configuration for tests in this package."""
    return SnapshotConfiguration(
        display_snapshots=bool(request.config.getoption(DISPLAY_SNAPSHOTS_CLI_FLAG, default=False)),
        overwrite_snapshots=bool(request.config.getoption(OVERWRITE_SNAPSHOTS_CLI_FLAG, default=False)),
        snapshot_directory=TESTS_METRICFLOW_SEMANTIC_INTERFACES_ANCHOR / "snapshots",
        tests_directory=TESTS_METRICFLOW_SEMANTIC_INTERFACES_ANCHOR,
    )
