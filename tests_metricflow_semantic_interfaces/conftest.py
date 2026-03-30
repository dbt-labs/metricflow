# These imports are required to properly set up pytest fixtures.
from __future__ import annotations

import _pytest.config
from metricflow_semantics.test_helpers.snapshot_helpers import (
    add_display_snapshots_cli_flag,
    add_overwrite_snapshots_cli_flag,
)

from tests_metricflow_semantic_interfaces.fixtures.semantic_manifest_fixtures import *  # noqa: F401, F403


def pytest_addoption(parser: _pytest.config.argparsing.Parser) -> None:
    """Add options for running pytest through the CLI."""
    add_overwrite_snapshots_cli_flag(parser)
    add_display_snapshots_cli_flag(parser)
