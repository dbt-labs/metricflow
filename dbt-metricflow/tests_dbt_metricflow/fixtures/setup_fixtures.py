from __future__ import annotations

import logging

import _pytest.config
from metricflow_semantics.test_helpers.snapshot_helpers import (
    add_display_snapshots_cli_flag,
    add_overwrite_snapshots_cli_flag,
)

logger = logging.getLogger(__name__)


def pytest_addoption(parser: _pytest.config.argparsing.Parser) -> None:
    """Add options for running pytest through the CLI."""
    add_overwrite_snapshots_cli_flag(parser)
    add_display_snapshots_cli_flag(parser)
