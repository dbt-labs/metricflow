from __future__ import annotations

import pathlib
import traceback
from dataclasses import dataclass

from metricflow_semantics.test_helpers.snapshot_helpers import SnapshotConfiguration


@dataclass(frozen=True)
class MetricFlowTestConfiguration(SnapshotConfiguration):
    """State that is shared between tests during a testing session."""

    sql_engine_url: str
    sql_engine_password: str
    # Where MF system tables should be stored.
    mf_system_schema: str
    # Where tables for test data sets should be stored.
    mf_source_schema: str

    # Whether to display the graph associated with a test session in a browser window.
    display_graphs: bool

    # The source schema contains tables that are used for running tests. If this is set, a source schema in the SQL
    # is created and persisted between runs. The source schema name includes a hash of the tables that should be in
    # the schema, so
    use_persistent_source_schema: bool


class DirectoryPathAnchor:
    """Defines a directory inside the repo.

    Using this object allows you to avoid using hard-coded paths and instead use objects that will be handled properly
    during refactoring.
    """

    def __init__(self) -> None:
        """Initializer.

        The directory associated this anchor is where it's initialized.
        """
        stack = traceback.extract_stack()
        self._directory = pathlib.Path(stack[-2].filename).parent

    @property
    def directory(self) -> pathlib.Path:  # noqa: D102
        return self._directory
