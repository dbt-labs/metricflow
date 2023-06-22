from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.source_schema_tools import create_tables_listed_in_table_snapshot_repository
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def source_table_snapshot_repository() -> SqlTableSnapshotRepository:  # noqa: D
    return SqlTableSnapshotRepository(Path(os.path.dirname(__file__)).joinpath("source_table_snapshots"))


@pytest.fixture(scope="session", autouse=True)
def check_time_spine_source(
    mf_test_session_state: MetricFlowTestSessionState,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    time_spine_source: TimeSpineSource,
) -> None:
    """Check that the time spine source follows the definition in the table snapshot.

    The time spine table is defined in a table snapshot YAML file and is restored to the source schema based on that
    definition. The definition in the YAML should align with the definition in the time_spine_source fixture.
    """
    assert (
        time_spine_source.schema_name == mf_test_session_state.mf_source_schema
    ), "The time spine source table should be in the source schema"

    time_spine_snapshot_candidates = tuple(
        snapshot
        for snapshot in source_table_snapshot_repository.table_snapshots
        if snapshot.table_name == time_spine_source.table_name
    )

    assert len(time_spine_snapshot_candidates) == 1, (
        f"Did not get exactly one table snapshot matching the time_spine_source table name. "
        f"Got: {time_spine_snapshot_candidates}"
    )

    time_spine_snapshot = time_spine_snapshot_candidates[0]

    assert len(time_spine_snapshot.column_definitions) == 1
    time_column = time_spine_snapshot.column_definitions[0]
    assert time_column.name == time_spine_source.time_column_name


@pytest.fixture(scope="session")
def create_source_tables(
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Creates all tables that should be in the source schema.

    If a table with a given name already exists in the source schema, it's assumed to have the expected schema / data.
    """
    create_tables_listed_in_table_snapshot_repository(
        sql_client=sql_client,
        schema_name=mf_test_session_state.mf_source_schema,
        table_snapshot_repository=source_table_snapshot_repository,
    )
