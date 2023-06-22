from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql_clients.sql_utils import create_time_spine_table_if_necessary
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.source_schema_tools import create_tables_listed_in_table_snapshot_repository
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def source_table_snapshot_repository() -> SqlTableSnapshotRepository:  # noqa: D
    return SqlTableSnapshotRepository(Path(os.path.dirname(__file__)).joinpath("source_table_snapshots"))


@pytest.fixture(scope="session")
def create_source_tables(
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    time_spine_source: TimeSpineSource,
) -> None:
    """Creates all tables that should be in the source schema.

    If a table with a given name already exists in the source schema, it's assumed to have the expected schema / data.
    """
    # TODO: consolidate time spine operations with the rest of the table creation configuration
    create_time_spine_table_if_necessary(time_spine_source=time_spine_source, sql_client=sql_client)

    create_tables_listed_in_table_snapshot_repository(
        sql_client=sql_client,
        schema_name=mf_test_session_state.mf_source_schema,
        table_snapshot_repository=source_table_snapshot_repository,
    )
