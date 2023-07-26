from __future__ import annotations

import logging

from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotLoader,
    SqlTableSnapshotRepository,
)

logger = logging.getLogger(__name__)


def create_tables_listed_in_table_snapshot_repository(
    sql_client: SqlClient,
    schema_name: str,
    table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Creates all tables in the table snapshot repository in the given schema."""
    snapshot_loader = SqlTableSnapshotLoader(sql_client=sql_client, schema_name=schema_name)
    for table_snapshot in table_snapshot_repository.table_snapshots:
        logger.info(f"Loading: {table_snapshot.table_name}")
        snapshot_loader.load(table_snapshot)


POPULATE_SOURCE_SCHEMA_SHELL_COMMAND = (
    f"hatch -v run dev-env:pytest "
    f"-vv "
    f"--log-cli-level info "
    f"--use-persistent-source-schema "
    f"{__file__}::populate_source_schema"
)


def populate_source_schema(
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Populate the source schema with the tables listed in table_snapshots.

    This can be run via pytest when this file is specified because this function was whitelisted as a "test" in
    pyproject.toml. However, because the filename does not begin with "test_", it's not normally collected and run. As
    such, all parameters to this function are defined in fixtures.
    """
    if not mf_test_session_state.use_persistent_source_schema:
        raise ValueError("This should be run with the flag to enable use of the persistent source schema")

    schema_name = mf_test_session_state.mf_source_schema

    logger.info(f"Dropping schema {schema_name}")
    sql_client.drop_schema(schema_name=schema_name, cascade=True)
    logger.info(f"Creating schema {schema_name}")
    sql_client.create_schema(schema_name=schema_name)
    create_tables_listed_in_table_snapshot_repository(
        sql_client=sql_client,
        schema_name=schema_name,
        table_snapshot_repository=source_table_snapshot_repository,
    )
