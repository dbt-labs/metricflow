from __future__ import annotations

import logging

from metricflow.protocols.sql_client import SqlEngine
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotLoader,
    SqlTableSnapshotRepository,
)

logger = logging.getLogger(__name__)


def create_tables_listed_in_table_snapshot_repository(
    ddl_sql_client: SqlClientWithDDLMethods,
    schema_name: str,
    table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Creates all tables in the table snapshot repository in the given schema."""
    snapshot_loader = SqlTableSnapshotLoader(ddl_sql_client=ddl_sql_client, schema_name=schema_name)
    for table_snapshot in table_snapshot_repository.table_snapshots:
        logger.info(f"Loading: {table_snapshot.table_name}")
        snapshot_loader.load(table_snapshot)


def get_populate_source_schema_shell_command(engine: SqlEngine) -> str:
    """Creates the environment-specific shell command for populating a source schema.

    This needs to be built in this way because different engines use different environments.
    """
    # The hatch environments are named in all lower-case, but otherwise should match the values.
    return (
        f"hatch -v run {engine.value.lower()}-env:pytest "
        f"-vv "
        f"--log-cli-level info "
        f"--use-persistent-source-schema "
        f"{__file__}::populate_source_schema"
    )


def populate_source_schema(
    mf_test_session_state: MetricFlowTestSessionState,
    ddl_sql_client: SqlClientWithDDLMethods,
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
    ddl_sql_client.drop_schema(schema_name=schema_name, cascade=True)
    logger.info(f"Creating schema {schema_name}")
    ddl_sql_client.create_schema(schema_name=schema_name)
    create_tables_listed_in_table_snapshot_repository(
        ddl_sql_client=ddl_sql_client,
        schema_name=schema_name,
        table_snapshot_repository=source_table_snapshot_repository,
    )
