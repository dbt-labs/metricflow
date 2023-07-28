from __future__ import annotations

import logging
import warnings

import pytest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.table_fixtures import CONFIGURED_SOURCE_TABLE_SNAPSHOT_REPOSITORY
from metricflow.test.source_schema_tools import get_populate_source_schema_shell_command
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
    TableSnapshotException,
)

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames="table_name",
    argvalues=tuple(
        table_snapshot.table_name for table_snapshot in CONFIGURED_SOURCE_TABLE_SNAPSHOT_REPOSITORY.table_snapshots
    ),
    ids=lambda table_name: f"table_name={table_name}",
)
def test_validate_data_in_source_schema(
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    table_name: str,
    create_source_tables: None,
) -> None:
    """Verifies that the source schema contains the tables as described in the snapshot repository.

    This is useful to run when a persisted source schema is used to validate that the tables were properly created by a
    call to populate_source_schema().
    """
    if not mf_test_session_state.use_persistent_source_schema:
        pytest.skip("Skipping as this session is running without the persistent source schema flag.")

    schema_name = mf_test_session_state.mf_source_schema

    matching_table_snapshots = tuple(
        table_snapshot
        for table_snapshot in source_table_snapshot_repository.table_snapshots
        if table_snapshot.table_name == table_name
    )

    assert (
        len(matching_table_snapshots) == 1
    ), f"Did not get exactly one matching table snapshot for table name {table_name}. Got {matching_table_snapshots}"

    for table_snapshot in matching_table_snapshots:
        try:
            sql_table = SqlTable(schema_name=schema_name, table_name=table_snapshot.table_name)
            expected_table_df = table_snapshot.as_df
            actual_table_df = sql_client.query(f"SELECT * FROM {sql_table.sql}")
            assert_dataframes_equal(
                actual=actual_table_df,
                expected=expected_table_df,
                compare_names_using_lowercase=sql_client.sql_engine_type is SqlEngine.SNOWFLAKE,
            )
        except Exception as e:
            error_message = (
                f"Error verifying that a table corresponding to {table_snapshot} exists in the persistent source "
                f"schema {schema_name}. \nTry re-populating with: \n\n"
                f"{get_populate_source_schema_shell_command(sql_client.sql_engine_type)}"
            )
            # Add it to the warnings so that it stands out in a sea of test failures.
            warnings.warn(error_message)
            raise TableSnapshotException(error_message) from e
