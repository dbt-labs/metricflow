import logging
import os
from pathlib import Path

import pandas as pd
import pytest

from metricflow.dataflow.sql_table import SqlTable
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
    SqlTableSnapshotRestorer,
)

logger = logging.getLogger(__name__)

DEFAULT_DS = "ds"


def create_table(sql_client: SqlClient, sql_table: SqlTable, df: pd.DataFrame) -> None:  # noqa: D
    # TODO: Replace with table_exists() check.
    sql_client.drop_table(sql_table)

    sql_client.create_table_from_dataframe(
        sql_table=sql_table,
        df=df,
    )


@pytest.fixture(scope="session")
def source_table_snapshot_repository() -> SqlTableSnapshotRepository:  # noqa: D
    return SqlTableSnapshotRepository(Path(os.path.dirname(__file__)).joinpath("source_table_snapshots"))


@pytest.fixture(scope="session")
def create_source_tables(
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Creates all tables that should be in the source schema.

    If a table with a given name already exists in the source schema, it's assumed to have the expected schema / data.
    """
    schema_name = mf_test_session_state.mf_source_schema
    # Figure out which tables are missing from the source schema.
    expected_table_names = sorted(
        [table_snapshot.table_name for table_snapshot in source_table_snapshot_repository.table_snapshots]
    )
    logger.info(
        f"The following tables are needed in schema {schema_name}:\n" f"{pformat_big_objects(expected_table_names)}"
    )
    source_schema_table_names = sorted(sql_client.list_tables(schema_name=schema_name))

    missing_table_names = set(expected_table_names).difference(source_schema_table_names)
    logger.info(
        f"The following tables are missing and will be restored:\n"
        f"{pformat_big_objects(sorted(missing_table_names))}"
    )
    # Restore the ones that are missing.
    snapshot_restorer = SqlTableSnapshotRestorer(
        sql_client=sql_client, schema_name=mf_test_session_state.mf_source_schema
    )
    for table_snapshot in source_table_snapshot_repository.table_snapshots:
        if table_snapshot.table_name in missing_table_names:
            logger.info(f"Restoring: {table_snapshot.table_name}")
            snapshot_restorer.restore(table_snapshot)
