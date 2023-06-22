from __future__ import annotations

import logging

from dbt_semantic_interfaces.pretty_print import pformat_big_objects

from metricflow.protocols.sql_client import SqlClient
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
    SqlTableSnapshotRestorer,
)

logger = logging.getLogger(__name__)


def create_tables_listed_in_table_snapshot_repository(
    sql_client: SqlClient,
    schema_name: str,
    table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Creates all tables in the table snapshot repository in the given schema.

    If a table with a given name already exists in the source schema, it's assumed to have the expected schema / data.
    """
    # Figure out which tables are missing from the source schema.
    expected_table_names = sorted(
        [table_snapshot.table_name for table_snapshot in table_snapshot_repository.table_snapshots]
    )
    logger.info(
        f"The following tables will be created if they don't exist in {schema_name}:\n" 
        f"{pformat_big_objects(expected_table_names)}"
    )
    source_schema_table_names = sorted(sql_client.list_tables(schema_name=schema_name))

    missing_table_names = set(expected_table_names).difference(source_schema_table_names)
    logger.info(
        f"The following tables are missing and will be restored:\n"
        f"{pformat_big_objects(sorted(missing_table_names))}"
    )
    # Restore the ones that are missing.
    snapshot_restorer = SqlTableSnapshotRestorer(sql_client=sql_client, schema_name=schema_name)
    for table_snapshot in table_snapshot_repository.table_snapshots:
        if table_snapshot.table_name in missing_table_names:
            logger.info(f"Restoring: {table_snapshot.table_name}")
            snapshot_restorer.restore(table_snapshot)
