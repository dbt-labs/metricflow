from __future__ import annotations

import datetime
import logging
import os
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import pytest
from dbt_semantic_interfaces.pretty_print import pformat_big_objects

from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
    SqlTableSnapshotRestorer,
)
from metricflow.time.time_granularity import TimeGranularity

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
    _create_time_spine_table_if_necessary(time_spine_source=time_spine_source, local_sql_client=sql_client)

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


def _create_time_spine_table_if_necessary(time_spine_source: TimeSpineSource, local_sql_client: SqlClient) -> None:
    """Creates a time spine table for the given time spine source.

    Note this covers a broader-than-necessary time range to ensure test updates work as expected.
    """
    if local_sql_client.table_exists(time_spine_source.spine_table):
        return
    assert (
        time_spine_source.time_column_granularity is TimeGranularity.DAY
    ), f"A time granularity of {time_spine_source.time_column_granularity} is not yet supported."
    current_period = TimeRangeConstraint.ALL_TIME_BEGIN()
    # Using a union type throws a type error for some reason, so going with this approach
    time_spine_table_data: List[Tuple[datetime.datetime]] = []

    while current_period <= TimeRangeConstraint.ALL_TIME_END():
        time_spine_table_data.append((current_period,))
        current_period = current_period + datetime.timedelta(days=1)

    local_sql_client.drop_table(time_spine_source.spine_table)
    len(time_spine_table_data)

    local_sql_client.create_table_from_dataframe(
        sql_table=time_spine_source.spine_table,
        df=pd.DataFrame(
            columns=[time_spine_source.time_column_name],
            data=time_spine_table_data,
        ),
        chunk_size=1000,
    )
