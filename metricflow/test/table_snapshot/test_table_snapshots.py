from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest
from dbt_semantic_interfaces.test_utils import as_datetime

from metricflow.protocols.sql_client import SqlEngine
from metricflow.random_id import random_id
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableColumnDefinition,
    SqlTableColumnType,
    SqlTableSnapshot,
    SqlTableSnapshotLoader,
    SqlTableSnapshotRepository,
)


@pytest.fixture
def table_snapshot() -> SqlTableSnapshot:  # noqa: D
    rows = (
        ("true", "1", "1.0", "2020-01-02", "hi"),
        ("false", "-1", "-1.0", "2020-03-04 05:06:07", "bye"),
    )

    return SqlTableSnapshot(
        table_name="example_snapshot",
        column_definitions=(
            SqlTableColumnDefinition(name="col0", type=SqlTableColumnType.BOOLEAN),
            SqlTableColumnDefinition(name="col1", type=SqlTableColumnType.INT),
            SqlTableColumnDefinition(name="col2", type=SqlTableColumnType.FLOAT),
            SqlTableColumnDefinition(name="col3", type=SqlTableColumnType.TIME),
            SqlTableColumnDefinition(name="col4", type=SqlTableColumnType.STRING),
        ),
        rows=rows,
        file_path=Path("/a/b/c"),
    )


def test_as_df(table_snapshot: SqlTableSnapshot) -> None:
    """Check that SqlTableSnapshot.as_df works as expected."""
    assert_dataframes_equal(
        actual=table_snapshot.as_df,
        expected=pd.DataFrame(
            columns=[f"col{i}" for i in range(5)],
            data=(
                (True, 1, 1.0, as_datetime("2020-01-02"), "hi"),
                (False, -1, -1.0, as_datetime("2020-03-04 05:06:07"), "bye"),
            ),
        ),
    )


def test_load(
    mf_test_session_state: MetricFlowTestSessionState,
    ddl_sql_client: SqlClientWithDDLMethods,
    table_snapshot: SqlTableSnapshot,
) -> None:
    """Test loading a snapshot to the engine."""
    schema_name = f"mf_test_snapshot_schema_{random_id()}"

    try:
        ddl_sql_client.create_schema(schema_name)

        snapshot_loader = SqlTableSnapshotLoader(ddl_sql_client=ddl_sql_client, schema_name=schema_name)
        snapshot_loader.load(table_snapshot)

        actual = ddl_sql_client.query(f"SELECT * FROM {schema_name}.{table_snapshot.table_name}")
        assert_dataframes_equal(
            actual=actual,
            expected=table_snapshot.as_df,
            compare_names_using_lowercase=ddl_sql_client.sql_engine_type is SqlEngine.SNOWFLAKE,
        )

    finally:
        ddl_sql_client.drop_schema(schema_name, cascade=True)


def test_snapshot_repository() -> None:
    """Tests that the snapshot repository loads snapshot YAML files correctly."""
    repo = SqlTableSnapshotRepository(config_directory=Path(os.path.dirname(__file__)))
    assert len(repo.table_snapshots) == 1

    # Replace the filepath so it can be compared consistently between hosts.
    example_snapshot = repo.table_snapshots[0]
    dummy_file_path = Path("/a/b/c")
    snapshot_to_check = SqlTableSnapshot(
        table_name=example_snapshot.table_name,
        column_definitions=example_snapshot.column_definitions,
        rows=example_snapshot.rows,
        file_path=dummy_file_path,
    )

    assert snapshot_to_check == SqlTableSnapshot(
        table_name="example_table",
        column_definitions=(
            SqlTableColumnDefinition(name="user_id", type=SqlTableColumnType.INT),
            SqlTableColumnDefinition(name="name", type=SqlTableColumnType.STRING),
            SqlTableColumnDefinition(name="balance", type=SqlTableColumnType.FLOAT),
            SqlTableColumnDefinition(name="is_active", type=SqlTableColumnType.BOOLEAN),
        ),
        rows=(
            ("0", "user0", "0.0", "False"),
            ("1", "user1", "0.1", "True"),
        ),
        file_path=dummy_file_path,
    )
