from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from metricflow.test.source_schema_tools import create_tables_listed_in_table_snapshot_repository
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
)

logger = logging.getLogger(__name__)


# Prefer to use the fixture, but there are some cases where fixtures aren't available. e.g. when defining a
# parameterized test.
CONFIGURED_SOURCE_TABLE_SNAPSHOT_REPOSITORY = SqlTableSnapshotRepository(
    Path(os.path.dirname(__file__)).joinpath("source_table_snapshots")
)


@pytest.fixture(scope="session")
def source_table_snapshot_repository() -> SqlTableSnapshotRepository:  # noqa: D
    return CONFIGURED_SOURCE_TABLE_SNAPSHOT_REPOSITORY


@pytest.fixture(scope="session", autouse=True)
def check_time_spine_source(
    mf_test_session_state: MetricFlowTestSessionState,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Check that the time spine source follows the definition in the table snapshot.

    The time spine table is defined in a table snapshot YAML file and is restored to the source schema based on that
    definition. The definition in the YAML should align with the definition in the time_spine_source fixture.
    """
    time_spine_source = simple_semantic_manifest_lookup.time_spine_source
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
    ddl_sql_client: SqlClientWithDDLMethods,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Creates all tables that should be in the source schema.

    If a table with a given name already exists in the source schema, it's assumed to have the expected schema / data.
    """
    if mf_test_session_state.use_persistent_source_schema:
        logger.info(
            "This session was configured to use a persistent source schema, so this fixture won't create new tables. "
            "See populate_source_schema() for more details."
        )
        return

    create_tables_listed_in_table_snapshot_repository(
        ddl_sql_client=ddl_sql_client,
        schema_name=mf_test_session_state.mf_source_schema,
        table_snapshot_repository=source_table_snapshot_repository,
    )
