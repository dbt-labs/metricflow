from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.time.time_spine_source import TimeSpineSource
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.source_schema_tools import create_tables_listed_in_table_snapshot_repository
from tests_metricflow.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
)

logger = logging.getLogger(__name__)


# Prefer to use the fixture, but there are some cases where fixtures aren't available. e.g. when defining a
# parameterized test.
CONFIGURED_SOURCE_TABLE_SNAPSHOT_REPOSITORY = SqlTableSnapshotRepository(
    Path(os.path.dirname(__file__)).joinpath("source_table_snapshots")
)


@pytest.fixture(scope="session")
def source_table_snapshot_repository() -> SqlTableSnapshotRepository:  # noqa: D103
    return CONFIGURED_SOURCE_TABLE_SNAPSHOT_REPOSITORY


@pytest.fixture(scope="session", autouse=True)
def check_time_spine_source(
    mf_test_configuration: MetricFlowTestConfiguration,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Check that the time spine source follows the definition in the table snapshot.

    The time spine table is defined in a table snapshot YAML file and is restored to the source schema based on that
    definition. The definition in the YAML should align with the definition in the time_spine_source fixture.
    """
    time_spine_source = TimeSpineSource.build_standard_time_spine_sources(
        simple_semantic_manifest_lookup.semantic_manifest
    )[TimeGranularity.DAY]
    assert (
        time_spine_source.sql_table.schema_name == mf_test_configuration.mf_source_schema
    ), "The time spine source table should be in the source schema"

    time_spine_snapshot_candidates = tuple(
        snapshot
        for snapshot in source_table_snapshot_repository.table_snapshots
        if snapshot.table_name == time_spine_source.sql_table.table_name
    )

    assert len(time_spine_snapshot_candidates) == 1, (
        f"Did not get exactly one table snapshot matching the time_spine_source table name. "
        f"Got: {time_spine_snapshot_candidates}"
    )

    time_spine_snapshot = time_spine_snapshot_candidates[0]

    # This assumes the time spine column is the first column in the snapshot.
    time_column = time_spine_snapshot.column_definitions[0]
    assert time_column.name == time_spine_source.base_column


@pytest.fixture(scope="session")
def create_source_tables(
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Creates all tables that should be in the source schema.

    If a table with a given name already exists in the source schema, it's assumed to have the expected schema / data.
    """
    if mf_test_configuration.use_persistent_source_schema:
        logger.debug(
            LazyFormat(
                lambda: "This session was configured to use a persistent source schema, so this fixture won't create new tables. "
                "See populate_source_schema() for more details."
            )
        )
        return

    create_tables_listed_in_table_snapshot_repository(
        ddl_sql_client=ddl_sql_client,
        schema_name=mf_test_configuration.mf_source_schema,
        table_snapshot_repository=source_table_snapshot_repository,
    )
