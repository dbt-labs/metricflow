from __future__ import annotations

import datetime
import logging
import os
from pathlib import Path

import _pytest.config
import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    DISPLAY_SNAPSHOTS_CLI_FLAG,
    OVERWRITE_SNAPSHOTS_CLI_FLAG,
    add_display_snapshots_cli_flag,
    add_overwrite_snapshots_cli_flag,
)
from metricflow_semantics.toolkit.id_helpers import mf_random_id
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from tests_metricflow_semantics.fixtures.setup_fixtures import mf_add_slow_marker

from metricflow.protocols.sql_client import SqlClient, SqlEngine
from tests_metricflow import TESTS_METRICFLOW_DIRECTORY_ANCHOR
from tests_metricflow.table_snapshot.table_snapshots import SqlTableSnapshotHash, SqlTableSnapshotRepository

logger = logging.getLogger(__name__)


DISPLAY_GRAPHS_CLI_FLAG = "--display-graphs"
USE_PERSISTENT_SOURCE_SCHEMA_CLI_FLAG = "--use-persistent-source-schema"


def add_display_graphs_cli_flag(parser: _pytest.config.argparsing.Parser) -> None:  # noqa: D103
    parser.addoption(
        DISPLAY_GRAPHS_CLI_FLAG,
        action="store_true",
        help="Allow display of graphs in a browser window when triggered in a test",
    )


def add_use_persistent_source_schema_cli_flag(parser: _pytest.config.argparsing.Parser) -> None:  # noqa: D103
    parser.addoption(
        USE_PERSISTENT_SOURCE_SCHEMA_CLI_FLAG,
        action="store_true",
        help="Use a source schema that is persisted between testing sessions. The name of the schema is generated from"
        "a hash of the source data, and the schema is created / populated if it does not exist.",
    )


def pytest_addoption(parser: _pytest.config.argparsing.Parser) -> None:
    """Add options for running pytest through the CLI."""
    add_overwrite_snapshots_cli_flag(parser)
    add_display_snapshots_cli_flag(parser)
    add_display_graphs_cli_flag(parser)
    add_use_persistent_source_schema_cli_flag(parser)


# Name of the pytest marker for tests that generate SQL-engine specific snapshots.
SQL_ENGINE_SNAPSHOT_MARKER_NAME = "sql_engine_snapshot"
# Name of the pytest marker to indicate that the test should only be run when the test session is configured to use
# DuckDB as the SQL engine.
DUCKDB_ONLY_MARKER_NAME = "duckdb_only"


def pytest_configure(config: _pytest.config.Config) -> None:
    """Hook as specified by the pytest API for configuration."""
    config.addinivalue_line(
        name="markers",
        line=f"{SQL_ENGINE_SNAPSHOT_MARKER_NAME}: mark tests as generating a snapshot specific to a SQL engine.",
    )
    config.addinivalue_line(
        name="markers",
        line=f"{DUCKDB_ONLY_MARKER_NAME}: mark tests as one that should only be run with DuckDB.",
    )
    mf_add_slow_marker(config)


def check_sql_engine_snapshot_marker(request: FixtureRequest) -> None:
    """Raises an error if the given test request does not have the sql-engine-test marker set."""
    if request.node.get_closest_marker(SQL_ENGINE_SNAPSHOT_MARKER_NAME) is None:
        raise ValueError(
            f"This test needs to be marked with {SQL_ENGINE_SNAPSHOT_MARKER_NAME!r} to keep track of all tests that "
            f"generate SQL-engine specific snapshots."
        )


@pytest.fixture(autouse=True)
def skip_if_not_duck_db(request: FixtureRequest, sql_client: SqlClient) -> None:
    """Skip tests if the test is marked for testing with DuckDB only, but the test session uses a different engine."""
    if request.node.get_closest_marker(DUCKDB_ONLY_MARKER_NAME) is None:
        return

    test_session_engine_type = sql_client.sql_engine_type
    duckdb_engine_type = SqlEngine.DUCKDB
    if test_session_engine_type is not duckdb_engine_type:
        pytest.skip(f"Skipping as this test is only run with {duckdb_engine_type}, but {test_session_engine_type=}")


@pytest.fixture(scope="session")
def mf_test_configuration(  # noqa: D103
    request: FixtureRequest,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
) -> MetricFlowTestConfiguration:
    engine_url = os.environ.get("MF_SQL_ENGINE_URL")
    assert engine_url is not None, (
        "MF_SQL_ENGINE_URL environment variable has not been set! Are you running in a properly configured "
        "environment? Check out our CONTRIBUTING.md for pointers to our environment configurations."
    )
    engine_password = os.environ.get("MF_SQL_ENGINE_PASSWORD", "")

    current_time = datetime.datetime.now().strftime("%Y_%m_%d")
    random_suffix = mf_random_id()
    mf_system_schema = f"mf_test_{current_time}_{random_suffix}"
    default_source_schema = mf_system_schema

    use_persistent_source_schema = bool(request.config.getoption(USE_PERSISTENT_SOURCE_SCHEMA_CLI_FLAG, default=False))
    if use_persistent_source_schema:
        source_table_snapshots_hash = SqlTableSnapshotHash.create_from_hashes(
            tuple(table_snapshot.snapshot_hash for table_snapshot in source_table_snapshot_repository.table_snapshots)
        )
        # Since the source schema is used in the SQL output snapshots (but replaced with *'s), make the name of the
        # schema to be the same length so that the SQL output snapshots don't change.
        persistent_source_schema_prefix = "mf_test_src_"

        # Ensure that there are a reasonable number of hash digits in the schema so that we don't get collisions.
        available_hash_digits = max(len(default_source_schema) - len(persistent_source_schema_prefix), 0)
        if available_hash_digits < 8:
            raise RuntimeError(
                f"The generated name for the persistent source schema would have {available_hash_digits} data hash "
                f"digits, which is not enough to have low collisions. default_source_schema: {default_source_schema} "
                f"source_schema_prefix: {persistent_source_schema_prefix}"
            )

        # Use as many digits of the hash without exceeding the length of the default source schema.
        mf_source_schema = f"{persistent_source_schema_prefix}{source_table_snapshots_hash.str_value}"[
            : len(default_source_schema)
        ]

        if len(mf_source_schema) != len(default_source_schema):
            raise RuntimeError(
                f"The persistent source schema should be the same length as the default source schema to keep the "
                f"generated SQL snapshots consistent between non-persistent and persistent modes. persistent source "
                f"schema: {mf_source_schema} default source schema: {default_source_schema}"
            )

        logger.debug(
            LazyFormat(
                lambda: f"Since the flag {USE_PERSISTENT_SOURCE_SCHEMA_CLI_FLAG} was specified, this session will use the "
                f"persistent source schema {mf_source_schema}. If the required source tables do not exist in this "
                f"schema, they will be created. However, the source schema (and the associated tables) will not "
                f"be dropped at the end of the testing session."
            )
        )
    else:
        mf_source_schema = default_source_schema

    return MetricFlowTestConfiguration(
        sql_engine_url=engine_url,
        sql_engine_password=engine_password,
        mf_system_schema=mf_system_schema,
        mf_source_schema=mf_source_schema,
        display_snapshots=bool(request.config.getoption(DISPLAY_SNAPSHOTS_CLI_FLAG, default=False)),
        display_graphs=bool(request.config.getoption(DISPLAY_GRAPHS_CLI_FLAG, default=False)),
        overwrite_snapshots=bool(request.config.getoption(OVERWRITE_SNAPSHOTS_CLI_FLAG, default=False)),
        use_persistent_source_schema=bool(
            request.config.getoption(USE_PERSISTENT_SOURCE_SCHEMA_CLI_FLAG, default=False)
        ),
        snapshot_directory=TESTS_METRICFLOW_DIRECTORY_ANCHOR.directory.joinpath("snapshots"),
        tests_directory=TESTS_METRICFLOW_DIRECTORY_ANCHOR.directory,
    )


def dbt_project_dir() -> str:
    """Return the canonical path string for the dbt project dir in the test package.

    This is necessary for configuring both the dbt adapter for integration tests and the project location for CLI tests.
    """
    return os.path.join(os.path.dirname(__file__), Path("dbt_projects", "metricflow_testing"))
