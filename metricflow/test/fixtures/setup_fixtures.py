from __future__ import annotations

import datetime
import logging
import os
import warnings
from dataclasses import dataclass

import _pytest.config
import pytest
import sqlalchemy.util
from _pytest.fixtures import FixtureRequest
from sqlalchemy.engine import make_url

from metricflow.random_id import random_id
from metricflow.test.fixtures.sql_clients.common_client import SqlDialect
from metricflow.test.table_snapshot.table_snapshots import SqlTableSnapshotHash, SqlTableSnapshotRepository

logger = logging.getLogger(__name__)


@dataclass
class MetricFlowTestSessionState:
    """State that is shared between tests during a testing session."""

    sql_engine_url: str
    sql_engine_password: str
    # Where MF system tables should be stored.
    mf_system_schema: str
    # Where tables for test data sets should be stored.
    mf_source_schema: str

    # Number of plans that were displayed to the user.
    display_plans: bool
    # Whether to overwrite any text files that were generated.
    overwrite_snapshots: bool
    # Number of plans that were displayed to the user.
    plans_displayed: int
    # Maximum number of plans to display to the user. If this is exceeded, an exception should be thrown. This is to
    # help avoid a case where an excessive number of plans are displayed. This could happen if the user accidentally
    # runs all tests when they were just looking to run one test and visualize the associated plan.
    max_plans_displayed: int

    # The source schema contains tables that are used for running tests. If this is set, a source schema in the SQL
    # is created and persisted between runs. The source schema name includes a hash of the tables that should be in
    # the schema, so
    use_persistent_source_schema: bool


DISPLAY_PLANS_CLI_FLAG = "--display-plans"
OVERWRITE_SNAPSHOTS_CLI_FLAG = "--overwrite-snapshots"
USE_PERSISTENT_SOURCE_SCHEMA_CLI_FLAG = "--use-persistent-source-schema"


def pytest_addoption(parser: _pytest.config.argparsing.Parser) -> None:
    """Add options for running pytest through the CLI."""
    parser.addoption(DISPLAY_PLANS_CLI_FLAG, action="store_true", help="Displays plans as SVGs in a browser tab if set")
    parser.addoption(
        OVERWRITE_SNAPSHOTS_CLI_FLAG,
        action="store_true",
        help="Overwrites existing snapshots by ones generated during this testing session",
    )
    parser.addoption(
        USE_PERSISTENT_SOURCE_SCHEMA_CLI_FLAG,
        action="store_true",
        help="Use a source schema that is persisted between testing sessions. The name of the schema is generated from"
        "a hash of the source data, and the schema is created / populated if it does not exist.",
    )


@pytest.fixture(scope="session")
def mf_test_session_state(  # noqa: D
    request: FixtureRequest,
    disable_sql_alchemy_deprecation_warning: None,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
) -> MetricFlowTestSessionState:
    engine_url = os.environ.get("MF_SQL_ENGINE_URL")
    assert engine_url is not None, (
        "MF_SQL_ENGINE_URL environment variable has not been set! Are you running in a properly configured "
        "environment? Check out our CONTRIBUTING.md for pointers to our environment configurations."
    )
    engine_password = os.environ.get("MF_SQL_ENGINE_PASSWORD", "")

    current_time = datetime.datetime.now().strftime("%Y_%m_%d")
    random_suffix = random_id()
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

        logger.info(
            f"Since the flag {USE_PERSISTENT_SOURCE_SCHEMA_CLI_FLAG} was specified, this session will use the "
            f"persistent source schema {mf_source_schema}. If the required source tables do not exist in this "
            f"schema, they will be created. However, the source schema (and the associated tables) will not "
            f"be dropped at the end of the testing session."
        )
    else:
        mf_source_schema = default_source_schema

    return MetricFlowTestSessionState(
        sql_engine_url=engine_url,
        sql_engine_password=engine_password,
        mf_system_schema=mf_system_schema,
        mf_source_schema=mf_source_schema,
        display_plans=bool(request.config.getoption(DISPLAY_PLANS_CLI_FLAG, default=False)),
        overwrite_snapshots=bool(request.config.getoption(OVERWRITE_SNAPSHOTS_CLI_FLAG, default=False)),
        plans_displayed=0,
        max_plans_displayed=6,
        use_persistent_source_schema=bool(
            request.config.getoption(USE_PERSISTENT_SOURCE_SCHEMA_CLI_FLAG, default=False)
        ),
    )


def dialect_from_url(url: str) -> SqlDialect:
    """Return the SQL dialect specified in the URL in the configuration."""
    dialect_protocol = make_url(url.split(";")[0]).drivername.split("+")
    if len(dialect_protocol) > 2:
        raise ValueError(f"Invalid # of +'s in {url}")
    return SqlDialect(dialect_protocol[0])


@pytest.fixture(scope="session", autouse=True)
def warn_user_about_slow_tests_without_parallelism(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    worker_count_env_var = os.environ.get("PYTEST_XDIST_WORKER_COUNT", "1")
    try:
        num_workers = int(worker_count_env_var)
    except ValueError as e:
        raise ValueError(
            f"Could not convert environment variable PYTEST_XDIST_WORKER_COUNT to int! "
            f"Value in environ was: {worker_count_env_var}"
        ) from e

    num_items = len(request.session.items)
    dialect = dialect_from_url(mf_test_session_state.sql_engine_url)

    # If already running in parallel or if there's not many test items, no need to print the warning. Picking 10/30 as
    # the thresholds, but not much thought has been put into it.
    if num_workers > 1 or num_items < 10:
        return

    # Since DuckDB / Postgres is fast, use 30 as the threshold.
    if (dialect is SqlDialect.DUCKDB or dialect is SqlDialect.POSTGRESQL) and num_items < 30:
        return

    if num_items > 10:
        warnings.warn(
            f"This test session with {dialect.name} and {num_items} item(s) is running with {num_workers} worker(s). "
            f'Consider using the pytest-xdist option "-n <number of workers>" to parallelize execution and speed '
            f"up the session."
        )


@pytest.fixture(scope="session", autouse=True)
def disable_sql_alchemy_deprecation_warning() -> None:
    """Since MF is tied to using SQLAlchemy 1.x.x due to the Snowflake connector, silence 2.0 deprecation warnings."""
    # Seeing 'error: Module has no attribute "SILENCE_UBER_WARNING"' in the type checker, but this seems to work.
    sqlalchemy.util.deprecations.SILENCE_UBER_WARNING = True  # type:ignore
