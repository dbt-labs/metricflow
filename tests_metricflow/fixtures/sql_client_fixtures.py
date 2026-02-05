from __future__ import annotations

import json
import logging
import os
import warnings
from typing import Generator

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from sqlalchemy import create_engine, make_url

from metricflow.protocols.sql_client import SqlClient, SqlEngine
from metricflow.sql.render.big_query import BigQuerySqlPlanRenderer
from metricflow.sql.render.databricks import DatabricksSqlPlanRenderer
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer
from metricflow.sql.render.postgres import PostgresSQLSqlPlanRenderer
from metricflow.sql.render.redshift import RedshiftSqlPlanRenderer
from metricflow.sql.render.snowflake import SnowflakeSqlPlanRenderer
from metricflow.sql.render.trino import TrinoSqlPlanRenderer
from tests_metricflow.fixtures.connection_url import SqlEngineConnectionParameterSet
from tests_metricflow.fixtures.sql_clients.common_client import SqlDialect
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.fixtures.sql_clients.sqlalchemy_ddl_client import SqlAlchemyDDLSqlClient
from tests_metricflow.fixtures.sql_clients.sqlalchemy_url_builder import SqlAlchemyUrlBuilder

logger = logging.getLogger(__name__)


def make_test_sql_client(url: str, password: str, schema: str) -> SqlClientWithDDLMethods:
    """Build test SQL client based on url, password, and schema defined in test environment.

    Apart from BigQuery, these all have the same basic API, differing only in how the URL object is constructed.
    BigQuery is in its own branch here because of the way it handles credentials and engine configuration properties.

    This is the standard implementation for our test runners, and all engines should use this unless there is no
    way to use SqlAlchemy with that engine type.
    """
    connection_params = SqlEngineConnectionParameterSet.create_from_url(url)
    dialect = SqlDialect(connection_params.dialect)
    # Build SqlAlchemy URL
    sqlalchemy_url = SqlAlchemyUrlBuilder.build_url(connection_params, password, schema)

    if dialect is SqlDialect.BIGQUERY:
        # `strict=False` required to work with BQ password characters.
        bq_credentials = json.loads(password, strict=False)
        engine = create_engine(
            sqlalchemy_url,
            pool_pre_ping=True,  # Verify connections before using
            echo=False,  # Set to True for SQL logging
            credentials_info=bq_credentials,
        )
        # copy and update the base URL to add the dry_run query parameter
        dry_run_url = make_url(sqlalchemy_url).update_query_dict({"dry_run": "true"})
        dry_run_engine = create_engine(
            dry_run_url,
            pool_pre_ping=True,  # Verify connections before using
            echo=False,  # Set to True for SQL logging
            credentials_info=bq_credentials,
        )
    else:
        # Create engine with connection pooling
        engine = create_engine(
            sqlalchemy_url,
            pool_pre_ping=True,  # Verify connections before using
            echo=False,  # Set to True for SQL logging
        )
        dry_run_engine = engine

    # Map dialect to engine type and renderer
    dialect_mapping = {
        SqlDialect.DUCKDB: (SqlEngine.DUCKDB, DuckDbSqlPlanRenderer()),
        SqlDialect.DATABRICKS: (SqlEngine.DATABRICKS, DatabricksSqlPlanRenderer()),
        SqlDialect.POSTGRESQL: (SqlEngine.POSTGRES, PostgresSQLSqlPlanRenderer()),
        SqlDialect.SNOWFLAKE: (SqlEngine.SNOWFLAKE, SnowflakeSqlPlanRenderer()),
        SqlDialect.REDSHIFT: (SqlEngine.REDSHIFT, RedshiftSqlPlanRenderer()),
        SqlDialect.BIGQUERY: (SqlEngine.BIGQUERY, BigQuerySqlPlanRenderer()),
        SqlDialect.TRINO: (SqlEngine.TRINO, TrinoSqlPlanRenderer()),
    }

    if dialect not in dialect_mapping:
        raise ValueError(f"Unsupported dialect: {dialect}")

    sql_engine_type, sql_plan_renderer = dialect_mapping[dialect]

    return SqlAlchemyDDLSqlClient(
        engine=engine,
        sql_engine_type=sql_engine_type,
        sql_plan_renderer=sql_plan_renderer,
        dry_run_engine=dry_run_engine,
    )


@pytest.fixture(scope="session")
def ddl_sql_client(
    mf_test_configuration: MetricFlowTestConfiguration,
) -> Generator[SqlClientWithDDLMethods, None, None]:
    """Provides a SqlClient with the necessary DDL and data loading methods for test configuration.

    This allows us to provide the operations necessary for executing the test suite without exposing those methods in
    MetricFlow's core SqlClient protocol.
    """
    sql_client = make_test_sql_client(
        url=mf_test_configuration.sql_engine_url,
        password=mf_test_configuration.sql_engine_password,
        schema=mf_test_configuration.mf_source_schema,
    )
    logger.debug(LazyFormat(lambda: f"Creating schema '{mf_test_configuration.mf_system_schema}'"))
    sql_client.create_schema(mf_test_configuration.mf_system_schema)
    if mf_test_configuration.mf_system_schema != mf_test_configuration.mf_source_schema:
        logger.debug(LazyFormat(lambda: f"Creating schema '{mf_test_configuration.mf_source_schema}'"))
        sql_client.create_schema(mf_test_configuration.mf_source_schema)

    yield sql_client

    logger.debug(LazyFormat(lambda: f"Dropping schema '{mf_test_configuration.mf_system_schema}'"))
    sql_client.drop_schema(mf_test_configuration.mf_system_schema, cascade=True)
    if (
        mf_test_configuration.mf_system_schema != mf_test_configuration.mf_source_schema
        and not mf_test_configuration.use_persistent_source_schema
    ):
        logger.debug(LazyFormat(lambda: f"Dropping schema '{mf_test_configuration.mf_source_schema}'"))
        sql_client.drop_schema(mf_test_configuration.mf_source_schema, cascade=True)

    sql_client.close()
    return None


@pytest.fixture(scope="session")
def sql_client(ddl_sql_client: SqlClientWithDDLMethods) -> SqlClient:
    """Provides a standard SqlClient instance for running MetricFlow tests.

    Unless the test case itself requires the DDL methods, this is the fixture we should use.
    """
    return ddl_sql_client


@pytest.fixture(scope="session", autouse=True)
def warn_user_about_slow_tests_without_parallelism(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
    dialect = SqlDialect(SqlEngineConnectionParameterSet.create_from_url(mf_test_configuration.sql_engine_url).dialect)

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
