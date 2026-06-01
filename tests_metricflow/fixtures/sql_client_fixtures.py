from __future__ import annotations

import json
import logging
import os
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Generator

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.adapters.factory import get_adapter_by_type
from dbt.cli.main import dbtRunner
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
from tests_metricflow.fixtures.sql_clients.adapter_backed_ddl_client import AdapterBackedDDLSqlClient
from tests_metricflow.fixtures.sql_clients.common_client import SqlDialect
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.fixtures.sql_clients.sqlalchemy_ddl_client import SqlAlchemyDDLSqlClient
from tests_metricflow.fixtures.sql_clients.sqlalchemy_url_builder import SqlAlchemyUrlBuilder

logger = logging.getLogger(__name__)


# Env vars for Athena dbt profile configuration. We use DBT_ENV_SECRET to avoid leaking secrets in CI logs.
AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
AWS_DEFAULT_REGION = "AWS_DEFAULT_REGION"
AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
AWS_SECURITY_TOKEN = "AWS_SECURITY_TOKEN"
AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN"
DBT_ENV_SECRET_AWS_PROFILE_NAME = "DBT_ENV_SECRET_AWS_PROFILE_NAME"
DBT_ENV_SECRET_DATABASE = "DBT_ENV_SECRET_DATABASE"
DBT_ENV_SECRET_REGION_NAME = "DBT_ENV_SECRET_REGION_NAME"
DBT_ENV_SECRET_S3_STAGING_DIR = "DBT_ENV_SECRET_S3_STAGING_DIR"
DBT_ENV_SECRET_SCHEMA = "DBT_ENV_SECRET_SCHEMA"


def _athena_dbt_project_dir() -> str:
    """Return the dbt project dir used for Athena adapter initialization in tests."""
    return os.path.join(os.path.dirname(__file__), Path("dbt_projects", "metricflow_testing"))


def _clear_athena_auth_env() -> None:
    """Clear Athena AWS auth env vars managed by this module."""
    for env_var in (
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_SESSION_TOKEN,
        AWS_SECURITY_TOKEN,
    ):
        os.environ.pop(env_var, None)


def _clear_athena_env() -> None:
    """Clear Athena-specific env vars so each run starts from a clean configuration state."""
    _clear_athena_auth_env()
    for env_var in (
        AWS_DEFAULT_REGION,
        DBT_ENV_SECRET_AWS_PROFILE_NAME,
        DBT_ENV_SECRET_DATABASE,
        DBT_ENV_SECRET_REGION_NAME,
        DBT_ENV_SECRET_S3_STAGING_DIR,
        DBT_ENV_SECRET_SCHEMA,
    ):
        os.environ.pop(env_var, None)


def _configure_athena_env_from_connection_parameters(
    connection_parameters: SqlEngineConnectionParameterSet, password: str, schema: str
) -> None:
    """Populate Athena-specific dbt and AWS environment variables from parsed connection parameters."""
    for env_var in (
        AWS_DEFAULT_REGION,
        DBT_ENV_SECRET_AWS_PROFILE_NAME,
        DBT_ENV_SECRET_DATABASE,
        DBT_ENV_SECRET_REGION_NAME,
        DBT_ENV_SECRET_S3_STAGING_DIR,
        DBT_ENV_SECRET_SCHEMA,
    ):
        os.environ.pop(env_var, None)
    aws_profile_names = connection_parameters.get_query_field_values("aws_profile_name")
    if len(aws_profile_names) > 1:
        raise ValueError(f"SQL engine URL specified multiple Athena aws_profile_name values: {aws_profile_names}")

    if aws_profile_names or connection_parameters.username or password:
        _clear_athena_auth_env()

    if aws_profile_names:
        os.environ[DBT_ENV_SECRET_AWS_PROFILE_NAME] = aws_profile_names[0]
    else:
        if connection_parameters.username:
            os.environ[AWS_ACCESS_KEY_ID] = connection_parameters.username

        if password:
            os.environ[AWS_SECRET_ACCESS_KEY] = password

    region_names = connection_parameters.get_query_field_values("region_name")
    if len(region_names) != 1:
        raise ValueError(f"SQL engine URL did not specify exactly 1 Athena region_name! Got {region_names}")
    os.environ[DBT_ENV_SECRET_REGION_NAME] = region_names[0]
    os.environ[AWS_DEFAULT_REGION] = region_names[0]

    s3_staging_dirs = connection_parameters.get_query_field_values("s3_staging_dir")
    if len(s3_staging_dirs) != 1:
        raise ValueError(f"SQL engine URL did not specify exactly 1 Athena s3_staging_dir! Got {s3_staging_dirs}")
    os.environ[DBT_ENV_SECRET_S3_STAGING_DIR] = s3_staging_dirs[0]

    if not connection_parameters.database:
        raise ValueError("SQL engine URL did not specify an Athena database/catalog in the URL path.")
    os.environ[DBT_ENV_SECRET_DATABASE] = connection_parameters.database

    os.environ[DBT_ENV_SECRET_SCHEMA] = schema


@lru_cache(maxsize=None)
def _initialize_dbt(project_dir: str, profiles_dir: str) -> None:
    """Invoke the dbt runner from the appropriate directory so we can fetch the relevant adapter."""
    dbtRunner().invoke(["debug"], project_dir=project_dir, profiles_dir=profiles_dir)


@pytest.fixture
def cleanup_athena_env() -> Generator[None, None, None]:
    """Clear Athena-specific env vars around tests that exercise Athena setup."""
    _clear_athena_env()
    yield
    _clear_athena_env()


def make_test_sql_client(url: str, password: str, schema: str) -> SqlClientWithDDLMethods:
    """Build test SQL client based on url, password, and schema defined in test environment."""
    connection_params = SqlEngineConnectionParameterSet.create_from_url(url)
    dialect = SqlDialect(connection_params.dialect)

    if dialect is SqlDialect.ATHENA:
        _configure_athena_env_from_connection_parameters(
            connection_params, password=password, schema=schema
        )
        project_dir = _athena_dbt_project_dir()
        _initialize_dbt(project_dir=project_dir, profiles_dir=project_dir)
        return AdapterBackedDDLSqlClient(adapter=get_adapter_by_type("athena"))

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
    """Provides a SqlClient with the necessary DDL and data loading methods for test configuration."""
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
    if sql_client.sql_engine_type is SqlEngine.ATHENA:
        _clear_athena_env()
    return None


@pytest.fixture(scope="session")
def sql_client(ddl_sql_client: SqlClientWithDDLMethods) -> SqlClient:
    """Provides a standard SqlClient instance for running MetricFlow tests."""
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

    if num_workers > 1 or num_items < 10:
        return

    if (dialect is SqlDialect.DUCKDB or dialect is SqlDialect.POSTGRESQL) and num_items < 30:
        return

    if num_items > 10:
        warnings.warn(
            f"This test session with {dialect.name} and {num_items} item(s) is running with {num_workers} worker(s). "
            f'Consider using the pytest-xdist option "-n <number of workers>" to parallelize execution and speed '
            f"up the session."
        )
