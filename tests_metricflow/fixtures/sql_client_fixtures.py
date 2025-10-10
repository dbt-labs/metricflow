from __future__ import annotations

import json
import logging
import os
import warnings
from typing import Generator

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.adapters.factory import get_adapter_by_type
from dbt.cli.main import dbtRunner
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.connection_url import SqlEngineConnectionParameterSet
from tests_metricflow.fixtures.setup_fixtures import dbt_project_dir
from tests_metricflow.fixtures.sql_clients.adapter_backed_ddl_client import AdapterBackedDDLSqlClient
from tests_metricflow.fixtures.sql_clients.common_client import SqlDialect
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods

logger = logging.getLogger(__name__)


# Env vars for dbt profile configuration. We use DBT_ENV_SECRET to avoid leaking secrets in CI logs.
# Note port cannot use the secret obfuscation because it must be type-cast to int
DBT_PROFILE_PORT = "DBT_PROFILE_PORT"
DBT_ENV_SECRET_DATABASE = "DBT_ENV_SECRET_DATABASE"
DBT_ENV_SECRET_HOST = "DBT_ENV_SECRET_HOST"
DBT_ENV_SECRET_HTTP_PATH = "DBT_ENV_SECRET_HTTP_PATH"
DBT_ENV_SECRET_PASSWORD = "DBT_ENV_SECRET_PASSWORD"
DBT_ENV_SECRET_SCHEMA = "DBT_ENV_SECRET_SCHEMA"
DBT_ENV_SECRET_USER = "DBT_ENV_SECRET_USER"
DBT_ENV_SECRET_WAREHOUSE = "DBT_ENV_SECRET_WAREHOUSE"

# BigQuery is special, so it gets its own set of env vars. Keeping them split out here for consistency.
DBT_ENV_SECRET_AUTH_PROVIDER_CERT_URL = "DBT_ENV_SECRET_AUTH_PROVIDER_CERT_URL"
DBT_ENV_SECRET_AUTH_TYPE = "DBT_ENV_SECRET_AUTH_TYPE"
DBT_ENV_SECRET_AUTH_URI = "DBT_ENV_SECRET_AUTH_URI"
DBT_ENV_SECRET_CLIENT_CERT_URL = "DBT_ENV_SECRET_CLIENT_CERT_URL"
DBT_ENV_SECRET_CLIENT_EMAIL = "DBT_ENV_SECRET_CLIENT_EMAIL"
DBT_ENV_SECRET_CLIENT_ID = "DBT_ENV_SECRET_CLIENT_ID"
DBT_ENV_SECRET_PRIVATE_KEY = "DBT_ENV_SECRET_PRIVATE_KEY"
DBT_ENV_SECRET_PRIVATE_KEY_ID = "DBT_ENV_SECRET_PRIVATE_KEY_ID"
DBT_ENV_SECRET_PROJECT_ID = "DBT_ENV_SECRET_PROJECT_ID"
DBT_ENV_SECRET_TOKEN_URI = "DBT_ENV_SECRET_TOKEN_URI"

# Trino is special, so it gets its own set of env vars. Keeping them split out here for consistency.
DBT_ENV_SECRET_CATALOG = "DBT_ENV_SECRET_CATALOG"


def __configure_test_env_from_url(url: str, password: str, schema: str) -> SqlEngineConnectionParameterSet:
    """Populates default env var mapping from a sqlalchemy URL string.

    This is used to configure the test environment from the original MF_SQL_ENGINE_URL environment variable in
    a manner compatible with the dbt profile configurations laid out for most supported engines. We return
    the parsed URL object so that individual engine configurations can override the environment variables
    as needed to match their dbt profile configuration.
    """
    connection_parameters = SqlEngineConnectionParameterSet.create_from_url(url)
    if connection_parameters.dialect != "duckdb":
        assert connection_parameters.hostname, "Engine host is not set in engine connection URL!"
        os.environ[DBT_ENV_SECRET_HOST] = connection_parameters.hostname

    if connection_parameters.username:
        os.environ[DBT_ENV_SECRET_USER] = connection_parameters.username

    if connection_parameters.database:
        os.environ[DBT_ENV_SECRET_DATABASE] = connection_parameters.database

    if connection_parameters.port:
        os.environ[DBT_PROFILE_PORT] = str(connection_parameters.port)

    os.environ[DBT_ENV_SECRET_PASSWORD] = password
    os.environ[DBT_ENV_SECRET_SCHEMA] = schema

    return connection_parameters


def __configure_bigquery_env_from_credential_string(password: str, schema: str) -> None:
    credential_string = password.replace("'", "")
    # `strict=False` required to work with BQ password characters.
    credentials = json.loads(credential_string, strict=False)

    assert isinstance(credentials, dict), "JSON credential string did not parse to dict type!"

    bq_env = {
        DBT_ENV_SECRET_AUTH_PROVIDER_CERT_URL: credentials.get("auth_provider_x509_cert_url"),
        DBT_ENV_SECRET_AUTH_TYPE: credentials.get("type"),
        DBT_ENV_SECRET_AUTH_URI: credentials.get("auth_uri"),
        DBT_ENV_SECRET_CLIENT_CERT_URL: credentials.get("client_x509_cert_url"),
        DBT_ENV_SECRET_CLIENT_EMAIL: credentials.get("client_email"),
        DBT_ENV_SECRET_CLIENT_ID: credentials.get("client_id"),
        DBT_ENV_SECRET_PRIVATE_KEY: credentials.get("private_key"),
        DBT_ENV_SECRET_PRIVATE_KEY_ID: credentials.get("private_key_id"),
        DBT_ENV_SECRET_PROJECT_ID: credentials.get("project_id"),
        DBT_ENV_SECRET_TOKEN_URI: credentials.get("token_uri"),
    }

    empty_keys = [k for k, v in bq_env.items() if v is None or v == ""]

    assert (
        not empty_keys
    ), f"BigQuery credentials did not contain all required values! Missing value for keys {empty_keys}."

    # Reconstruct the dict to refine the value type to str
    os.environ.update({k: v for k, v in bq_env.items() if v is not None})
    os.environ[DBT_ENV_SECRET_SCHEMA] = schema


def __configure_databricks_env_from_url(url: str, password: str, schema: str) -> None:
    """Databricks has a custom http path attribute, which we have encoded into a SqlAlchemy-like URL appendage.

    This custom parsing was ported from our original client implementation for backwards compatibility with the
    existing CI job configurations.
    """
    http_path_key = "httppath="
    url_pieces = url.split(";")
    http_paths = [url_piece for url_piece in url_pieces[1:] if url_piece.lower().startswith(http_path_key)]
    assert len(http_paths) == 1, (
        f"There should be exactly one http path in a Databricks test engine URL, but we found http paths: "
        f"{http_paths} in url {url}!"
    )
    http_path = http_paths[0].split("=")[1]
    os.environ[DBT_ENV_SECRET_HTTP_PATH] = http_path
    __configure_test_env_from_url(url_pieces[0], password=password, schema=schema)


def __initialize_dbt() -> None:
    """Invoke the dbt runner from the appropriate directory so we can fetch the relevant adapter.

    We use the debug command to initialize the profile and make it accessible. This has the nice property of
    triggering a failure with reasonable error messages in the event the dbt configs are not set up correctly.
    """
    dbtRunner().invoke(["debug"], project_dir=dbt_project_dir(), PROFILES_DIR=dbt_project_dir())


def make_test_sql_client(url: str, password: str, schema: str) -> SqlClientWithDDLMethods:
    """Build SQL client based on env configs."""
    # TODO: Switch on an enum of adapter type when all engines are cut over
    dialect = SqlDialect(SqlEngineConnectionParameterSet.create_from_url(url).dialect)

    if dialect is SqlDialect.REDSHIFT:
        __configure_test_env_from_url(url, password=password, schema=schema)
        __initialize_dbt()
        return AdapterBackedDDLSqlClient(adapter=get_adapter_by_type("redshift"))
    elif dialect is SqlDialect.SNOWFLAKE:
        connection_parameters = __configure_test_env_from_url(url, password=password, schema=schema)
        warehouse_names = connection_parameters.get_query_field_values("warehouse")
        assert (
            len(warehouse_names) == 1
        ), f"SQL engine URL did not specify exactly 1 Snowflake warehouse! Got {warehouse_names}"
        os.environ[DBT_ENV_SECRET_WAREHOUSE] = warehouse_names[0]
        __initialize_dbt()
        return AdapterBackedDDLSqlClient(adapter=get_adapter_by_type("snowflake"))
    elif dialect is SqlDialect.BIGQUERY:
        __configure_bigquery_env_from_credential_string(password=password, schema=schema)
        __initialize_dbt()
        return AdapterBackedDDLSqlClient(adapter=get_adapter_by_type("bigquery"))
    elif dialect is SqlDialect.POSTGRESQL:
        __configure_test_env_from_url(url, password=password, schema=schema)
        __initialize_dbt()
        return AdapterBackedDDLSqlClient(adapter=get_adapter_by_type("postgres"))
    elif dialect is SqlDialect.DUCKDB:
        __configure_test_env_from_url(url, password=password, schema=schema)
        __initialize_dbt()
        return AdapterBackedDDLSqlClient(adapter=get_adapter_by_type("duckdb"))
    elif dialect is SqlDialect.DATABRICKS:
        __configure_databricks_env_from_url(url, password=password, schema=schema)
        __initialize_dbt()
        return AdapterBackedDDLSqlClient(adapter=get_adapter_by_type("databricks"))
    elif dialect is SqlDialect.TRINO:
        __configure_test_env_from_url(url, password=password, schema=schema)
        __initialize_dbt()
        return AdapterBackedDDLSqlClient(adapter=get_adapter_by_type("trino"))
    else:
        raise ValueError(f"Unknown dialect: `{dialect}` in URL {url}")


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
