from __future__ import annotations

import logging
import os
from typing import Generator

import pytest
import sqlalchemy
from dbt.adapters.factory import get_adapter_by_type
from dbt.cli.main import dbtRunner

from metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql_clients.big_query import BigQuerySqlClient
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.databricks import DatabricksSqlClient
from metricflow.sql_clients.duckdb import DuckDbSqlClient
from metricflow.sql_clients.redshift import RedshiftSqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState, dialect_from_url

logger = logging.getLogger(__name__)


MF_SQL_ENGINE_DATABASE = "MF_SQL_ENGINE_DATABASE"
MF_SQL_ENGINE_HOST = "MF_SQL_ENGINE_HOST"
MF_SQL_ENGINE_PORT = "MF_SQL_ENGINE_PORT"
MF_SQL_ENGINE_USER = "MF_SQL_ENGINE_USER"
MF_SQL_ENGINE_SCHEMA = "MF_SQL_ENGINE_SCHEMA"
MF_SQL_ENGINE_WAREHOUSE = "MF_SQL_ENGINE_WAREHOUSE"


def configure_test_env_from_url(url: str, schema: str) -> sqlalchemy.engine.URL:
    """Populates default env var mapping from a sqlalchemy URL string.

    This is used to configure the test environment from the original MF_SQL_ENGINE_URL environment variable in
    a manner compatible with the dbt profile configurations laid out for most supported engines. We return
    the parsed URL object so that individual engine configurations can override the environment variables
    as needed to match their dbt profile configuration.
    """
    parsed_url = sqlalchemy.engine.make_url(url)

    assert parsed_url.host, "Engine host is not set in engine connection URL!"
    os.environ[MF_SQL_ENGINE_HOST] = parsed_url.host

    assert parsed_url.username, "Username must be set in engine connection URL!"
    os.environ[MF_SQL_ENGINE_USER] = parsed_url.username

    assert parsed_url.database, "Database must be set in engine connection URL!"
    os.environ[MF_SQL_ENGINE_DATABASE] = parsed_url.database

    os.environ[MF_SQL_ENGINE_SCHEMA] = schema

    if parsed_url.port:
        os.environ[MF_SQL_ENGINE_PORT] = str(parsed_url.port)

    return parsed_url


def __initialize_dbt() -> None:
    """Invoke the dbt runner from the appropriate directory so we can fetch the relevant adapter.

    We use the debug command to initialize the profile and make it accessible. This has the nice property of
    triggering a failure with reasonable error messages in the event the dbt configs are not set up correctly.
    """
    dbt_dir = os.path.join(os.path.dirname(__file__), "dbt_projects/metricflow_testing/")
    dbtRunner().invoke(["-q", "debug"], project_dir=dbt_dir, PROFILES_DIR=dbt_dir)


def make_test_sql_client(url: str, password: str, schema: str) -> SqlClient:
    """Build SQL client based on env configs."""
    # TODO: Switch on an enum of adapter type when all engines are cut over
    dialect = dialect_from_url(url=url)

    if dialect == SqlDialect.REDSHIFT:
        return RedshiftSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.SNOWFLAKE:
        parsed_url = configure_test_env_from_url(url, schema)
        assert "warehouse" in parsed_url.normalized_query, "Sql engine URL params did not include Snowflake warehouse!"
        warehouses = parsed_url.normalized_query["warehouse"]
        assert len(warehouses) == 1, f"Found more than 1 warehouse in Snowflake URL: `{warehouses}`"
        os.environ[MF_SQL_ENGINE_WAREHOUSE] = warehouses[0]
        __initialize_dbt()
        return AdapterBackedSqlClient(adapter=get_adapter_by_type("snowflake"))
    elif dialect == SqlDialect.BIGQUERY:
        return BigQuerySqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.POSTGRESQL:
        configure_test_env_from_url(url, "mf_demo")
        __initialize_dbt()
        return AdapterBackedSqlClient(adapter=get_adapter_by_type("postgres"))
    elif dialect == SqlDialect.DUCKDB:
        return DuckDbSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.DATABRICKS:
        return DatabricksSqlClient.from_connection_details(url, password)
    else:
        raise ValueError(f"Unknown dialect: `{dialect}` in URL {url}")


@pytest.fixture(scope="session")
def sql_client(mf_test_session_state: MetricFlowTestSessionState) -> Generator[SqlClient, None, None]:
    """Provides an SqlClient requiring warehouse access."""
    sql_client = make_test_sql_client(
        url=mf_test_session_state.sql_engine_url,
        password=mf_test_session_state.sql_engine_password,
        schema=mf_test_session_state.mf_source_schema,
    )
    logger.info(f"Creating schema '{mf_test_session_state.mf_system_schema}'")
    sql_client.create_schema(mf_test_session_state.mf_system_schema)
    if mf_test_session_state.mf_system_schema != mf_test_session_state.mf_source_schema:
        logger.info(f"Creating schema '{mf_test_session_state.mf_source_schema}'")
        sql_client.create_schema(mf_test_session_state.mf_source_schema)

    yield sql_client

    logger.info(f"Dropping schema '{mf_test_session_state.mf_system_schema}'")
    sql_client.drop_schema(mf_test_session_state.mf_system_schema, cascade=True)
    if (
        mf_test_session_state.mf_system_schema != mf_test_session_state.mf_source_schema
        and not mf_test_session_state.use_persistent_source_schema
    ):
        logger.info(f"Dropping schema '{mf_test_session_state.mf_source_schema}'")
        sql_client.drop_schema(mf_test_session_state.mf_source_schema, cascade=True)

    sql_client.close()
    return None
