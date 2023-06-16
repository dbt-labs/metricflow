from __future__ import annotations

import logging
from typing import Generator

import pytest

from metricflow.protocols.sql_client import SqlClient
from metricflow.sql_clients.big_query import BigQuerySqlClient
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.databricks import DatabricksSqlClient
from metricflow.sql_clients.duckdb import DuckDbSqlClient
from metricflow.sql_clients.postgres import PostgresSqlClient
from metricflow.sql_clients.redshift import RedshiftSqlClient
from metricflow.sql_clients.snowflake import SnowflakeSqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState, dialect_from_url

logger = logging.getLogger(__name__)


def make_test_sql_client(url: str, password: str) -> SqlClient:
    """Build SQL client based on env configs."""
    dialect = dialect_from_url(url=url)

    if dialect == SqlDialect.REDSHIFT:
        return RedshiftSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.SNOWFLAKE:
        return SnowflakeSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.BIGQUERY:
        return BigQuerySqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.POSTGRESQL:
        return PostgresSqlClient.from_connection_details(url, password)
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
