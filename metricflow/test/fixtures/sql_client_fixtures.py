import logging

import pytest
from typing import Generator

from metricflow.sql_clients.sql_utils import make_sql_client
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def sql_client(mf_test_session_state: MetricFlowTestSessionState) -> Generator[SqlClient, None, None]:
    """Provides a base SqlClient for use in integration tests requiring warehouse access."""
    sql_client = make_sql_client(
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
    if mf_test_session_state.mf_system_schema != mf_test_session_state.mf_source_schema:
        logger.info(f"Dropping schema '{mf_test_session_state.mf_source_schema}'")
        sql_client.drop_schema(mf_test_session_state.mf_source_schema, cascade=True)

    sql_client.close()
    return None
