from __future__ import annotations

import logging

import pytest

from metricflow.protocols.sql_client import SqlClient, SqlEngine

logger = logging.getLogger(__name__)


@pytest.mark.duckdb_only
def test_duckdb_only(sql_client: SqlClient) -> None:
    """Check that the `duckdb_only` skips tests when the tests are run with another SQL engine.

    This depends on auto-using of the `skip_if_not_duck_db` fixture.
    """
    duckdb_engine_type = SqlEngine.DUCKDB
    engine_type_under_test = sql_client.sql_engine_type
    assert engine_type_under_test is duckdb_engine_type, (
        f"This test should have only been run when using {duckdb_engine_type}, but the engine type under test is"
        f" {engine_type_under_test}"
    )
