from __future__ import annotations

import logging

from metricflow_semantics.sql.sql_table import SqlTable

from metricflow.protocols.sql_client import SqlEngine
from tests_metricflow.fixtures.sql_clients.sqlalchemy_ddl_client import make_create_table_statement

logger = logging.getLogger(__name__)


def test_clickhouse_create_table_statement_includes_engine() -> None:
    """Checks ClickHouse fixture tables have a deterministic table engine."""
    statement = make_create_table_statement(
        sql_engine_type=SqlEngine.CLICKHOUSE,
        sql_table=SqlTable(schema_name="test_schema", table_name="test_table"),
        column_defs=("id Nullable(Int64)",),
    )

    assert statement == (
        "CREATE TABLE IF NOT EXISTS test_schema.test_table (id Nullable(Int64)) "
        "ENGINE = MergeTree ORDER BY tuple()"
    )


def test_postgres_create_table_statement_does_not_include_clickhouse_engine() -> None:
    """Checks non-ClickHouse fixture tables keep the generic DDL."""
    statement = make_create_table_statement(
        sql_engine_type=SqlEngine.POSTGRES,
        sql_table=SqlTable(schema_name="test_schema", table_name="test_table"),
        column_defs=("id bigint",),
    )

    assert statement == "CREATE TABLE IF NOT EXISTS test_schema.test_table (id bigint)"
