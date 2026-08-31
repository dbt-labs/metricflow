"""ClickHouse outer-join NULL contract.

ClickHouse fills unmatched LEFT/FULL OUTER JOIN cells with type defaults (0, '') unless
`join_use_nulls = 1`. MetricFlow fill-nulls / ratio metrics assume SQL NULL.
"""

from __future__ import annotations

import pytest

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlClient, SqlEngine


def _extract_data_table_value(data_table: MetricFlowDataTable) -> object:
    assert data_table.row_count == 1
    assert data_table.column_count == 1
    return data_table.get_cell_value(0, 0)


def _skip_if_not_clickhouse(sql_client: SqlClient) -> None:
    if sql_client.sql_engine_type is not SqlEngine.CLICKHOUSE:
        pytest.skip("ClickHouse join_use_nulls contract")


def test_unmatched_left_join_is_sql_null_with_query_settings(sql_client: SqlClient) -> None:
    """Prove ClickHouse returns SQL NULL for unmatched outer-join cells when join_use_nulls is set."""
    _skip_if_not_clickhouse(sql_client)
    result = sql_client.query(
        """
        SELECT t2.x AS x
        FROM (SELECT 1 AS id) AS t1
        LEFT JOIN (SELECT 2 AS id, 5 AS x) AS t2 ON t1.id = t2.id
        SETTINGS join_use_nulls = 1
        """
    )
    assert _extract_data_table_value(result) is None


def test_unmatched_left_join_is_sql_null_without_query_settings(sql_client: SqlClient) -> None:
    """Session / URL settings must also make unmatched outer-join cells SQL NULL."""
    _skip_if_not_clickhouse(sql_client)
    result = sql_client.query(
        """
        SELECT t2.x AS x
        FROM (SELECT 1 AS id) AS t1
        LEFT JOIN (SELECT 2 AS id, 5 AS x) AS t2 ON t1.id = t2.id
        """
    )
    assert _extract_data_table_value(result) is None
