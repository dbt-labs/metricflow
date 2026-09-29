from __future__ import annotations

from unittest.mock import MagicMock

from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient, SupportedAdapterTypes
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.clickhouse import ClickHouseSqlPlanRenderer


def test_supported_adapter_types_include_clickhouse() -> None:
    """ClickHouse should be registered as a supported adapter."""
    adapter_values = [adapter_type.value for adapter_type in SupportedAdapterTypes]

    assert "clickhouse" in adapter_values
    assert SupportedAdapterTypes.CLICKHOUSE.sql_engine_type is SqlEngine.CLICKHOUSE
    assert isinstance(SupportedAdapterTypes.CLICKHOUSE.sql_plan_renderer, ClickHouseSqlPlanRenderer)


def test_adapter_backed_sql_client_supports_clickhouse_adapter() -> None:
    """The dbt adapter wrapper should map clickhouse to the ClickHouse renderer."""
    mock_adapter = MagicMock()
    mock_adapter.type.return_value = "clickhouse"

    sql_client = AdapterBackedSqlClient(mock_adapter)

    assert sql_client.sql_engine_type is SqlEngine.CLICKHOUSE
    assert isinstance(sql_client.sql_plan_renderer, ClickHouseSqlPlanRenderer)


def _clickhouse_sql_client() -> tuple[AdapterBackedSqlClient, MagicMock]:
    mock_adapter = MagicMock()
    mock_adapter.type.return_value = "clickhouse"
    return AdapterBackedSqlClient(mock_adapter), mock_adapter


def test_clickhouse_dry_run_uses_query_tree_for_select() -> None:
    """SELECT dry-run must use EXPLAIN QUERY TREE and must not issue session SET."""
    sql_client, mock_adapter = _clickhouse_sql_client()
    stmt = "SELECT 1 AS x\nSETTINGS join_use_nulls = 1"

    sql_client.dry_run(stmt)

    mock_adapter.execute.assert_called_once()
    explain_sql = mock_adapter.execute.call_args.args[0]
    assert explain_sql == f"EXPLAIN QUERY TREE {stmt}"
    assert "SET join_use_nulls" not in explain_sql


def test_clickhouse_dry_run_uses_syntax_for_ddl() -> None:
    """CREATE TABLE AS is not a query tree; EXPLAIN SYNTAX covers it."""
    sql_client, mock_adapter = _clickhouse_sql_client()
    stmt = "CREATE TABLE t AS SELECT 1"

    sql_client.dry_run(stmt)

    mock_adapter.execute.assert_called_once()
    assert mock_adapter.execute.call_args.args[0] == f"EXPLAIN SYNTAX {stmt}"


def test_clickhouse_query_does_not_issue_session_set() -> None:
    """join_use_nulls lives on compiled SQL; the adapter must not SET it."""
    sql_client, mock_adapter = _clickhouse_sql_client()
    row = MagicMock()
    row.values.return_value = (1,)
    table = MagicMock()
    table.rows = [row]
    table.column_names = ["x"]
    mock_adapter.execute.return_value = ("ok", table)

    sql_client.query("SELECT 1 AS x\nSETTINGS join_use_nulls = 1")

    executed_sql = [
        call.kwargs.get("sql") or (call.args[0] if call.args else None) for call in mock_adapter.execute.call_args_list
    ]
    assert executed_sql == ["SELECT 1 AS x\nSETTINGS join_use_nulls = 1"]
