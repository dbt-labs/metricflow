from __future__ import annotations

from unittest.mock import Mock

from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient, SupportedAdapterTypes
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.athena import AthenaSqlPlanRenderer


def test_supported_adapter_types_include_athena() -> None:
    """Athena should be registered as a supported adapter."""
    adapter_values = [adapter_type.value for adapter_type in SupportedAdapterTypes]

    assert "athena" in adapter_values
    assert SupportedAdapterTypes.ATHENA.sql_engine_type is SqlEngine.ATHENA
    assert isinstance(SupportedAdapterTypes.ATHENA.sql_plan_renderer, AthenaSqlPlanRenderer)


def test_adapter_backed_sql_client_supports_athena_adapter() -> None:
    """The dbt adapter wrapper should map athena to the Athena renderer."""
    mock_adapter = Mock()
    mock_adapter.type.return_value = "athena"

    sql_client = AdapterBackedSqlClient(mock_adapter)

    assert sql_client.sql_engine_type is SqlEngine.ATHENA
    assert isinstance(sql_client.sql_plan_renderer, AthenaSqlPlanRenderer)
