from __future__ import annotations

import pytest
from unittest.mock import Mock

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.athena import AthenaSqlPlanRenderer


class TestAthenaAdapterRegistration:
    """Test cases for Athena adapter registration and integration."""

    def test_supported_adapter_types_includes_athena(self) -> None:
        """Test that ATHENA is included in SupportedAdapterTypes."""
        from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import SupportedAdapterTypes

        # Check that ATHENA is in the enum
        adapter_values = [adapter_type.value for adapter_type in SupportedAdapterTypes]
        assert "athena" in adapter_values

        # Check that we can get the ATHENA enum member
        athena_adapter = SupportedAdapterTypes.ATHENA
        assert athena_adapter.value == "athena"

    def test_athena_sql_engine_type_mapping(self) -> None:
        """Test that Athena adapter type maps to correct SqlEngine."""
        from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import SupportedAdapterTypes

        athena_adapter = SupportedAdapterTypes.ATHENA
        sql_engine = athena_adapter.sql_engine_type

        assert sql_engine == SqlEngine.ATHENA

    def test_athena_sql_plan_renderer_mapping(self) -> None:
        """Test that Athena adapter type returns correct renderer."""
        from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import SupportedAdapterTypes

        athena_adapter = SupportedAdapterTypes.ATHENA
        renderer = athena_adapter.sql_plan_renderer

        assert isinstance(renderer, AthenaSqlPlanRenderer)

    def test_adapter_backed_sql_client_with_athena_adapter(self) -> None:
        """Test that AdapterBackedSqlClient works with Athena adapter."""
        from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient

        # Create a mock dbt adapter that reports athena type
        mock_adapter = Mock()
        mock_adapter.type.return_value = "athena"

        # This should not raise an exception
        sql_client = AdapterBackedSqlClient(mock_adapter)

        # Verify the client is configured correctly
        assert sql_client.sql_engine_type == SqlEngine.ATHENA
        assert isinstance(sql_client.sql_plan_renderer, AthenaSqlPlanRenderer)

    def test_adapter_backed_sql_client_unsupported_adapter_raises_error(self) -> None:
        """Test that unsupported adapter types raise appropriate errors."""
        from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient

        # Create a mock dbt adapter with unsupported type
        mock_adapter = Mock()
        mock_adapter.type.return_value = "unsupported_engine"

        # This should raise a ValueError
        with pytest.raises(ValueError, match="Adapter type unsupported_engine is not supported"):
            AdapterBackedSqlClient(mock_adapter)

    def test_all_supported_adapters_have_engine_mapping(self) -> None:
        """Test that all supported adapter types have engine mappings."""
        from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import SupportedAdapterTypes

        # This test ensures we don't miss any mappings when adding new adapters
        for adapter_type in SupportedAdapterTypes:
            # This should not raise assert_values_exhausted error
            engine = adapter_type.sql_engine_type
            assert isinstance(engine, SqlEngine)

    def test_all_supported_adapters_have_renderer_mapping(self) -> None:
        """Test that all supported adapter types have renderer mappings."""
        from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import SupportedAdapterTypes
        from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer

        # This test ensures we don't miss any renderer mappings when adding new adapters
        for adapter_type in SupportedAdapterTypes:
            # This should not raise assert_values_exhausted error
            renderer = adapter_type.sql_plan_renderer
            assert isinstance(renderer, SqlPlanRenderer)

    def test_athena_renderer_import_accessible(self) -> None:
        """Test that Athena renderer can be imported from expected location."""
        # This test ensures the import path is correct
        from metricflow.sql.render.athena import AthenaSqlPlanRenderer, AthenaSqlExpressionRenderer

        # Should be able to instantiate without error
        plan_renderer = AthenaSqlPlanRenderer()
        expr_renderer = AthenaSqlExpressionRenderer()

        assert plan_renderer is not None
        assert expr_renderer is not None
        assert plan_renderer.expr_renderer.sql_engine == SqlEngine.ATHENA


class TestAthenaEngineIntegration:
    """Test cases for Athena engine integration with MetricFlow systems."""

    def test_sql_engine_enum_completeness(self) -> None:
        """Test that SqlEngine enum includes Athena and all engines are handled."""
        from metricflow.protocols.sql_client import SqlEngine

        # Athena should be in the enum
        assert hasattr(SqlEngine, 'ATHENA')
        assert SqlEngine.ATHENA.value == "Athena"

        # Test that unsupported_granularities property works for all engines including Athena
        for engine in SqlEngine:
            # This should not raise assert_values_exhausted error
            unsupported = engine.unsupported_granularities
            assert isinstance(unsupported, set)

    def test_athena_engine_properties(self) -> None:
        """Test specific properties of the Athena engine."""
        from metricflow.protocols.sql_client import SqlEngine
        from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

        athena_engine = SqlEngine.ATHENA

        # Test basic properties
        assert athena_engine.value == "Athena"

        # Test unsupported granularities (should match Trino's limitations)
        unsupported = athena_engine.unsupported_granularities
        expected_unsupported = {TimeGranularity.NANOSECOND, TimeGranularity.MICROSECOND}
        assert unsupported == expected_unsupported

    def test_athena_in_engine_listings(self) -> None:
        """Test that Athena appears in engine enumerations."""
        from metricflow.protocols.sql_client import SqlEngine

        all_engines = list(SqlEngine)
        engine_values = [engine.value for engine in all_engines]

        assert SqlEngine.ATHENA in all_engines
        assert "Athena" in engine_values