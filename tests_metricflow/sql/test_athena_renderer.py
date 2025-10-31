from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlBetweenExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlGenerateUuidExpression,
    SqlIntegerExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringLiteralExpression,
    SqlSubtractTimeIntervalExpression,
    SqlExtractExpression,
)
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.athena import AthenaSqlExpressionRenderer, AthenaSqlPlanRenderer
from metricflow.sql.sql_plan import SqlSelectColumn
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.compare_sql_plan import assert_rendered_sql_equal


@pytest.fixture
def athena_expr_renderer() -> AthenaSqlExpressionRenderer:
    """Fixture for Athena expression renderer."""
    return AthenaSqlExpressionRenderer()


@pytest.fixture
def athena_plan_renderer() -> AthenaSqlPlanRenderer:
    """Fixture for Athena plan renderer."""
    return AthenaSqlPlanRenderer()


class TestAthenaSqlExpressionRenderer:
    """Test cases for Athena SQL expression rendering."""

    def test_sql_engine_property(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test that the renderer reports the correct SQL engine."""
        assert athena_expr_renderer.sql_engine == SqlEngine.ATHENA

    def test_supported_percentile_function_types(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test that Athena supports only approximate continuous percentiles."""
        supported_types = athena_expr_renderer.supported_percentile_function_types
        assert supported_types == {SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS}

    def test_generate_uuid_expr(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test UUID generation renders correctly for Athena."""
        uuid_expr = SqlGenerateUuidExpression.create()
        result = athena_expr_renderer.visit_generate_uuid_expr(uuid_expr)

        assert result.sql == "uuid()"
        assert len(result.bind_parameter_set.param_dict) == 0

    def test_subtract_time_interval_expr(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test time subtraction renders correctly for Athena."""
        base_expr = SqlColumnReferenceExpression.create(SqlColumnReference("alias", "my_column"))
        subtract_expr = SqlSubtractTimeIntervalExpression.create(
            arg=base_expr,
            count=5,
            granularity=TimeGranularity.DAY
        )

        result = athena_expr_renderer.visit_subtract_time_interval_expr(subtract_expr)
        assert result.sql == "DATE_ADD('day', -5, alias.my_column)"

    def test_subtract_time_interval_quarter_conversion(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test that quarters are converted to months in time subtraction."""
        base_expr = SqlColumnReferenceExpression.create(SqlColumnReference("alias", "date_col"))
        subtract_expr = SqlSubtractTimeIntervalExpression.create(
            arg=base_expr,
            count=2,
            granularity=TimeGranularity.QUARTER
        )

        result = athena_expr_renderer.visit_subtract_time_interval_expr(subtract_expr)
        # 2 quarters = 6 months
        assert result.sql == "DATE_ADD('month', -6, alias.date_col)"

    def test_add_time_expr(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test time addition renders correctly for Athena."""
        base_expr = SqlColumnReferenceExpression.create(SqlColumnReference("alias", "timestamp_col"))
        count_expr = SqlIntegerExpression.create(10)
        add_expr = SqlAddTimeExpression.create(
            arg=base_expr,
            count_expr=count_expr,
            granularity=TimeGranularity.HOUR
        )

        result = athena_expr_renderer.visit_add_time_expr(add_expr)
        assert result.sql == "DATE_ADD('hour', 10, alias.timestamp_col)"

    def test_add_time_quarter_conversion(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test that quarters are converted to months in time addition."""
        base_expr = SqlColumnReferenceExpression.create(SqlColumnReference("alias", "date_col"))
        count_expr = SqlIntegerExpression.create(1)
        add_expr = SqlAddTimeExpression.create(
            arg=base_expr,
            count_expr=count_expr,
            granularity=TimeGranularity.QUARTER
        )

        result = athena_expr_renderer.visit_add_time_expr(add_expr)
        # 1 quarter = 3 months, should multiply the count expression
        assert "DATE_ADD('month'," in result.sql
        assert "timestamp_col" in result.sql or "date_col" in result.sql

    def test_percentile_expr_approximate_continuous(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test approximate continuous percentile rendering."""
        column_expr = SqlColumnReferenceExpression.create(SqlColumnReference("alias", "value_column"))
        percentile_expr = SqlPercentileExpression.create(
            order_by_arg=column_expr,
            percentile_args=SqlPercentileExpressionArgument(
                percentile=0.5,
                function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
            )
        )

        result = athena_expr_renderer.visit_percentile_expr(percentile_expr)
        assert result.sql == "approx_percentile(alias.value_column, 0.5)"

    def test_percentile_expr_unsupported_types(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test that unsupported percentile types raise errors."""
        column_expr = SqlColumnReferenceExpression.create(SqlColumnReference("alias", "value_column"))

        # Test discrete percentile raises error
        discrete_percentile = SqlPercentileExpression.create(
            order_by_arg=column_expr,
            percentile_args=SqlPercentileExpressionArgument(
                percentile=0.5,
                function_type=SqlPercentileFunctionType.DISCRETE
            )
        )

        with pytest.raises(RuntimeError, match="not supported for Athena"):
            athena_expr_renderer.visit_percentile_expr(discrete_percentile)

    def test_between_expr_with_timestamps(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test BETWEEN expression with timestamp literals."""
        column_expr = SqlColumnReferenceExpression.create(SqlColumnReference("alias", "event_time"))
        start_expr = SqlStringLiteralExpression.create("'2023-01-01'")
        end_expr = SqlStringLiteralExpression.create("'2023-12-31'")

        between_expr = SqlBetweenExpression.create(
            column_arg=column_expr,
            start_expr=start_expr,
            end_expr=end_expr
        )

        result = athena_expr_renderer.visit_between_expr(between_expr)
        # Should handle potential timestamp parsing gracefully
        assert "alias.event_time BETWEEN" in result.sql
        assert "'2023-01-01'" in result.sql
        assert "'2023-12-31'" in result.sql

    def test_render_date_part_day_of_week(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test that DAY_OF_WEEK is rendered correctly for Athena."""
        result = athena_expr_renderer.render_date_part(DatePart.DOW)
        assert result == "DAY_OF_WEEK"

    def test_render_date_part_standard(self, athena_expr_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test that standard date parts are rendered as-is."""
        result = athena_expr_renderer.render_date_part(DatePart.YEAR)
        assert result == "year"


class TestAthenaSqlPlanRenderer:
    """Test cases for Athena SQL plan rendering."""

    def test_expr_renderer_property(self, athena_plan_renderer: AthenaSqlPlanRenderer) -> None:
        """Test that the plan renderer uses the correct expression renderer."""
        expr_renderer = athena_plan_renderer.expr_renderer
        assert isinstance(expr_renderer, AthenaSqlExpressionRenderer)
        assert expr_renderer.sql_engine == SqlEngine.ATHENA


class TestAthenaEngineProperties:
    """Test SqlEngine properties for Athena."""

    def test_unsupported_granularities(self) -> None:
        """Test that Athena has correct unsupported granularities."""
        unsupported = SqlEngine.ATHENA.unsupported_granularities
        expected = {TimeGranularity.NANOSECOND, TimeGranularity.MICROSECOND}
        assert unsupported == expected

    def test_engine_value(self) -> None:
        """Test that Athena engine has correct string value."""
        assert SqlEngine.ATHENA.value == "Athena"


@pytest.mark.sql_engine_snapshot
def test_athena_uuid_rendering_integration(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Integration test for UUID rendering in Athena SQL plans."""
    # This test will automatically run against Athena when the environment is available
    select_columns = [
        SqlSelectColumn(
            expr=SqlGenerateUuidExpression.create(),
            column_alias="generated_uuid",
        ),
    ]

    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="test_schema", table_name="test_table"))

    sql_plan = SqlSelectStatementNode.create(
        description="Test Athena UUID Generation",
        select_columns=tuple(select_columns),
        from_source=from_source,
        from_source_alias="t",
    )

    # Create a mock SQL client for Athena to test rendering
    from metricflow.sql.render.athena import AthenaSqlPlanRenderer
    athena_renderer = AthenaSqlPlanRenderer()

    # Test that the SQL renders without error
    rendered_sql = athena_renderer.render_sql_plan(sql_plan)
    assert "uuid()" in rendered_sql.sql
    assert "test_schema.test_table" in rendered_sql.sql


@pytest.mark.sql_engine_snapshot
def test_athena_time_operations_integration(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Integration test for time operations in Athena SQL plans."""
    base_column = SqlColumnReferenceExpression.create(SqlColumnReference("alias", "event_timestamp"))

    select_columns = [
        SqlSelectColumn(
            expr=SqlAddTimeExpression.create(
                arg=base_column,
                count_expr=SqlIntegerExpression.create(1),
                granularity=TimeGranularity.DAY
            ),
            column_alias="next_day",
        ),
        SqlSelectColumn(
            expr=SqlSubtractTimeIntervalExpression.create(
                arg=base_column,
                count=7,
                granularity=TimeGranularity.DAY
            ),
            column_alias="last_week",
        ),
    ]

    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="analytics", table_name="events"))

    sql_plan = SqlSelectStatementNode.create(
        description="Test Athena Time Operations",
        select_columns=tuple(select_columns),
        from_source=from_source,
        from_source_alias="e",
    )

    # Test rendering
    from metricflow.sql.render.athena import AthenaSqlPlanRenderer
    athena_renderer = AthenaSqlPlanRenderer()
    rendered_sql = athena_renderer.render_sql_plan(sql_plan)

    assert "DATE_ADD('day', 1, alias.event_timestamp)" in rendered_sql.sql
    assert "DATE_ADD('day', -7, alias.event_timestamp)" in rendered_sql.sql