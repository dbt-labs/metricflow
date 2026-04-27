"""Tests for the Apache Doris SQL expression and plan renderers."""

from __future__ import annotations

import pytest
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlAggregateFunctionExpression,
    SqlCastToTimestampExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparisonExpression,
    SqlComparison,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlFunction,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlRatioComputationExpression,
    SqlStringExpression,
    SqlStringLiteralExpression,
    SqlSubtractTimeIntervalExpression,
)
from metricflow_semantics.sql.sql_table import SqlTable

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.doris import DorisSqlExpressionRenderer, DorisSqlPlanRenderer
from metricflow.sql.sql_plan import SqlSelectColumn
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode


@pytest.fixture
def doris_renderer() -> DorisSqlExpressionRenderer:
    """Fixture providing a Doris expression renderer."""
    return DorisSqlExpressionRenderer()


@pytest.fixture
def doris_plan_renderer() -> DorisSqlPlanRenderer:
    """Fixture providing a Doris plan renderer."""
    return DorisSqlPlanRenderer()


class TestDorisSqlEngine:
    """Tests for SqlEngine.DORIS registration and properties."""

    def test_doris_engine_exists(self) -> None:
        assert SqlEngine.DORIS is not None
        assert SqlEngine.DORIS.value == "Doris"

    def test_unsupported_granularities(self) -> None:
        unsupported = SqlEngine.DORIS.unsupported_granularities
        assert TimeGranularity.NANOSECOND in unsupported
        assert TimeGranularity.MICROSECOND in unsupported
        assert TimeGranularity.MILLISECOND in unsupported
        # These should be supported:
        assert TimeGranularity.SECOND not in unsupported
        assert TimeGranularity.MINUTE not in unsupported
        assert TimeGranularity.HOUR not in unsupported
        assert TimeGranularity.DAY not in unsupported
        assert TimeGranularity.WEEK not in unsupported
        assert TimeGranularity.MONTH not in unsupported
        assert TimeGranularity.QUARTER not in unsupported
        assert TimeGranularity.YEAR not in unsupported


class TestDorisExpressionRenderer:
    """Tests for DorisSqlExpressionRenderer."""

    def test_sql_engine(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        assert doris_renderer.sql_engine is SqlEngine.DORIS

    def test_double_data_type(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        assert doris_renderer.double_data_type == "DOUBLE"

    def test_timestamp_data_type(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        assert doris_renderer.timestamp_data_type == "DATETIME"

    def test_supported_percentile_function_types(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        supported = doris_renderer.supported_percentile_function_types
        assert SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS in supported
        assert len(supported) == 1

    def test_can_render_approximate_continuous_percentile(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        assert doris_renderer.can_render_percentile_function(SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS) is True

    def test_cannot_render_continuous_percentile(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        assert doris_renderer.can_render_percentile_function(SqlPercentileFunctionType.CONTINUOUS) is False

    def test_cannot_render_discrete_percentile(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        assert doris_renderer.can_render_percentile_function(SqlPercentileFunctionType.DISCRETE) is False

    def test_cannot_render_approximate_discrete_percentile(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        assert doris_renderer.can_render_percentile_function(SqlPercentileFunctionType.APPROXIMATE_DISCRETE) is False

    # -- Cast to Timestamp --

    def test_cast_to_timestamp(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        """Doris should cast to DATETIME instead of TIMESTAMP."""
        result = doris_renderer.render_sql_expr(
            SqlCastToTimestampExpression.create(
                arg=SqlStringLiteralExpression.create(literal_value="2020-01-01")
            )
        )
        assert result.sql == "CAST('2020-01-01' AS DATETIME)"

    # -- DATE_TRUNC --

    def test_date_trunc_day(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        """DATE_TRUNC should use standard order: DATE_TRUNC('day', col)."""
        result = doris_renderer.render_sql_expr(
            SqlDateTruncExpression.create(
                time_granularity=TimeGranularity.DAY,
                arg=SqlStringExpression.create("ds"),
            )
        )
        assert result.sql == "DATE_TRUNC('day', ds)"

    def test_date_trunc_month(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlDateTruncExpression.create(
                time_granularity=TimeGranularity.MONTH,
                arg=SqlStringExpression.create("ds"),
            )
        )
        assert result.sql == "DATE_TRUNC('month', ds)"

    def test_date_trunc_year(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlDateTruncExpression.create(
                time_granularity=TimeGranularity.YEAR,
                arg=SqlStringExpression.create("ds"),
            )
        )
        assert result.sql == "DATE_TRUNC('year', ds)"

    def test_date_trunc_quarter(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlDateTruncExpression.create(
                time_granularity=TimeGranularity.QUARTER,
                arg=SqlStringExpression.create("ds"),
            )
        )
        assert result.sql == "DATE_TRUNC('quarter', ds)"

    # -- EXTRACT --

    def test_extract_year(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlExtractExpression.create(
                date_part=DatePart.YEAR,
                arg=SqlStringExpression.create("ds"),
            )
        )
        assert result.sql == "EXTRACT(year FROM ds)"

    def test_extract_month(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlExtractExpression.create(
                date_part=DatePart.MONTH,
                arg=SqlStringExpression.create("ds"),
            )
        )
        assert result.sql == "EXTRACT(month FROM ds)"

    def test_extract_day(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlExtractExpression.create(
                date_part=DatePart.DAY,
                arg=SqlStringExpression.create("ds"),
            )
        )
        assert result.sql == "EXTRACT(day FROM ds)"

    def test_extract_doy(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlExtractExpression.create(
                date_part=DatePart.DOY,
                arg=SqlStringExpression.create("ds"),
            )
        )
        assert result.sql == "EXTRACT(doy FROM ds)"

    def test_extract_dow_iso_conversion(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        """DOW extraction should convert from Sunday=1..Saturday=7 to ISO Monday=1..Sunday=7."""
        result = doris_renderer.render_sql_expr(
            SqlExtractExpression.create(
                date_part=DatePart.DOW,
                arg=SqlStringExpression.create("ds"),
            )
        )
        # Should produce IF(EXTRACT(DOW FROM ds) = 1, 7, EXTRACT(DOW FROM ds) - 1)
        assert "IF(" in result.sql
        assert "EXTRACT(DOW FROM ds)" in result.sql
        assert "= 1, 7" in result.sql
        assert "- 1)" in result.sql

    # -- Subtract Time Interval --

    def test_subtract_time_interval_day(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlSubtractTimeIntervalExpression.create(
                arg=SqlColumnReferenceExpression.create(SqlColumnReference("t", "ds")),
                count=1,
                granularity=TimeGranularity.DAY,
            )
        )
        assert result.sql == "DATE_SUB(CAST(t.ds AS DATETIME), INTERVAL 1 day)"

    def test_subtract_time_interval_month(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlSubtractTimeIntervalExpression.create(
                arg=SqlColumnReferenceExpression.create(SqlColumnReference("t", "ds")),
                count=3,
                granularity=TimeGranularity.MONTH,
            )
        )
        assert result.sql == "DATE_SUB(CAST(t.ds AS DATETIME), INTERVAL 3 month)"

    def test_subtract_time_interval_quarter_converts_to_months(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        """QUARTER should be converted to 3 months."""
        result = doris_renderer.render_sql_expr(
            SqlSubtractTimeIntervalExpression.create(
                arg=SqlColumnReferenceExpression.create(SqlColumnReference("t", "ds")),
                count=2,
                granularity=TimeGranularity.QUARTER,
            )
        )
        assert result.sql == "DATE_SUB(CAST(t.ds AS DATETIME), INTERVAL 6 month)"

    def test_subtract_time_interval_year(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlSubtractTimeIntervalExpression.create(
                arg=SqlColumnReferenceExpression.create(SqlColumnReference("t", "ds")),
                count=1,
                granularity=TimeGranularity.YEAR,
            )
        )
        assert result.sql == "DATE_SUB(CAST(t.ds AS DATETIME), INTERVAL 1 year)"

    def test_subtract_time_interval_week(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlSubtractTimeIntervalExpression.create(
                arg=SqlColumnReferenceExpression.create(SqlColumnReference("t", "ds")),
                count=2,
                granularity=TimeGranularity.WEEK,
            )
        )
        assert result.sql == "DATE_SUB(CAST(t.ds AS DATETIME), INTERVAL 2 week)"

    # -- Add Time --

    def test_add_time_day(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlAddTimeExpression.create(
                arg=SqlStringLiteralExpression.create("2020-01-01"),
                count_expr=SqlStringExpression.create("1"),
                granularity=TimeGranularity.DAY,
            )
        )
        assert result.sql == "DATE_ADD(CAST('2020-01-01' AS DATETIME), INTERVAL 1 day)"

    def test_add_time_quarter_converts_to_months(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        """QUARTER should be converted to MONTH with count * 3."""
        result = doris_renderer.render_sql_expr(
            SqlAddTimeExpression.create(
                arg=SqlStringLiteralExpression.create("2020-01-01"),
                count_expr=SqlStringExpression.create("1"),
                granularity=TimeGranularity.QUARTER,
            )
        )
        assert "DATE_ADD(CAST('2020-01-01' AS DATETIME), INTERVAL" in result.sql
        assert "month" in result.sql

    def test_add_time_year(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlAddTimeExpression.create(
                arg=SqlStringLiteralExpression.create("2020-01-01"),
                count_expr=SqlStringExpression.create("5"),
                granularity=TimeGranularity.YEAR,
            )
        )
        assert result.sql == "DATE_ADD(CAST('2020-01-01' AS DATETIME), INTERVAL 5 year)"

    # -- Percentile --

    def test_approximate_continuous_percentile(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlPercentileExpression.create(
                order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
                ),
            )
        )
        assert result.sql == "PERCENTILE_APPROX(a.col0, 0.5)"

    def test_approximate_continuous_percentile_p95(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlPercentileExpression.create(
                order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "val")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.95, function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
                ),
            )
        )
        assert result.sql == "PERCENTILE_APPROX(a.val, 0.95)"

    def test_continuous_percentile_raises(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        with pytest.raises(RuntimeError, match="Only approximate continuous percentile"):
            doris_renderer.render_sql_expr(
                SqlPercentileExpression.create(
                    order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
                    percentile_args=SqlPercentileExpressionArgument(
                        percentile=0.5, function_type=SqlPercentileFunctionType.CONTINUOUS
                    ),
                )
            )

    def test_discrete_percentile_raises(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        with pytest.raises(RuntimeError, match="Only approximate continuous percentile"):
            doris_renderer.render_sql_expr(
                SqlPercentileExpression.create(
                    order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
                    percentile_args=SqlPercentileExpressionArgument(
                        percentile=0.5, function_type=SqlPercentileFunctionType.DISCRETE
                    ),
                )
            )

    def test_approximate_discrete_percentile_raises(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        with pytest.raises(RuntimeError, match="Only approximate continuous percentile"):
            doris_renderer.render_sql_expr(
                SqlPercentileExpression.create(
                    order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
                    percentile_args=SqlPercentileExpressionArgument(
                        percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_DISCRETE
                    ),
                )
            )

    # -- Ratio Computation (uses double_data_type) --

    def test_ratio_computation(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlRatioComputationExpression.create(
                numerator=SqlAggregateFunctionExpression.create(
                    SqlFunction.SUM,
                    sql_function_args=[SqlStringExpression.create(sql_expr="1", requires_parenthesis=False)],
                ),
                denominator=SqlColumnReferenceExpression.create(
                    SqlColumnReference(column_name="divide_by_me", table_alias="a")
                ),
            ),
        )
        assert result.sql == "CAST(SUM(1) AS DOUBLE) / CAST(NULLIF(a.divide_by_me, 0) AS DOUBLE)"

    # -- Generate UUID --

    def test_generate_uuid(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(SqlGenerateUuidExpression.create())
        assert result.sql == "UUID()"

    # -- Case sensitivity (Doris is case-sensitive for table aliases) --

    def test_column_reference_lowercase_alias(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        """Uppercase table aliases should be lowercased for Doris."""
        result = doris_renderer.render_sql_expr(
            SqlColumnReferenceExpression.create(SqlColumnReference("C", "ds"))
        )
        assert result.sql == "c.ds"

    def test_column_reference_already_lowercase(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "my_col"))
        )
        assert result.sql == "my_table.my_col"

    def test_string_expr_lowercase_alias(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        """String expressions like 'V.ds' should have the alias lowercased."""
        result = doris_renderer.render_sql_expr(SqlStringExpression.create("V.ds"))
        assert result.sql == "v.ds"

    def test_string_expr_no_alias_unchanged(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        """String expressions without alias.column pattern should be unchanged."""
        result = doris_renderer.render_sql_expr(SqlStringExpression.create("a + b"))
        assert result.sql == "a + b"

    def test_string_expr_complex_unchanged(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        """Complex expressions with dots but also operators should be unchanged."""
        result = doris_renderer.render_sql_expr(SqlStringExpression.create("SUM(a.col)"))
        assert result.sql == "SUM(a.col)"

    def test_comparison_expr(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(
            SqlComparisonExpression.create(
                left_expr=SqlColumnReferenceExpression.create(SqlColumnReference("t", "col")),
                comparison=SqlComparison.EQUALS,
                right_expr=SqlStringExpression.create("42"),
            )
        )
        assert result.sql == "t.col = (42)"

    def test_string_literal(self, doris_renderer: DorisSqlExpressionRenderer) -> None:
        result = doris_renderer.render_sql_expr(SqlStringLiteralExpression.create("hello"))
        assert result.sql == "'hello'"


class TestDorisPlanRenderer:
    """Tests for the DorisSqlPlanRenderer."""

    def test_expr_renderer_type(self, doris_plan_renderer: DorisSqlPlanRenderer) -> None:
        assert isinstance(doris_plan_renderer.expr_renderer, DorisSqlExpressionRenderer)

    def test_render_simple_select(self, doris_plan_renderer: DorisSqlPlanRenderer) -> None:
        """Test rendering a simple SELECT statement."""
        from metricflow_semantics.dag.mf_dag import DagId
        from metricflow.sql.sql_plan import SqlPlan

        select_columns = (
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(SqlColumnReference("a", "id")),
                column_alias="id",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(SqlColumnReference("a", "value")),
                column_alias="value",
            ),
        )

        from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="test_db", table_name="test_table"))
        node = SqlSelectStatementNode.create(
            description="Simple select",
            select_columns=select_columns,
            from_source=from_source,
            from_source_alias="a",
        )

        plan = SqlPlan(render_node=node, plan_id=DagId.from_str("plan0"))
        result = doris_plan_renderer.render_sql_plan(plan)
        sql = result.sql

        assert "SELECT" in sql
        assert "a.id" in sql
        assert "a.value" in sql
        assert "test_db.test_table" in sql

    def test_render_select_with_cast_to_timestamp(self, doris_plan_renderer: DorisSqlPlanRenderer) -> None:
        """Test that CAST uses DATETIME in rendered SQL."""
        from metricflow_semantics.dag.mf_dag import DagId
        from metricflow.sql.sql_plan import SqlPlan

        select_columns = (
            SqlSelectColumn(
                expr=SqlCastToTimestampExpression.create(
                    arg=SqlStringLiteralExpression.create("2020-01-01")
                ),
                column_alias="ts",
            ),
        )

        from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
        node = SqlSelectStatementNode.create(
            description="Test cast",
            select_columns=select_columns,
            from_source=from_source,
            from_source_alias="a",
        )

        plan = SqlPlan(render_node=node, plan_id=DagId.from_str("plan0"))
        result = doris_plan_renderer.render_sql_plan(plan)

        assert "CAST('2020-01-01' AS DATETIME)" in result.sql

    def test_render_select_with_date_trunc(self, doris_plan_renderer: DorisSqlPlanRenderer) -> None:
        """Test DATE_TRUNC rendering in a full query."""
        from metricflow_semantics.dag.mf_dag import DagId
        from metricflow.sql.sql_plan import SqlPlan

        select_columns = (
            SqlSelectColumn(
                expr=SqlDateTruncExpression.create(
                    time_granularity=TimeGranularity.MONTH,
                    arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "ds")),
                ),
                column_alias="ds_month",
            ),
        )

        from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
        node = SqlSelectStatementNode.create(
            description="Test date_trunc",
            select_columns=select_columns,
            from_source=from_source,
            from_source_alias="a",
        )

        plan = SqlPlan(render_node=node, plan_id=DagId.from_str("plan0"))
        result = doris_plan_renderer.render_sql_plan(plan)

        assert "DATE_TRUNC('month', a.ds)" in result.sql

    def test_render_select_with_percentile(self, doris_plan_renderer: DorisSqlPlanRenderer) -> None:
        """Test PERCENTILE_APPROX rendering in a full query."""
        from metricflow_semantics.dag.mf_dag import DagId
        from metricflow.sql.sql_plan import SqlPlan

        select_columns = (
            SqlSelectColumn(
                expr=SqlPercentileExpression.create(
                    order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "value")),
                    percentile_args=SqlPercentileExpressionArgument(
                        percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
                    ),
                ),
                column_alias="p50",
            ),
        )

        from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
        node = SqlSelectStatementNode.create(
            description="Test percentile",
            select_columns=select_columns,
            from_source=from_source,
            from_source_alias="a",
        )

        plan = SqlPlan(render_node=node, plan_id=DagId.from_str("plan0"))
        result = doris_plan_renderer.render_sql_plan(plan)

        assert "PERCENTILE_APPROX(a.value, 0.5)" in result.sql
