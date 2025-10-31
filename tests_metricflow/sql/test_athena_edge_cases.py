from __future__ import annotations

import pytest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlArithmeticExpression,
    SqlArithmeticOperator,
    SqlBetweenExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlIntegerExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringLiteralExpression,
    SqlSubtractTimeIntervalExpression,
)
from metricflow.sql.render.athena import AthenaSqlExpressionRenderer


@pytest.fixture
def athena_renderer() -> AthenaSqlExpressionRenderer:
    """Fixture for Athena expression renderer."""
    return AthenaSqlExpressionRenderer()


class TestAthenaEdgeCases:
    """Test edge cases and error conditions for Athena SQL rendering."""

    def test_add_time_complex_count_expression(self, athena_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test time addition with complex count expressions."""
        base_expr = SqlColumnReferenceExpression.create("base_date")
        # Create a complex count expression that requires parentheses
        count_expr = SqlArithmeticExpression.create(
            left_expr=SqlIntegerExpression.create(5),
            operator=SqlArithmeticOperator.MULTIPLY,
            right_expr=SqlIntegerExpression.create(2)
        )

        add_expr = SqlAddTimeExpression.create(
            arg=base_expr,
            count_expr=count_expr,
            granularity=TimeGranularity.MONTH
        )

        result = athena_renderer.visit_add_time_expr(add_expr)

        # Should handle complex expressions properly
        assert "DATE_ADD('month'," in result.sql
        assert "base_date" in result.sql
        # Should include the complex count expression
        assert "5 * 2" in result.sql or "(5 * 2)" in result.sql

    def test_add_time_quarter_with_complex_count(self, athena_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test quarter addition with complex count expression gets multiplied by 3."""
        base_expr = SqlColumnReferenceExpression.create("date_col")
        count_expr = SqlArithmeticExpression.create(
            left_expr=SqlIntegerExpression.create(2),
            operator=SqlArithmeticOperator.ADD,
            right_expr=SqlIntegerExpression.create(1)
        )

        add_expr = SqlAddTimeExpression.create(
            arg=base_expr,
            count_expr=count_expr,
            granularity=TimeGranularity.QUARTER
        )

        result = athena_renderer.visit_add_time_expr(add_expr)

        # Should convert to months and multiply count by 3
        assert "DATE_ADD('month'," in result.sql
        assert "3" in result.sql  # Should contain the multiplication factor

    def test_percentile_edge_cases(self, athena_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test percentile expressions with edge case values."""
        column_expr = SqlColumnReferenceExpression.create("value_col")

        # Test with 0th percentile
        percentile_0 = SqlPercentileExpression.create(
            order_by_arg=column_expr,
            percentile_args=SqlPercentileExpressionArgument(
                percentile=0.0,
                function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
            )
        )

        result_0 = athena_renderer.visit_percentile_expr(percentile_0)
        assert result_0.sql == "approx_percentile(value_col, 0.0)"

        # Test with 100th percentile
        percentile_100 = SqlPercentileExpression.create(
            order_by_arg=column_expr,
            percentile_args=SqlPercentileExpressionArgument(
                percentile=1.0,
                function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
            )
        )

        result_100 = athena_renderer.visit_percentile_expr(percentile_100)
        assert result_100.sql == "approx_percentile(value_col, 1.0)"

    def test_all_unsupported_percentile_types_raise_errors(self, athena_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test that all unsupported percentile function types raise appropriate errors."""
        column_expr = SqlColumnReferenceExpression.create("test_col")

        unsupported_types = [
            SqlPercentileFunctionType.DISCRETE,
            SqlPercentileFunctionType.CONTINUOUS,
            SqlPercentileFunctionType.APPROXIMATE_DISCRETE,
        ]

        for percentile_type in unsupported_types:
            percentile_expr = SqlPercentileExpression.create(
                order_by_arg=column_expr,
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5,
                    function_type=percentile_type
                )
            )

            with pytest.raises(RuntimeError) as exc_info:
                athena_renderer.visit_percentile_expr(percentile_expr)

            assert "not supported for Athena" in str(exc_info.value)
            assert "use_approximate_percentile" in str(exc_info.value)

    def test_between_expr_with_non_timestamp_strings(self, athena_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test BETWEEN expression with non-timestamp string literals."""
        column_expr = SqlColumnReferenceExpression.create("text_column")
        start_expr = SqlStringLiteralExpression.create("'apple'")
        end_expr = SqlStringLiteralExpression.create("'zebra'")

        between_expr = SqlBetweenExpression.create(
            column_arg=column_expr,
            start_expr=start_expr,
            end_expr=end_expr
        )

        result = athena_renderer.visit_between_expr(between_expr)

        # Should not try to parse non-timestamp strings as timestamps
        assert result.sql == "text_column BETWEEN 'apple' AND 'zebra'"

    def test_between_expr_with_invalid_timestamp_strings(self, athena_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test BETWEEN expression gracefully handles invalid timestamp strings."""
        column_expr = SqlColumnReferenceExpression.create("some_column")
        start_expr = SqlStringLiteralExpression.create("'not-a-timestamp'")
        end_expr = SqlStringLiteralExpression.create("'also-not-a-timestamp'")

        between_expr = SqlBetweenExpression.create(
            column_arg=column_expr,
            start_expr=start_expr,
            end_expr=end_expr
        )

        # Should not raise an exception
        result = athena_renderer.visit_between_expr(between_expr)

        # Should fall back to regular BETWEEN syntax
        assert "some_column BETWEEN" in result.sql
        assert "'not-a-timestamp'" in result.sql
        assert "'also-not-a-timestamp'" in result.sql

    def test_time_granularity_edge_cases(self, athena_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test various time granularities in time operations."""
        base_expr = SqlColumnReferenceExpression.create("ts")

        # Test various granularities
        granularities = [
            TimeGranularity.SECOND,
            TimeGranularity.MINUTE,
            TimeGranularity.HOUR,
            TimeGranularity.DAY,
            TimeGranularity.WEEK,
            TimeGranularity.MONTH,
            TimeGranularity.YEAR,
        ]

        for granularity in granularities:
            subtract_expr = SqlSubtractTimeIntervalExpression.create(
                arg=base_expr,
                count=1,
                granularity=granularity
            )

            result = athena_renderer.visit_subtract_time_interval_expr(subtract_expr)

            # Should render without error
            assert "DATE_ADD" in result.sql
            assert f"'{granularity.value}'" in result.sql
            assert "ts" in result.sql

    def test_large_time_intervals(self, athena_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test handling of large time interval counts."""
        base_expr = SqlColumnReferenceExpression.create("event_time")

        # Test large positive interval
        large_subtract = SqlSubtractTimeIntervalExpression.create(
            arg=base_expr,
            count=999999,
            granularity=TimeGranularity.DAY
        )

        result = athena_renderer.visit_subtract_time_interval_expr(large_subtract)
        assert "DATE_ADD('day', -999999, event_time)" == result.sql

        # Test large quarter conversion
        large_quarter_subtract = SqlSubtractTimeIntervalExpression.create(
            arg=base_expr,
            count=1000,
            granularity=TimeGranularity.QUARTER
        )

        result = athena_renderer.visit_subtract_time_interval_expr(large_quarter_subtract)
        # 1000 quarters = 3000 months
        assert "DATE_ADD('month', -3000, event_time)" == result.sql

    def test_expression_renderer_bind_parameters(self, athena_renderer: AthenaSqlExpressionRenderer) -> None:
        """Test that bind parameters are properly handled in expressions."""
        # Test that expressions with no bind parameters return empty sets
        uuid_expr = SqlGenerateUuidExpression.create()
        result = athena_renderer.visit_generate_uuid_expr(uuid_expr)
        assert len(result.bind_parameter_set.param_dict) == 0

        # Test complex expression bind parameter propagation
        column_expr = SqlColumnReferenceExpression.create("test_col")
        between_expr = SqlBetweenExpression.create(
            column_arg=column_expr,
            start_expr=SqlStringLiteralExpression.create("'start'"),
            end_expr=SqlStringLiteralExpression.create("'end'")
        )

        result = athena_renderer.visit_between_expr(between_expr)
        # Should merge bind parameters from all sub-expressions
        assert result.bind_parameter_set is not None