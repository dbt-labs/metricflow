from __future__ import annotations

import logging
import textwrap
from abc import ABC, abstractmethod
from collections import namedtuple
from dataclasses import dataclass
from typing import Collection, List

import jinja2
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_exprs import (
    SqlAggregateFunctionExpression,
    SqlBetweenExpression,
    SqlCastToTimestampExpression,
    SqlColumnAliasReferenceExpression,
    SqlColumnReferenceExpression,
    SqlComparisonExpression,
    SqlDateTruncExpression,
    SqlExpressionNode,
    SqlExpressionNodeVisitor,
    SqlExtractExpression,
    SqlFunction,
    SqlGenerateUuidExpression,
    SqlIsNullExpression,
    SqlLogicalExpression,
    SqlNullExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlRatioComputationExpression,
    SqlStringExpression,
    SqlStringLiteralExpression,
    SqlSubtractTimeIntervalExpression,
    SqlWindowFunctionExpression,
)
from metricflow.sql.sql_plan import SqlSelectColumn
from metricflow.time.date_part import DatePart

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlExpressionRenderResult:
    """The result of rendering an SQL expression tree to a string."""

    sql: str
    bind_parameters: SqlBindParameters


class SqlExpressionRenderer(SqlExpressionNodeVisitor[SqlExpressionRenderResult], ABC):
    """Renders SqlExpressions into strings."""

    def render_sql_expr(self, sql_expr: SqlExpressionNode) -> SqlExpressionRenderResult:
        """Render the given expression to a string."""
        return sql_expr.accept(self)

    def render_group_by_expr(self, group_by_column: SqlSelectColumn) -> SqlExpressionRenderResult:
        """Render the input group by column to a string.

        This allows for engine-level overrides of the group by rendering behavior, since some engines only support
        rendering group by columns based on aliases.
        """
        return self.render_sql_expr(sql_expr=group_by_column.expr)

    @property
    @abstractmethod
    def double_data_type(self) -> str:
        """Property for the double data type, for engine-specific type casting."""
        raise NotImplementedError

    @property
    @abstractmethod
    def timestamp_data_type(self) -> str:
        """Property for the timestamp data type, for engine-specific type casting."""
        raise NotImplementedError

    @property
    @abstractmethod
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        """Returns the sequence of supported percentile function types for the implementing renderer."""
        raise NotImplementedError

    def can_render_percentile_function(self, percentile_type: SqlPercentileFunctionType) -> bool:
        """Whether or not this SQL renderer supports rendering the particular percentile function type."""
        return percentile_type in self.supported_percentile_function_types


class DefaultSqlExpressionRenderer(SqlExpressionRenderer):
    """Renders the SQL query plan assuming ANSI SQL."""

    @property
    @override
    def double_data_type(self) -> str:
        return "DOUBLE"

    @property
    @override
    def timestamp_data_type(self) -> str:
        return "TIMESTAMP"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {}

    def visit_string_expr(self, node: SqlStringExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Renders an arbitrary string expression like 1+1=2."""
        return SqlExpressionRenderResult(sql=node.sql_expr, bind_parameters=node.bind_parameters)

    def visit_column_reference_expr(self, node: SqlColumnReferenceExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Render a reference to a column in a table like my_table.my_col."""
        return SqlExpressionRenderResult(
            sql=(
                f"{node.col_ref.table_alias}.{node.col_ref.column_name}"
                if node.should_render_table_alias
                else node.col_ref.column_name
            ),
            bind_parameters=SqlBindParameters(),
        )

    def visit_column_alias_reference_expr(  # noqa: D
        self, node: SqlColumnAliasReferenceExpression
    ) -> SqlExpressionRenderResult:
        """Render a reference to a column without a known table alias. e.g. foo.bar vs bar."""
        return SqlExpressionRenderResult(
            sql=node.column_alias,
            bind_parameters=SqlBindParameters(),
        )

    def visit_comparison_expr(self, node: SqlComparisonExpression) -> SqlExpressionRenderResult:
        """Render a comparison expression like 1 = 2."""
        combined_params = SqlBindParameters()

        left_expr_rendered = self.render_sql_expr(node.left_expr)
        combined_params = combined_params.combine(left_expr_rendered.bind_parameters)

        right_expr_rendered = self.render_sql_expr(node.right_expr)
        combined_params = combined_params.combine(right_expr_rendered.bind_parameters)

        # To avoid issues with operator precedence, use parenthesis to group the left / right expressions if they
        # contain operators.
        return SqlExpressionRenderResult(
            # Render a + b = c
            sql=(
                (f"({left_expr_rendered.sql})" if node.left_expr.requires_parenthesis else left_expr_rendered.sql)
                + f" {node.comparison.value} "
                + (f"({right_expr_rendered.sql})" if node.right_expr.requires_parenthesis else right_expr_rendered.sql)
            ),
            bind_parameters=combined_params,
        )

    def visit_function_expr(self, node: SqlAggregateFunctionExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Render a function call like CONCAT(a, b)."""
        args_rendered = [self.render_sql_expr(x) for x in node.sql_function_args]
        combined_params = SqlBindParameters()
        for arg_rendered in args_rendered:
            combined_params = combined_params.combine(arg_rendered.bind_parameters)

        distinct_prefix = "DISTINCT " if SqlFunction.is_distinct_aggregation(node.sql_function) else ""
        args_string = ", ".join([x.sql for x in args_rendered])

        return SqlExpressionRenderResult(
            sql=f"{node.sql_function.value}({distinct_prefix}{args_string})",
            bind_parameters=combined_params,
        )

    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression."""
        raise RuntimeError(
            "Default expression render has no percentile implementation - an engine-specific renderer should be implemented."
        )

    def visit_null_expr(self, node: SqlNullExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql="NULL",
            bind_parameters=SqlBindParameters(),
        )

    def visit_string_literal_expr(self, node: SqlStringLiteralExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql=f"'{node.literal_value}'",
            bind_parameters=SqlBindParameters(),
        )

    def visit_logical_expr(self, node: SqlLogicalExpression) -> SqlExpressionRenderResult:  # noqa: D
        RenderedExpr = namedtuple("RenderedExpr", ["expr", "requires_parenthesis"])
        args_rendered = [RenderedExpr(self.render_sql_expr(x), x.requires_parenthesis) for x in node.args]
        combined_parameters = SqlBindParameters()
        args_sql: List[str] = []

        can_be_rendered_in_one_line = sum(len(x.expr.sql) for x in args_rendered) < 60

        for arg_rendered in args_rendered:
            combined_parameters.combine(arg_rendered.expr.bind_parameters)
            arg_sql = self._render_logical_arg(
                arg_rendered.expr, arg_rendered.requires_parenthesis, render_in_one_line=can_be_rendered_in_one_line
            )
            args_sql.append(arg_sql)

        sql = f" {node.operator.value} ".join(args_sql)

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameters=combined_parameters,
        )

    @staticmethod
    def _render_logical_arg(
        arg_rendered: SqlExpressionRenderResult, requires_parenthesis: bool, render_in_one_line: bool
    ) -> str:
        # Put everything on 1 line for short expressions, but otherwise put one expression per line
        # Note: multi-line expressions are always enclosed in parentheses.

        # e.g.
        # Put everything on 1 line for short expressions like:
        #
        # (1 < 2) AND foo
        #
        # but for long expressions do:
        #
        #  (
        #    some_long_expression1
        #  ) AND (
        #    some_long_expression2
        #  ) AND (
        #    some_long_expression3
        # )

        if render_in_one_line:
            return arg_rendered.sql if not requires_parenthesis else f"({arg_rendered.sql})"
        else:
            return (
                jinja2.Template(
                    textwrap.dedent(
                        """\
                    (
                      {{ arg_sql | indent(2) }}
                    )
                    """
                    )
                )
                .render(arg_sql=arg_rendered.sql)
                .rstrip()
            )

    def visit_is_null_expr(self, node: SqlIsNullExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)

        return SqlExpressionRenderResult(
            sql=f"{arg_rendered.sql} IS NULL" if not node.arg.requires_parenthesis else f"({arg_rendered.sql}) IS NULL",
            bind_parameters=arg_rendered.bind_parameters,
        )

    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS {self.timestamp_data_type})",
            bind_parameters=arg_rendered.bind_parameters,
        )

    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)

        return SqlExpressionRenderResult(
            sql=f"DATE_TRUNC('{node.time_granularity.value}', {arg_rendered.sql})",
            bind_parameters=arg_rendered.bind_parameters,
        )

    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)

        return SqlExpressionRenderResult(
            sql=f"EXTRACT({self.render_date_part(node.date_part)} FROM {arg_rendered.sql})",
            bind_parameters=arg_rendered.bind_parameters,
        )

    def render_date_part(self, date_part: DatePart) -> str:
        """Render DATE PART for an EXTRACT expression."""
        return date_part.value

    def visit_time_delta_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = node.arg.accept(self)

        count = node.count
        granularity = node.granularity
        if granularity == TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3
        return SqlExpressionRenderResult(
            sql=f"DATEADD({granularity.value}, -{count}, {arg_rendered.sql})",
            bind_parameters=arg_rendered.bind_parameters,
        )

    def visit_ratio_computation_expr(self, node: SqlRatioComputationExpression) -> SqlExpressionRenderResult:
        """Render the ratio computation for a ratio metric.

        This requires both a type cast to a floating point type (default to DOUBLE, engine-permitting) and
        the requisite division between numerator and denominator
        """
        rendered_numerator = self.render_sql_expr(node.numerator)
        rendered_denominator = self.render_sql_expr(node.denominator)

        numerator_sql = f"CAST({rendered_numerator.sql} AS {self.double_data_type})"
        denominator_sql = f"CAST(NULLIF({rendered_denominator.sql}, 0) AS {self.double_data_type})"

        bind_parameters = SqlBindParameters()
        bind_parameters = bind_parameters.combine(rendered_numerator.bind_parameters)
        bind_parameters = bind_parameters.combine(rendered_denominator.bind_parameters)

        return SqlExpressionRenderResult(
            sql=f"{numerator_sql} / {denominator_sql}",
            bind_parameters=bind_parameters,
        )

    def visit_between_expr(self, node: SqlBetweenExpression) -> SqlExpressionRenderResult:  # noqa: D
        rendered_column_arg = self.render_sql_expr(node.column_arg)
        rendered_start_expr = self.render_sql_expr(node.start_expr)
        rendered_end_expr = self.render_sql_expr(node.end_expr)

        bind_parameters = SqlBindParameters()
        bind_parameters = bind_parameters.combine(rendered_column_arg.bind_parameters)
        bind_parameters = bind_parameters.combine(rendered_start_expr.bind_parameters)
        bind_parameters = bind_parameters.combine(rendered_end_expr.bind_parameters)

        return SqlExpressionRenderResult(
            sql=f"{rendered_column_arg.sql} BETWEEN {rendered_start_expr.sql} AND {rendered_end_expr.sql}",
            bind_parameters=bind_parameters,
        )

    def visit_window_function_expr(self, node: SqlWindowFunctionExpression) -> SqlExpressionRenderResult:  # noqa: D
        sql_function_args_rendered = [self.render_sql_expr(x) for x in node.sql_function_args]
        partition_by_args_rendered = [self.render_sql_expr(x) for x in node.partition_by_args]
        order_by_args_rendered = {self.render_sql_expr(x.expr): x for x in node.order_by_args}

        combined_params = SqlBindParameters()
        args_rendered = []
        if sql_function_args_rendered:
            args_rendered.extend(sql_function_args_rendered)
        if partition_by_args_rendered:
            args_rendered.extend(partition_by_args_rendered)
        if order_by_args_rendered:
            args_rendered.extend(list(order_by_args_rendered.keys()))
        for arg_rendered in args_rendered:
            combined_params = combined_params.combine(arg_rendered.bind_parameters)

        sql_function_args_string = ", ".join([x.sql for x in sql_function_args_rendered])
        partition_by_args_string = (
            ("PARTITION BY " + ", ".join([x.sql for x in partition_by_args_rendered]))
            if partition_by_args_rendered
            else ""
        )
        order_by_args_string = (
            (
                "ORDER BY "
                + ", ".join(
                    [
                        rendered_result.sql + (f" {x.suffix}" if x.suffix else "")
                        for rendered_result, x in order_by_args_rendered.items()
                    ]
                )
            )
            if order_by_args_rendered
            else ""
        )

        window_string = " ".join(filter(bool, [partition_by_args_string, order_by_args_string]))
        return SqlExpressionRenderResult(
            sql=f"{node.sql_function.value}({sql_function_args_string}) OVER ({window_string})",
            bind_parameters=combined_params,
        )

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql="UUID()",
            bind_parameters=SqlBindParameters(),
        )
