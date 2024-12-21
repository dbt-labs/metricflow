from __future__ import annotations

import logging
import textwrap
from abc import ABC, abstractmethod
from collections import namedtuple
from dataclasses import dataclass
from typing import TYPE_CHECKING, Collection, List, Optional

import jinja2
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlAggregateFunctionExpression,
    SqlArithmeticExpression,
    SqlArithmeticOperator,
    SqlBetweenExpression,
    SqlCaseExpression,
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
    SqlIntegerExpression,
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
from typing_extensions import override

from metricflow.sql.render.rendering_constants import SqlRenderingConstants
from metricflow.sql.sql_plan import SqlSelectColumn

if TYPE_CHECKING:
    from metricflow.protocols.sql_client import SqlEngine


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlExpressionRenderResult:
    """The result of rendering an SQL expression tree to a string."""

    sql: str
    bind_parameter_set: SqlBindParameterSet


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
    def sql_engine(self) -> Optional[SqlEngine]:  # noqa: D102
        return None

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

    def visit_string_expr(self, node: SqlStringExpression) -> SqlExpressionRenderResult:
        """Renders an arbitrary string expression like 1+1=2."""
        return SqlExpressionRenderResult(sql=node.sql_expr, bind_parameter_set=node.bind_parameter_set)

    def visit_column_reference_expr(self, node: SqlColumnReferenceExpression) -> SqlExpressionRenderResult:
        """Render a reference to a column in a table like my_table.my_col."""
        return SqlExpressionRenderResult(
            sql=(
                f"{node.col_ref.table_alias}.{node.col_ref.column_name}"
                if node.should_render_table_alias
                else node.col_ref.column_name
            ),
            bind_parameter_set=SqlBindParameterSet(),
        )

    def visit_column_alias_reference_expr(self, node: SqlColumnAliasReferenceExpression) -> SqlExpressionRenderResult:
        """Render a reference to a column without a known table alias. e.g. foo.bar vs bar."""
        return SqlExpressionRenderResult(
            sql=node.column_alias,
            bind_parameter_set=SqlBindParameterSet(),
        )

    def visit_comparison_expr(self, node: SqlComparisonExpression) -> SqlExpressionRenderResult:
        """Render a comparison expression like 1 = 2."""
        combined_params = SqlBindParameterSet()

        left_expr_rendered = self.render_sql_expr(node.left_expr)
        combined_params = combined_params.merge(left_expr_rendered.bind_parameter_set)

        right_expr_rendered = self.render_sql_expr(node.right_expr)
        combined_params = combined_params.merge(right_expr_rendered.bind_parameter_set)

        # To avoid issues with operator precedence, use parenthesis to group the left / right expressions if they
        # contain operators.
        return SqlExpressionRenderResult(
            # Render a + b = c
            sql=(
                (f"({left_expr_rendered.sql})" if node.left_expr.requires_parenthesis else left_expr_rendered.sql)
                + f" {node.comparison.value} "
                + (f"({right_expr_rendered.sql})" if node.right_expr.requires_parenthesis else right_expr_rendered.sql)
            ),
            bind_parameter_set=combined_params,
        )

    def visit_function_expr(self, node: SqlAggregateFunctionExpression) -> SqlExpressionRenderResult:
        """Render a function call like CONCAT(a, b)."""
        args_rendered = [self.render_sql_expr(x) for x in node.sql_function_args]
        combined_params = SqlBindParameterSet()
        for arg_rendered in args_rendered:
            combined_params = combined_params.merge(arg_rendered.bind_parameter_set)

        distinct_prefix = "DISTINCT " if SqlFunction.is_distinct_aggregation(node.sql_function) else ""
        args_string = ", ".join([x.sql for x in args_rendered])

        return SqlExpressionRenderResult(
            sql=f"{node.sql_function.value}({distinct_prefix}{args_string})",
            bind_parameter_set=combined_params,
        )

    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression."""
        raise RuntimeError(
            "Default expression render has no percentile implementation - an engine-specific renderer should be implemented."
        )

    def visit_null_expr(self, node: SqlNullExpression) -> SqlExpressionRenderResult:  # noqa: D102
        return SqlExpressionRenderResult(
            sql="NULL",
            bind_parameter_set=SqlBindParameterSet(),
        )

    def visit_string_literal_expr(self, node: SqlStringLiteralExpression) -> SqlExpressionRenderResult:  # noqa: D102
        return SqlExpressionRenderResult(
            sql=f"'{node.literal_value}'",
            bind_parameter_set=SqlBindParameterSet(),
        )

    def visit_logical_expr(self, node: SqlLogicalExpression) -> SqlExpressionRenderResult:  # noqa: D102
        RenderedExpr = namedtuple("RenderedExpr", ["expr", "requires_parenthesis"])
        args_rendered = [RenderedExpr(self.render_sql_expr(x), x.requires_parenthesis) for x in node.args]
        combined_parameters = SqlBindParameterSet()
        args_sql: List[str] = []

        can_be_rendered_in_one_line = sum(len(x.expr.sql) for x in args_rendered) < 60

        for arg_rendered in args_rendered:
            combined_parameters.merge(arg_rendered.expr.bind_parameter_set)
            arg_sql = self._render_logical_arg(
                arg_rendered.expr, arg_rendered.requires_parenthesis, render_in_one_line=can_be_rendered_in_one_line
            )
            args_sql.append(arg_sql)

        sql = f" {node.operator.value} ".join(args_sql)

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=combined_parameters,
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

    def visit_is_null_expr(self, node: SqlIsNullExpression) -> SqlExpressionRenderResult:  # noqa: D102
        arg_rendered = self.render_sql_expr(node.arg)

        return SqlExpressionRenderResult(
            sql=f"{arg_rendered.sql} IS NULL" if not node.arg.requires_parenthesis else f"({arg_rendered.sql}) IS NULL",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    def visit_cast_to_timestamp_expr(  # noqa: D102
        self, node: SqlCastToTimestampExpression
    ) -> SqlExpressionRenderResult:  # noqa: D102
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS {self.timestamp_data_type})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    def _validate_granularity_for_engine(self, time_granularity: TimeGranularity) -> None:
        if self.sql_engine and time_granularity in self.sql_engine.unsupported_granularities:
            raise RuntimeError(f"{self.sql_engine.name} does not support time granularity {time_granularity.name}.")

    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:  # noqa: D102
        self._validate_granularity_for_engine(node.time_granularity)

        arg_rendered = self.render_sql_expr(node.arg)

        return SqlExpressionRenderResult(
            sql=f"DATE_TRUNC('{node.time_granularity.value}', {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:  # noqa: D102
        arg_rendered = self.render_sql_expr(node.arg)

        return SqlExpressionRenderResult(
            sql=f"EXTRACT({self.render_date_part(node.date_part)} FROM {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    def render_date_part(self, date_part: DatePart) -> str:
        """Render DATE PART for an EXTRACT expression.

        For DatePart.DOW (day of week) we use the ISO date part to ensure all engines return consistent results.
        """
        if date_part is DatePart.DOW:
            return "isodow"

        return date_part.value

    def visit_subtract_time_interval_expr(  # noqa: D102
        self, node: SqlSubtractTimeIntervalExpression
    ) -> SqlExpressionRenderResult:
        arg_rendered = node.arg.accept(self)

        count = node.count
        granularity = node.granularity
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3
        return SqlExpressionRenderResult(
            sql=f"DATEADD({granularity.value}, -{count}, {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:  # noqa: D102
        granularity = node.granularity
        count_expr = node.count_expr
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count_expr = SqlArithmeticExpression.create(
                left_expr=node.count_expr,
                operator=SqlArithmeticOperator.MULTIPLY,
                right_expr=SqlIntegerExpression.create(3),
            )

        arg_rendered = node.arg.accept(self)
        count_rendered = count_expr.accept(self)
        count_sql = f"({count_rendered.sql})" if count_expr.requires_parenthesis else count_rendered.sql

        return SqlExpressionRenderResult(
            sql=f"DATEADD({granularity.value}, {count_sql}, {arg_rendered.sql})",
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
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

        bind_parameter_set = SqlBindParameterSet()
        bind_parameter_set = bind_parameter_set.merge(rendered_numerator.bind_parameter_set)
        bind_parameter_set = bind_parameter_set.merge(rendered_denominator.bind_parameter_set)

        return SqlExpressionRenderResult(
            sql=f"{numerator_sql} / {denominator_sql}",
            bind_parameter_set=bind_parameter_set,
        )

    def visit_between_expr(self, node: SqlBetweenExpression) -> SqlExpressionRenderResult:  # noqa: D102
        rendered_column_arg = self.render_sql_expr(node.column_arg)
        rendered_start_expr = self.render_sql_expr(node.start_expr)
        rendered_end_expr = self.render_sql_expr(node.end_expr)

        bind_parameter_set = SqlBindParameterSet()
        bind_parameter_set = bind_parameter_set.merge(rendered_column_arg.bind_parameter_set)
        bind_parameter_set = bind_parameter_set.merge(rendered_start_expr.bind_parameter_set)
        bind_parameter_set = bind_parameter_set.merge(rendered_end_expr.bind_parameter_set)

        return SqlExpressionRenderResult(
            sql=f"{rendered_column_arg.sql} BETWEEN {rendered_start_expr.sql} AND {rendered_end_expr.sql}",
            bind_parameter_set=bind_parameter_set,
        )

    def visit_window_function_expr(self, node: SqlWindowFunctionExpression) -> SqlExpressionRenderResult:  # noqa: D102
        sql_function_args_rendered = [self.render_sql_expr(x) for x in node.sql_function_args]
        partition_by_args_rendered = [self.render_sql_expr(x) for x in node.partition_by_args]
        order_by_args_rendered = {self.render_sql_expr(x.expr): x for x in node.order_by_args}

        combined_params = SqlBindParameterSet()
        args_rendered = []
        if sql_function_args_rendered:
            args_rendered.extend(sql_function_args_rendered)
        if partition_by_args_rendered:
            args_rendered.extend(partition_by_args_rendered)
        if order_by_args_rendered:
            args_rendered.extend(list(order_by_args_rendered.keys()))
        for arg_rendered in args_rendered:
            combined_params = combined_params.merge(arg_rendered.bind_parameter_set)

        sql_function_args_string = ", ".join([x.sql for x in sql_function_args_rendered])
        window_string_lines: List[str] = []
        if len(partition_by_args_rendered) == 1:
            window_string_lines.append(f"PARTITION BY {partition_by_args_rendered[0].sql}")
        elif len(partition_by_args_rendered) > 1:
            window_string_lines.append("PARTITION BY")
            window_string_lines.append(
                indent(
                    "\n, ".join([x.sql for x in partition_by_args_rendered]),
                    indent_prefix=SqlRenderingConstants.INDENT,
                )
            )
        if len(order_by_args_rendered) == 1:
            rendered_result, order_by_arg = tuple(order_by_args_rendered.items())[0]
            window_string_lines.append(
                "ORDER BY " + rendered_result.sql + (f" {order_by_arg.suffix}" if order_by_arg.suffix else "")
            )
        elif len(order_by_args_rendered) > 1:
            window_string_lines.append("ORDER BY")
            window_string_lines.append(
                indent(
                    "\n, ".join(
                        [
                            rendered_result.sql + (f" {order_by_arg.suffix}" if order_by_arg.suffix else "")
                            for rendered_result, order_by_arg in order_by_args_rendered.items()
                        ]
                    ),
                    indent_prefix=SqlRenderingConstants.INDENT,
                )
            )

        if len(order_by_args_rendered) > 0 and node.sql_function.allows_frame_clause:
            window_string_lines.append("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING")

        window_string = "\n".join(window_string_lines)

        if len(window_string_lines) <= 1:
            return SqlExpressionRenderResult(
                sql=f"{node.sql_function.value}({sql_function_args_string}) OVER ({window_string})",
                bind_parameter_set=combined_params,
            )
        else:
            indented_window_string = indent(window_string, indent_prefix=SqlRenderingConstants.INDENT)
            return SqlExpressionRenderResult(
                sql=f"{node.sql_function.value}({sql_function_args_string}) OVER (\n{indented_window_string}\n)",
                bind_parameter_set=combined_params,
            )

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D102
        return SqlExpressionRenderResult(
            sql="UUID()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    def visit_case_expr(self, node: SqlCaseExpression) -> SqlExpressionRenderResult:  # noqa: D102
        sql = "CASE\n"
        for when, then in node.when_to_then_exprs.items():
            sql += indent(
                f"WHEN {self.render_sql_expr(when).sql}\n", indent_prefix=SqlRenderingConstants.INDENT
            ) + indent(
                f"THEN {self.render_sql_expr(then).sql}\n",
                indent_prefix=SqlRenderingConstants.INDENT * 2,
            )
        if node.else_expr:
            sql += indent(
                f"ELSE {self.render_sql_expr(node.else_expr).sql}\n",
                indent_prefix=SqlRenderingConstants.INDENT,
            )
        sql += "END"
        return SqlExpressionRenderResult(sql=sql, bind_parameter_set=SqlBindParameterSet())

    def visit_arithmetic_expr(self, node: SqlArithmeticExpression) -> SqlExpressionRenderResult:  # noqa: D102
        sql = f"{self.render_sql_expr(node.left_expr).sql} {node.operator.value} {self.render_sql_expr(node.right_expr).sql}"
        return SqlExpressionRenderResult(sql=sql, bind_parameter_set=SqlBindParameterSet())

    def visit_integer_expr(self, node: SqlIntegerExpression) -> SqlExpressionRenderResult:  # noqa: D102
        return SqlExpressionRenderResult(sql=str(node.integer_value), bind_parameter_set=SqlBindParameterSet())
