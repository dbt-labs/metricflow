from __future__ import annotations

import logging
import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from string import Template
from typing import List, Optional, Sequence

from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import SqlExpressionNode
from typing_extensions import override

from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.rendering_constants import SqlRenderingConstants
from metricflow.sql.sql_plan import (
    SqlCreateTableAsNode,
    SqlCteNode,
    SqlJoinDescription,
    SqlOrderByDescription,
    SqlPlan,
    SqlQueryPlanNode,
    SqlQueryPlanNodeVisitor,
    SqlSelectColumn,
    SqlSelectQueryFromClauseNode,
    SqlSelectStatementNode,
    SqlTableNode,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlPlanRenderResult:  # noqa: D101
    # The SQL string that could be run.
    sql: str
    # The execution parameters that should be specified along with the SQL str to execute()
    bind_parameter_set: SqlBindParameterSet


class SqlQueryPlanRenderer(SqlQueryPlanNodeVisitor[SqlPlanRenderResult], ABC):
    """Renders SQL plans to a string."""

    def _render_node(self, node: SqlQueryPlanNode) -> SqlPlanRenderResult:
        return node.accept(self)

    def render_sql_query_plan(self, sql_query_plan: SqlPlan) -> SqlPlanRenderResult:  # noqa: D102
        return self._render_node(sql_query_plan.render_node)

    @property
    @abstractmethod
    def expr_renderer(self) -> SqlExpressionRenderer:
        """Return the renderer that this uses to render expressions."""
        pass


@dataclass
class StringJoinDescription:
    """Helper class to store the join clause as a string. Useful to avoid logic in the Jinja template."""

    join_source_str: str
    join_source_is_a_table: bool
    join_source_alias: str
    on_condition_str: str


class DefaultSqlQueryPlanRenderer(SqlQueryPlanRenderer):
    """Renders an SQL plan following ANSI SQL."""

    # The renderer that is used to render the SQL expressions.
    EXPR_RENDERER = DefaultSqlExpressionRenderer()

    def _render_description_section(self, description: str) -> Optional[SqlPlanRenderResult]:
        """Render the description section as a comment.

        e.g.
        -- Description of the node.

        """
        if len(description) == 0:
            return None
        description_lines = [f"-- {x}" for x in description.split("\n") if x]
        return SqlPlanRenderResult("\n".join(description_lines), SqlBindParameterSet())

    @override
    def visit_cte_node(self, node: SqlCteNode) -> SqlPlanRenderResult:
        lines = []
        collected_bind_parameters = []
        lines.append(f"{node.cte_alias} AS (")
        select_statement_render_result = node.select_statement.accept(self)
        lines.append(indent(select_statement_render_result.sql, indent_prefix=SqlRenderingConstants.INDENT))
        collected_bind_parameters.append(select_statement_render_result.bind_parameter_set)
        lines.append(")")

        return SqlPlanRenderResult(
            sql="\n".join(lines), bind_parameter_set=SqlBindParameterSet.merge_iterable(collected_bind_parameters)
        )

    def _render_cte_sections(self, cte_nodes: Sequence[SqlCteNode]) -> Optional[SqlPlanRenderResult]:
        """Convert the CTEs into a series of `WITH` clauses.

        e.g.
            WITH cte_alias_0 AS (
                ...
            )
            cte_alias_1 AS (
                ...
            )
            ...
        """
        if len(cte_nodes) == 0:
            return None

        cte_render_results = tuple(self.visit_cte_node(cte_node) for cte_node in cte_nodes)

        return SqlPlanRenderResult(
            sql="WITH " + "\n, ".join(cte_render_result.sql + "\n" for cte_render_result in cte_render_results),
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                [cte_render_result.bind_parameter_set for cte_render_result in cte_render_results]
            ),
        )

    def _render_select_columns_section(
        self,
        select_columns: Sequence[SqlSelectColumn],
        num_parents: int,
        distinct: bool,
    ) -> SqlPlanRenderResult:
        """Convert the select columns into a "SELECT" section.

        e.g.
        SELECT
          1 AS bookings
          , listing_id AS listing
          ...

        Returns a tuple of the "SELECT" section as a string and the associated execution parameters.
        """
        params = SqlBindParameterSet()
        select_section_lines = ["SELECT DISTINCT" if distinct else "SELECT"]
        first_column = True
        for select_column in select_columns:
            expr_rendered = self.EXPR_RENDERER.render_sql_expr(select_column.expr)
            # Merge all execution parameters together. Similar pattern follows below.
            params = params.merge(expr_rendered.bind_parameter_set)

            column_select_str = f"{expr_rendered.sql} AS {select_column.column_alias}"

            # For cases where the alias is the same as column like "src.foo AS foo", just render it as "src.foo"
            # SQLite throws an ambiguous column error with something like
            #
            # SELECT a.ds
            # FROM fct_bookings a
            # JOIN dim_users b
            # on a.ds = b.ds
            #
            # so don't do this if it has joins.
            if num_parents <= 1 and select_column.expr.as_column_reference_expression:
                column_reference = select_column.expr.as_column_reference_expression.col_ref
                if column_reference.column_name == select_column.column_alias:
                    column_select_str = expr_rendered.sql

            if first_column:
                first_column = False
                select_section_lines.append(indent(column_select_str, indent_prefix=SqlRenderingConstants.INDENT))
            else:
                select_section_lines.append(
                    indent(", " + column_select_str, indent_prefix=SqlRenderingConstants.INDENT)
                )

        return SqlPlanRenderResult("\n".join(select_section_lines), params)

    def _render_from_section(self, from_source: SqlQueryPlanNode, from_source_alias: str) -> SqlPlanRenderResult:
        """Convert the node into a "FROM" section.

        e.g.
        FROM (
          SELECT
            1 AS bookings
            ...
        )

        Returns a tuple of the "FROM" section as a string and the associated execution parameters.
        """
        from_render_result = self._render_node(from_source)

        from_section_lines = []
        if from_source.as_sql_table_node is not None:
            from_section_lines.append(f"FROM {from_render_result.sql} {from_source_alias}")
        else:
            from_section_lines.append("FROM (")
            from_section_lines.append(indent(from_render_result.sql, indent_prefix=SqlRenderingConstants.INDENT))
            from_section_lines.append(f") {from_source_alias}")
        from_section = "\n".join(from_section_lines)

        return SqlPlanRenderResult(from_section, from_render_result.bind_parameter_set)

    def _render_joins_section(self, join_descriptions: Sequence[SqlJoinDescription]) -> Optional[SqlPlanRenderResult]:
        """Convert the join descriptions into a "JOIN" section.

        e.g.
        JOIN (
          SELECT...
        ) right
        ON
          left.ds = right.ds

        Returns a tuple of the "JOIN" section as a string and the associated execution parameters.
        """
        if len(join_descriptions) == 0:
            return None

        params = SqlBindParameterSet()
        join_section_lines = []
        for join_description in join_descriptions:
            # Render the source for the join
            right_source_rendered = self._render_node(join_description.right_source)
            params = params.merge(right_source_rendered.bind_parameter_set)

            # Render the on condition for the join
            on_condition_rendered: Optional[SqlExpressionRenderResult] = None
            if join_description.on_condition:
                on_condition_rendered = self.EXPR_RENDERER.render_sql_expr(join_description.on_condition)
                params = params.merge(on_condition_rendered.bind_parameter_set)

            if join_description.right_source.as_sql_table_node is not None:
                join_section_lines.append(join_description.join_type.value)
                join_section_lines.append(
                    textwrap.indent(
                        f"{right_source_rendered.sql} {join_description.right_source_alias}",
                        prefix=SqlRenderingConstants.INDENT,
                    )
                )
            else:
                join_section_lines.append(f"{join_description.join_type.value} (")
                join_section_lines.append(
                    textwrap.indent(right_source_rendered.sql, prefix=SqlRenderingConstants.INDENT)
                )
                join_section_lines.append(f") {join_description.right_source_alias}")

            if on_condition_rendered:
                join_section_lines.append("ON")
                join_section_lines.append(
                    textwrap.indent(on_condition_rendered.sql, prefix=SqlRenderingConstants.INDENT)
                )

        return SqlPlanRenderResult("\n".join(join_section_lines), params)

    def _render_where(self, where_expression: Optional[SqlExpressionNode]) -> Optional[SqlPlanRenderResult]:
        if where_expression is None:
            return None

        where_expression_render_result = self.EXPR_RENDERER.render_sql_expr(where_expression)
        where_section = f"WHERE {where_expression_render_result.sql}"
        return SqlPlanRenderResult(where_section, where_expression_render_result.bind_parameter_set)

    def _render_group_by_section(self, group_by_columns: Sequence[SqlSelectColumn]) -> Optional[SqlPlanRenderResult]:
        """Convert the group by columns into a "GROUP BY" section.

        e.g.
        GROUP BY
          a.ds

        Returns a tuple of the "GROUP BY" section as a string and the associated execution parameters.
        """
        if len(group_by_columns) == 0:
            return None

        group_by_section_lines: List[str] = []
        params = SqlBindParameterSet()
        first = True
        for group_by_column in group_by_columns:
            group_by_expr_rendered = self.EXPR_RENDERER.render_group_by_expr(group_by_column)
            params = params.merge(group_by_expr_rendered.bind_parameter_set)
            if first:
                first = False
                group_by_section_lines.append("GROUP BY")
                group_by_section_lines.append(
                    textwrap.indent(group_by_expr_rendered.sql, prefix=SqlRenderingConstants.INDENT)
                )
            else:
                group_by_section_lines.append(
                    textwrap.indent(f", {group_by_expr_rendered.sql}", prefix=SqlRenderingConstants.INDENT)
                )

        return SqlPlanRenderResult("\n".join(group_by_section_lines), params)

    def _render_order_by_section(self, order_bys: Sequence[SqlOrderByDescription]) -> Optional[SqlPlanRenderResult]:
        """Convert the group by columns into a "GROUP BY" section.

        e.g.
        ORDER BY
          a.ds DESC
        """
        if len(order_bys) == 0:
            return None

        order_by_items: List[str] = []
        bind_parameters = []

        for order_by in order_bys:
            expression_render_result = self.EXPR_RENDERER.render_sql_expr(order_by.expr)
            order_by_items.append(expression_render_result.sql + (" DESC" if order_by.desc else ""))
            bind_parameters.append(expression_render_result.bind_parameter_set)

        return SqlPlanRenderResult(
            "ORDER BY " + ", ".join(order_by_items), SqlBindParameterSet.merge_iterable(bind_parameters)
        )

    def _render_limit_section(self, limit_value: Optional[int]) -> Optional[SqlPlanRenderResult]:
        """Convert the limit value into a LIMIT section.

        e.g.
        LIMIT 1
        """
        if limit_value is None:
            return None
        return SqlPlanRenderResult(sql=f"LIMIT {limit_value}", bind_parameter_set=SqlBindParameterSet())

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> SqlPlanRenderResult:  # noqa: D102
        render_results = [
            self._render_description_section(node.description),
            self._render_cte_sections(node.cte_sources),
            self._render_select_columns_section(node.select_columns, len(node.parent_nodes), node.distinct),
            self._render_from_section(node.from_source, node.from_source_alias),
            self._render_joins_section(node.join_descs),
            self._render_where(node.where),
            self._render_group_by_section(node.group_bys),
            self._render_order_by_section(node.order_bys),
            self._render_limit_section(node.limit),
        ]

        valid_render_results = tuple(render_result for render_result in render_results if render_result is not None)
        return SqlPlanRenderResult(
            sql="\n".join(render_result.sql for render_result in valid_render_results),
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                [render_result.bind_parameter_set for render_result in valid_render_results]
            ),
        )

    def visit_table_node(self, node: SqlTableNode) -> SqlPlanRenderResult:  # noqa: D102
        return SqlPlanRenderResult(
            sql=node.sql_table.sql,
            bind_parameter_set=SqlBindParameterSet(),
        )

    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> SqlPlanRenderResult:  # noqa: D102
        return SqlPlanRenderResult(
            sql=node.select_query.rstrip(),
            bind_parameter_set=SqlBindParameterSet(),
        )

    def visit_create_table_as_node(self, node: SqlCreateTableAsNode) -> SqlPlanRenderResult:  # noqa: D102
        inner_sql_render_result = node.parent_node.accept(self)
        inner_sql = inner_sql_render_result.sql
        # Using a substitution since inner_sql can have multiple lines, and then dedent() wouldn't dent due to the
        # short line.
        sql = Template(
            textwrap.dedent(
                f"""\
                CREATE {node.sql_table.table_type.value.upper()} {node.sql_table.sql} AS (
                $inner_sql
                )
                """
            ).rstrip()
        ).substitute({"inner_sql": indent(inner_sql, indent_prefix=SqlRenderingConstants.INDENT)})

        return SqlPlanRenderResult(
            sql=sql,
            bind_parameter_set=inner_sql_render_result.bind_parameter_set,
        )

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
