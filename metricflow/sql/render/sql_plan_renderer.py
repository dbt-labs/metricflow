from __future__ import annotations

import logging
import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, Sequence

from metricflow.sql.render.expr_renderer import DefaultSqlExpressionRenderer, SqlExpressionRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_plan import (
    SqlQueryPlanNodeVisitor,
    SqlTableFromClauseNode,
    SqlSelectStatementNode,
    SqlQueryPlanNode,
    SqlQueryPlan,
    SqlSelectQueryFromClauseNode,
    SqlSelectColumn,
    SqlJoinDescription,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlPlanRenderResult:  # noqa: D
    # The SQL string that could be run.
    sql: str
    # The execution parameters that should be specified along with the SQL str to execute()
    execution_parameters: SqlBindParameters


class SqlQueryPlanRenderer(SqlQueryPlanNodeVisitor[SqlPlanRenderResult], ABC):
    """Renders SQL plans to a string."""

    def _render_node(self, node: SqlQueryPlanNode) -> SqlPlanRenderResult:  # noqa: D
        return node.accept(self)

    def render_sql_query_plan(self, sql_query_plan: SqlQueryPlan) -> SqlPlanRenderResult:  # noqa: D
        return self._render_node(sql_query_plan.render_node)

    @property
    @abstractmethod
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa: D
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
    """Renders an SQL plan following ANSI SQL"""

    # The renderer that is used to render the SQL expressions.
    EXPR_RENDERER = DefaultSqlExpressionRenderer()
    # The amount to indent when formatting SQL
    INDENT = "  "

    def _render_select_columns_section(
        self,
        select_columns: Sequence[SqlSelectColumn],
        num_parents: int,
    ) -> Tuple[str, SqlBindParameters]:
        """Convert the select columns into a "SELECT" section.

        e.g.
        SELECT
          1 AS bookings
          , listing_id AS listing
          ...

        Returns a tuple of the "SELECT" section as a string and the associated execution parameters.
        """
        params = SqlBindParameters()
        select_section_lines = ["SELECT"]
        first_column = True
        for select_column in select_columns:
            expr_rendered = self.EXPR_RENDERER.render_sql_expr(select_column.expr)
            # Merge all execution parameters together. Similar pattern follows below.
            params.update(expr_rendered.execution_parameters)

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
                select_section_lines.append(f"{self.INDENT}{column_select_str}")
            else:
                select_section_lines.append(f"{self.INDENT}, {column_select_str}")

        return "\n".join(select_section_lines), params

    def _render_from_section(
        self, from_source: SqlQueryPlanNode, from_source_alias: str
    ) -> Tuple[str, SqlBindParameters]:
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
        if from_source.is_table:
            from_section_lines.append(f"FROM {from_render_result.sql} {from_source_alias}")
        else:
            from_section_lines.append("FROM (")
            from_section_lines.append(textwrap.indent(from_render_result.sql, prefix=self.INDENT))
            from_section_lines.append(f") {from_source_alias}")
        from_section = "\n".join(from_section_lines)

        return from_section, from_render_result.execution_parameters

    def _render_joins_section(self, join_descriptions: Sequence[SqlJoinDescription]) -> Tuple[str, SqlBindParameters]:
        """Convert the join descriptions into a "JOIN" section.

        e.g.
        JOIN (
          SELECT...
        ) right
        ON
          left.ds = right.ds

        Returns a tuple of the "JOIN" section as a string and the associated execution parameters.
        """
        params = SqlBindParameters()
        join_section_lines = []
        for join_description in join_descriptions:
            # Render the source for the join
            right_source_rendered = self._render_node(join_description.right_source)
            params.update(right_source_rendered.execution_parameters)

            # Render the on condition for the join
            on_condition_rendered = self.EXPR_RENDERER.render_sql_expr(join_description.on_condition)
            params.update(on_condition_rendered.execution_parameters)

            if join_description.right_source.is_table:
                join_section_lines.append(join_description.join_type.value)
                join_section_lines.append(
                    textwrap.indent(
                        f"{right_source_rendered.sql} {join_description.right_source_alias}", prefix=self.INDENT
                    )
                )
            else:
                join_section_lines.append(f"{join_description.join_type.value} (")
                join_section_lines.append(textwrap.indent(right_source_rendered.sql, prefix=self.INDENT))
                join_section_lines.append(f") {join_description.right_source_alias}")
            join_section_lines.append("ON")
            join_section_lines.append(textwrap.indent(on_condition_rendered.sql, prefix=self.INDENT))

        return "\n".join(join_section_lines), params

    def _render_group_by_section(self, group_by_columns: Sequence[SqlSelectColumn]) -> Tuple[str, SqlBindParameters]:
        """Convert the group by columns into a "GROUP BY" section.

        e.g.
        GROUP BY
          a.ds

        Returns a tuple of the "GROUP BY" section as a string and the associated execution parameters.
        """
        group_by_section_lines: List[str] = []
        params = SqlBindParameters()
        first = True
        for group_by_column in group_by_columns:
            group_by_expr_rendered = self.EXPR_RENDERER.render_sql_expr(group_by_column.expr)
            params.update(group_by_expr_rendered.execution_parameters)
            if first:
                first = False
                group_by_section_lines.append("GROUP BY")
                group_by_section_lines.append(textwrap.indent(group_by_expr_rendered.sql, prefix=self.INDENT))
            else:
                group_by_section_lines.append(textwrap.indent(f", {group_by_expr_rendered.sql}", prefix=self.INDENT))

        return "\n".join(group_by_section_lines), params

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> SqlPlanRenderResult:  # noqa: D
        # Keep track of all execution parameters for all expressions
        combined_params = SqlBindParameters()

        # Render description section
        description_section = "\n".join([f"-- {x}" for x in node.description.split("\n")])

        # Render "SELECT" column section
        select_section, select_params = self._render_select_columns_section(node.select_columns, len(node.parent_nodes))
        combined_params.update(select_params)

        # Render "FROM" section
        from_section, from_params = self._render_from_section(node.from_source, node.from_source_alias)
        combined_params.update(from_params)

        # Render "JOIN" section
        join_section, join_params = self._render_joins_section(node.join_descs)
        combined_params.update(join_params)

        # Render "GROUP BY" section
        group_by_section, group_by_params = self._render_group_by_section(node.group_bys)
        combined_params.update(group_by_params)

        # Render "WHERE" section
        where_section = None
        if node.where:
            where_render_result = self.EXPR_RENDERER.render_sql_expr(node.where)
            combined_params.update(where_render_result.execution_parameters)
            where_section = f"WHERE {where_render_result.sql}"

        # Render "ORDER BY" section
        order_by_section = None
        if node.order_bys:
            order_by_items: List[str] = []
            for order_by in node.order_bys:
                order_by_render_result = self.EXPR_RENDERER.render_sql_expr(order_by.expr)
                order_by_items.append(order_by_render_result.sql + (" DESC" if order_by.desc else ""))
                combined_params.update(order_by_render_result.execution_parameters)

            order_by_section = "ORDER BY " + ", ".join(order_by_items)

        # Render "LIMIT" section
        limit_section = None
        if node.limit:
            limit_section = f"LIMIT {node.limit}"

        # Combine the sections into a single string.
        sections_to_render = []

        if description_section:
            sections_to_render.append(description_section)

        sections_to_render.append(select_section)
        sections_to_render.append(from_section)

        if join_section:
            sections_to_render.append(join_section)

        if where_section:
            sections_to_render.append(where_section)

        if group_by_section:
            sections_to_render.append(group_by_section)

        if order_by_section:
            sections_to_render.append(order_by_section)

        if limit_section:
            sections_to_render.append(limit_section)

        return SqlPlanRenderResult(
            sql="\n".join(sections_to_render),
            execution_parameters=combined_params,
        )

    def visit_table_from_clause_node(self, node: SqlTableFromClauseNode) -> SqlPlanRenderResult:  # noqa: D
        return SqlPlanRenderResult(
            sql=node.sql_table.sql,
            execution_parameters=SqlBindParameters(),
        )

    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> SqlPlanRenderResult:  # noqa: D
        return SqlPlanRenderResult(
            sql=node.select_query.rstrip(),
            execution_parameters=SqlBindParameters(),
        )

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
