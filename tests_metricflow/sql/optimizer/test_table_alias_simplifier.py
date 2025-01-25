from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
)
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.sql.optimizer.table_alias_simplifier import SqlTableAliasSimplifier
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer, SqlPlanRenderer
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.compare_sql_plan import assert_default_rendered_sql_equal


@pytest.fixture
def sql_plan_renderer() -> SqlPlanRenderer:  # noqa: D103
    return DefaultSqlPlanRenderer()


@pytest.fixture
def base_select_statement() -> SqlSelectStatementNode:
    """SELECT statement used to build test cases.

    -- test0
    SELECT
      from_source.col0 AS from_source_col0
      , joined_source.col0 AS joined_source_col0
    FROM (
      -- from_source
      SELECT
        from_source_table.col0 AS col0
        , from_source_table.join_col AS join_col
      FROM demo.from_source_table from_source_table
    ) from_source
    JOIN (
      -- joined_source
      SELECT
        joined_source_table.col0 AS col0
        , joined_source_table.join_col AS join_col
      FROM demo.joined_source_table joined_source_table
    ) joined_source
    ON
      from_source.join_col = joined_source.join_col
    """
    return SqlSelectStatementNode.create(
        description="test0",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="from_source", column_name="col0")
                ),
                column_alias="from_source_col0",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="joined_source", column_name="col0")
                ),
                column_alias="joined_source_col0",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="from_source",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(
                            table_alias="from_source_table",
                            column_name="col0",
                        )
                    ),
                    column_alias="col0",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(
                            table_alias="from_source_table",
                            column_name="join_col",
                        )
                    ),
                    column_alias="join_col",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="from_source_table")),
            from_source_alias="from_source_table",
        ),
        from_source_alias="from_source",
        join_descs=(
            SqlJoinDescription(
                right_source=SqlSelectStatementNode.create(
                    description="joined_source",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="joined_source_table",
                                    column_name="col0",
                                )
                            ),
                            column_alias="col0",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="joined_source_table",
                                    column_name="join_col",
                                )
                            ),
                            column_alias="join_col",
                        ),
                    ),
                    from_source=SqlTableNode.create(
                        sql_table=SqlTable(schema_name="demo", table_name="joined_source_table")
                    ),
                    from_source_alias="joined_source_table",
                ),
                right_source_alias="joined_source",
                on_condition=SqlComparisonExpression.create(
                    left_expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="from_source", column_name="join_col")
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="joined_source", column_name="join_col")
                    ),
                ),
                join_type=SqlJoinType.INNER,
            ),
        ),
    )


def test_table_alias_simplification(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests that table aliases are removed when not needed."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=base_select_statement,
        plan_id="before_alias_simplification",
    )
    simplified_select_node = SqlTableAliasSimplifier().optimize(base_select_statement)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=simplified_select_node,
        plan_id="after_alias_simplification",
    )
