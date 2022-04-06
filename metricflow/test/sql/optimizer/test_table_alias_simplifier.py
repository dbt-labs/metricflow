import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.sql.optimizer.table_alias_simplifier import SqlTableAliasSimplifier
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer, SqlQueryPlanRenderer
from metricflow.sql.sql_exprs import (
    SqlColumnReferenceExpression,
    SqlColumnReference,
    SqlComparisonExpression,
    SqlComparison,
)
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
    SqlTableFromClauseNode,
    SqlSelectStatementNode,
    SqlJoinDescription,
    SqlJoinType,
)
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.sql.compare_sql_plan import assert_default_rendered_sql_equal


@pytest.fixture
def sql_plan_renderer() -> SqlQueryPlanRenderer:  # noqa: D
    return DefaultSqlQueryPlanRenderer()


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
    return SqlSelectStatementNode(
        description="test0",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression(
                    col_ref=SqlColumnReference(table_alias="from_source", column_name="col0")
                ),
                column_alias="from_source_col0",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression(
                    col_ref=SqlColumnReference(table_alias="joined_source", column_name="col0")
                ),
                column_alias="joined_source_col0",
            ),
        ),
        from_source=SqlSelectStatementNode(
            description="from_source",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(
                        col_ref=SqlColumnReference(
                            table_alias="from_source_table",
                            column_name="col0",
                        )
                    ),
                    column_alias="col0",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(
                        col_ref=SqlColumnReference(
                            table_alias="from_source_table",
                            column_name="join_col",
                        )
                    ),
                    column_alias="join_col",
                ),
            ),
            from_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="demo", table_name="from_source_table")),
            from_source_alias="from_source_table",
            joins_descs=(),
            group_bys=(),
            order_bys=(),
        ),
        from_source_alias="from_source",
        joins_descs=(
            SqlJoinDescription(
                right_source=SqlSelectStatementNode(
                    description="joined_source",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression(
                                col_ref=SqlColumnReference(
                                    table_alias="joined_source_table",
                                    column_name="col0",
                                )
                            ),
                            column_alias="col0",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression(
                                col_ref=SqlColumnReference(
                                    table_alias="joined_source_table",
                                    column_name="join_col",
                                )
                            ),
                            column_alias="join_col",
                        ),
                    ),
                    from_source=SqlTableFromClauseNode(
                        sql_table=SqlTable(schema_name="demo", table_name="joined_source_table")
                    ),
                    from_source_alias="joined_source_table",
                    joins_descs=(),
                    group_bys=(),
                    order_bys=(),
                ),
                right_source_alias="joined_source",
                on_condition=SqlComparisonExpression(
                    left_expr=SqlColumnReferenceExpression(
                        col_ref=SqlColumnReference(table_alias="from_source", column_name="join_col")
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression(
                        col_ref=SqlColumnReference(table_alias="joined_source", column_name="join_col")
                    ),
                ),
                join_type=SqlJoinType.INNER,
            ),
        ),
        where=None,
        group_bys=(),
        order_bys=(),
    )


def test_table_alias_simplification(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where no pruning should occur"""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_plan_node=base_select_statement,
        plan_id="before_alias_simplification",
    )
    simplified_select_node = SqlTableAliasSimplifier().optimize(base_select_statement)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_plan_node=simplified_select_node,
        plan_id="after_alias_simplification",
    )
