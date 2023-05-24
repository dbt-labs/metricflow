from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.sql.optimizer.sub_query_reducer import SqlSubQueryReducer
from metricflow.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
)
from metricflow.sql.sql_plan import (
    SqlJoinDescription,
    SqlJoinType,
    SqlOrderByDescription,
    SqlSelectColumn,
    SqlSelectStatementNode,
    SqlTableFromClauseNode,
)
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.sql.compare_sql_plan import assert_default_rendered_sql_equal


@pytest.fixture
def base_select_statement() -> SqlSelectStatementNode:
    """SELECT statement used to build test cases.

    SELECT
      src2.col0 AS col0
      src2.col1 AS col1
      -- src2
      SELECT
        src1.col0 AS col0
        , src1.col1 AS col1
      FROM (
        -- src1
        SELECT
          src0.col0 AS col0
          , src0.col1 AS col1
        FROM demo.from_source_table src0
        LIMIT 2
      ) src1
      LIMIT 1
    ) src2
    ORDER BY src2.col0
    """
    return SqlSelectStatementNode(
        description="src3",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression(col_ref=SqlColumnReference(table_alias="src2", column_name="col0")),
                column_alias="col0",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression(col_ref=SqlColumnReference(table_alias="src2", column_name="col1")),
                column_alias="col1",
            ),
        ),
        from_source=SqlSelectStatementNode(
            description="src2",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(
                        col_ref=SqlColumnReference(table_alias="src1", column_name="col0")
                    ),
                    column_alias="col0",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(
                        col_ref=SqlColumnReference(table_alias="src1", column_name="col1")
                    ),
                    column_alias="col1",
                ),
            ),
            from_source=SqlSelectStatementNode(
                description="src1",
                select_columns=(
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression(
                            col_ref=SqlColumnReference(
                                table_alias="src0",
                                column_name="col0",
                            )
                        ),
                        column_alias="col0",
                    ),
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression(
                            col_ref=SqlColumnReference(
                                table_alias="src0",
                                column_name="col1",
                            )
                        ),
                        column_alias="col1",
                    ),
                ),
                from_source=SqlTableFromClauseNode(
                    sql_table=SqlTable(schema_name="demo", table_name="from_source_table")
                ),
                from_source_alias="src0",
                joins_descs=(),
                group_bys=(),
                order_bys=(),
                limit=2,
            ),
            from_source_alias="src1",
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
            limit=1,
        ),
        from_source_alias="src2",
        joins_descs=(),
        group_bys=(),
        where=None,
        order_bys=(
            SqlOrderByDescription(
                expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias="src2",
                        column_name="col0",
                    )
                ),
                desc=False,
            ),
        ),
        limit=None,
    )


def test_reduce_sub_query(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where an outer query should be reduced into its inner query with merged LIMIT expressions."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_plan_node=base_select_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_plan_node=sub_query_reducer.optimize(base_select_statement),
        plan_id="after_reducing",
    )


@pytest.fixture
def rewrite_order_by_statement() -> SqlSelectStatementNode:
    """Helps check order-by rewrites when a query is reduced.

    --
    SELECT
      src2.col0
      src2.col1
    FROM (
      SELECT
        src0.col0 AS col0
        src1.col1 AS col1
      FROM demo.src0 src0
      JOIN demo.src1 src1
      ON
        src0.join_col = src1.join_col
    ) src2
    ORDER BY
    src2.col1
    """
    return SqlSelectStatementNode(
        description="src3",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression(col_ref=SqlColumnReference(table_alias="src2", column_name="col0")),
                column_alias="col0",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression(col_ref=SqlColumnReference(table_alias="src2", column_name="col1")),
                column_alias="col1",
            ),
        ),
        from_source=(
            SqlSelectStatementNode(
                description="src2",
                select_columns=(
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression(
                            col_ref=SqlColumnReference(table_alias="src0", column_name="col0")
                        ),
                        column_alias="col0",
                    ),
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression(
                            col_ref=SqlColumnReference(table_alias="src1", column_name="col1")
                        ),
                        column_alias="col1",
                    ),
                ),
                from_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="demo", table_name="src0")),
                from_source_alias="src0",
                joins_descs=(
                    SqlJoinDescription(
                        right_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="demo", table_name="src1")),
                        right_source_alias="src1",
                        on_condition=SqlComparisonExpression(
                            left_expr=SqlColumnReferenceExpression(
                                col_ref=SqlColumnReference(table_alias="src0", column_name="join_col")
                            ),
                            comparison=SqlComparison.EQUALS,
                            right_expr=SqlColumnReferenceExpression(
                                col_ref=SqlColumnReference(table_alias="src1", column_name="join_col")
                            ),
                        ),
                        join_type=SqlJoinType.INNER,
                    ),
                ),
                where=None,
                group_bys=(),
                order_bys=(),
            )
        ),
        from_source_alias="src2",
        joins_descs=(),
        group_bys=(),
        where=None,
        order_bys=(
            SqlOrderByDescription(
                expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias="src2",
                        column_name="col1",
                    )
                ),
                desc=False,
            ),
        ),
        limit=None,
    )


def test_rewrite_order_by_with_a_join_in_parent(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    rewrite_order_by_statement: SqlSelectStatementNode,
) -> None:
    """Tests rewriting an order by when the parent has a join."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_plan_node=rewrite_order_by_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_plan_node=sub_query_reducer.optimize(rewrite_order_by_statement),
        plan_id="after_reducing",
    )
