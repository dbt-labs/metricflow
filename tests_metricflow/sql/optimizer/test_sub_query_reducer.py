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

from metricflow.sql.optimizer.sub_query_reducer import SqlSubQueryReducer
from metricflow.sql.sql_plan import (
    SqlJoinDescription,
    SqlOrderByDescription,
    SqlSelectColumn,
    SqlSelectStatementNode,
    SqlTableNode,
)
from tests_metricflow.sql.compare_sql_plan import assert_default_rendered_sql_equal


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
    return SqlSelectStatementNode.create(
        description="src3",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src2", column_name="col0")
                ),
                column_alias="col0",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src2", column_name="col1")
                ),
                column_alias="col1",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="src2",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="src1", column_name="col0")
                    ),
                    column_alias="col0",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="src1", column_name="col1")
                    ),
                    column_alias="col1",
                ),
            ),
            from_source=SqlSelectStatementNode.create(
                description="src1",
                select_columns=(
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(
                                table_alias="src0",
                                column_name="col0",
                            )
                        ),
                        column_alias="col0",
                    ),
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(
                                table_alias="src0",
                                column_name="col1",
                            )
                        ),
                        column_alias="col1",
                    ),
                ),
                from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="from_source_table")),
                from_source_alias="src0",
                limit=2,
            ),
            from_source_alias="src1",
            limit=1,
        ),
        from_source_alias="src2",
        order_bys=(
            SqlOrderByDescription(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias="src2",
                        column_name="col0",
                    )
                ),
                desc=False,
            ),
        ),
    )


def test_reduce_sub_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where an outer query should be reduced into its inner query with merged LIMIT expressions."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=base_select_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
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
    return SqlSelectStatementNode.create(
        description="src3",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src2", column_name="col0")
                ),
                column_alias="col0",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src2", column_name="col1")
                ),
                column_alias="col1",
            ),
        ),
        from_source=(
            SqlSelectStatementNode.create(
                description="src2",
                select_columns=(
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="src0", column_name="col0")
                        ),
                        column_alias="col0",
                    ),
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="src1", column_name="col1")
                        ),
                        column_alias="col1",
                    ),
                ),
                from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="src0")),
                from_source_alias="src0",
                join_descs=(
                    SqlJoinDescription(
                        right_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="src1")),
                        right_source_alias="src1",
                        on_condition=SqlComparisonExpression.create(
                            left_expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="src0", column_name="join_col")
                            ),
                            comparison=SqlComparison.EQUALS,
                            right_expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="src1", column_name="join_col")
                            ),
                        ),
                        join_type=SqlJoinType.INNER,
                    ),
                ),
            )
        ),
        from_source_alias="src2",
        order_bys=(
            SqlOrderByDescription(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias="src2",
                        column_name="col1",
                    )
                ),
                desc=False,
            ),
        ),
    )


def test_rewrite_order_by_with_a_join_in_parent(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    rewrite_order_by_statement: SqlSelectStatementNode,
) -> None:
    """Tests rewriting an order by when the parent has a join."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=rewrite_order_by_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=sub_query_reducer.optimize(rewrite_order_by_statement),
        plan_id="after_reducing",
    )


def test_distinct_select_node_is_not_reduced(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests to ensure distinct select node doesn't get overwritten."""
    select_node = SqlSelectStatementNode.create(
        description="test0",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="a", column_name="booking_value")
                ),
                column_alias="booking_value",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="test1",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="a", column_name="booking_value")
                    ),
                    column_alias="booking_value",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="a", column_name="bookings")
                    ),
                    column_alias="bookings",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="a",
            distinct=True,
        ),
        from_source_alias="b",
    )
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=select_node,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=sub_query_reducer.optimize(select_node),
        plan_id="after_reducing",
    )
