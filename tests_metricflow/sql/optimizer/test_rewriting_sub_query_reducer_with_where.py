from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
    SqlStringExpression,
    SqlStringLiteralExpression,
)
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.sql.optimizer.rewriting_sub_query_reducer import SqlRewritingSubQueryReducer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer, SqlPlanRenderer
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.optimizer.check_optimizer import assert_optimizer_result_snapshot_equal


@pytest.fixture(scope="session")
def sub_query_reducer() -> SqlRewritingSubQueryReducer:  # noqa: D103
    return SqlRewritingSubQueryReducer()


@pytest.fixture(scope="session")
def sql_plan_renderer() -> SqlPlanRenderer:  # noqa: D103
    return DefaultSqlPlanRenderer()


def test_reduce_sub_query_with_where(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_plan_renderer: SqlPlanRenderer,
    sub_query_reducer: SqlRewritingSubQueryReducer,
) -> None:
    """Tests a case where an outer query with a WHERE filter should be reduced into its inner query.

    This is allowed as the `SELECT` statement in the inner query consists of expressions that reference columns with
    the same alias.

    SQL:
        -- src2
        SELECT
          src1.bookings AS bookings
          , src1.ds AS ds
        FROM (
          -- src1
          SELECT
            src0.bookings AS bookings
            , src0.ds AS ds
          FROM demo.fct_bookings src0
        ) src1
        WHERE src1.ds >= "2020-01-01"
    """
    select_statement_node = SqlSelectStatementNode.create(
        description="src2",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src1", column_name="bookings")
                ),
                column_alias="bookings",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src1", column_name="ds")
                ),
                column_alias="ds",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="src1",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(
                            table_alias="src0",
                            column_name="bookings",
                        )
                    ),
                    column_alias="bookings",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(
                            table_alias="src0",
                            column_name="ds",
                        )
                    ),
                    column_alias="ds",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="src0",
        ),
        from_source_alias="src1",
        where=SqlComparisonExpression.create(
            left_expr=SqlColumnReferenceExpression.create(
                SqlColumnReference(
                    table_alias="src1",
                    column_name="ds",
                )
            ),
            comparison=SqlComparison.GREATER_THAN_OR_EQUALS,
            right_expr=SqlStringLiteralExpression.create("2020-01-01"),
        ),
    )

    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=sub_query_reducer,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=select_statement_node,
    )


def test_reduce_sub_query_with_where_and_alias_mismatch(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_plan_renderer: SqlPlanRenderer,
    sub_query_reducer: SqlRewritingSubQueryReducer,
) -> None:
    """Tests a case where an outer query with a WHERE filter should be not reduced into its inner query.

    This is not allowed as the `SELECT` statement in the inner query consists of expressions that reference columns with
    different aliases.

    SQL:
        -- src2
        SELECT
          src1.bookings AS bookings
          , src1.ds AS ds
        FROM (
          -- src1
          SELECT
            src0.bookings AS bookings
            , src0.created_at AS ds
          FROM demo.fct_bookings src0
        ) src1
        WHERE src1.ds >= "2020-01-01"
    """
    select_statement_node = SqlSelectStatementNode.create(
        description="src2",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src1", column_name="bookings")
                ),
                column_alias="bookings",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src1", column_name="ds")
                ),
                column_alias="ds",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="src1",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(
                            table_alias="src0",
                            column_name="bookings",
                        )
                    ),
                    column_alias="bookings",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(
                            table_alias="src0",
                            column_name="created_at",
                        )
                    ),
                    column_alias="ds",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="src0",
        ),
        from_source_alias="src1",
        where=SqlComparisonExpression.create(
            left_expr=SqlColumnReferenceExpression.create(
                SqlColumnReference(
                    table_alias="src1",
                    column_name="ds",
                )
            ),
            comparison=SqlComparison.GREATER_THAN_OR_EQUALS,
            right_expr=SqlStringLiteralExpression.create("2020-01-01"),
        ),
    )

    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=sub_query_reducer,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=select_statement_node,
    )


def test_reduce_sub_query_with_where_and_non_column_reference(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_plan_renderer: SqlPlanRenderer,
    sub_query_reducer: SqlRewritingSubQueryReducer,
) -> None:
    """Tests a case where an outer query with a WHERE filter should be not reduced into its inner query.

    This is not allowed as the `SELECT` statement in the inner query includes expressions that aren't referencing
    columns.

    SQL:
        -- src2
        SELECT
          src1.bookings AS bookings
          , src1.ds AS ds
        FROM (
          -- src1
          SELECT
            1 AS bookings
            , src0.created_at AS ds
          FROM demo.fct_bookings src0
        ) src1
        WHERE src1.ds >= "2020-01-01"
    """
    select_statement_node = SqlSelectStatementNode.create(
        description="src2",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src1", column_name="bookings")
                ),
                column_alias="bookings",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src1", column_name="ds")
                ),
                column_alias="ds",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="src1",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlStringExpression.create(
                        sql_expr="1",
                        used_columns=(),
                    ),
                    column_alias="bookings",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(
                            table_alias="src0",
                            column_name="ds",
                        )
                    ),
                    column_alias="ds",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="src0",
        ),
        from_source_alias="src1",
        where=SqlComparisonExpression.create(
            left_expr=SqlColumnReferenceExpression.create(
                SqlColumnReference(
                    table_alias="src1",
                    column_name="ds",
                )
            ),
            comparison=SqlComparison.GREATER_THAN_OR_EQUALS,
            right_expr=SqlStringLiteralExpression.create("2020-01-01"),
        ),
    )

    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=sub_query_reducer,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=select_statement_node,
    )
