from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.sql.optimizer.rewriting_sub_query_reducer import SqlRewritingSubQueryReducer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer, SqlQueryPlanRenderer
from metricflow.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
)
from metricflow.sql.sql_plan import (
    SqlCteNode,
    SqlJoinDescription,
    SqlSelectColumn,
    SqlSelectStatementNode,
    SqlTableNode,
)
from tests_metricflow.sql.optimizer.check_optimizer import assert_optimizer_result_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def sub_query_reducer() -> SqlRewritingSubQueryReducer:  # noqa: D103
    return SqlRewritingSubQueryReducer()


@pytest.fixture
def sql_plan_renderer() -> SqlQueryPlanRenderer:  # noqa: D103
    return DefaultSqlQueryPlanRenderer()


@pytest.fixture(scope="module")
def base_select_statement_to_reduce() -> SqlSelectStatementNode:
    """A SELECT statement that has sub-queries that can be reduced.

    This renders to the following SQL:

    -- Top-level SELECT
    WITH cte_source_0 AS (
      -- CTE source 0
      SELECT
        test_table_alias.col_0 AS cte_source_0__col_0
        , test_table_alias.col_1 AS cte_source_0__col_1
      FROM (
        -- CTE source 0 sub-query
        SELECT
          test_table_alias.col_0 AS cte_source_0_subquery__col_0
          , test_table_alias.col_0 AS cte_source_0_subquery__col_1
        FROM test_schema.test_table test_table_alias
      ) cte_source_0_subquery
    )

    , cte_source_1 AS (
      -- CTE source 1
      SELECT
        test_table_alias.col_0 AS cte_source_1__col_0
        , test_table_alias.col_1 AS cte_source_1__col_1
      FROM (
        -- CTE source 1 sub-query
        SELECT
          cte_source_0_alias.cte_source_0__col_0 AS cte_source_1_subquery__col_0
          , cte_source_0_alias.cte_source_0__col_0 AS cte_source_1_subquery__col_1
        FROM cte_source_0 cte_source_0_alias
      ) cte_source_1_subquery
    )

    SELECT
      cte_source_0_alias.cte_source_0__col_0 AS top_level__col_0
      , right_source_alias.right_source__col_1 AS top_level__col_1
    FROM cte_source_0 cte_source_0_alias
    INNER JOIN
      cte_source_1 right_source_alias
    ON
      cte_source_0_alias.cte_source_0__col_1 = right_source_alias.right_source__col_1
    GROUP BY
      cte_source_0_alias.cte_source_0__col_0
      , right_source_alias.right_source__col_1
    """
    return SqlSelectStatementNode.create(
        description="Top-level SELECT",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="cte_source_0_alias", column_name="cte_source_0__col_0")
                ),
                column_alias="top_level__col_0",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="right_source_alias", column_name="right_source__col_1")
                ),
                column_alias="top_level__col_1",
            ),
        ),
        from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_source_0")),
        from_source_alias="cte_source_0_alias",
        join_descs=(
            SqlJoinDescription(
                right_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_source_1")),
                right_source_alias="right_source_alias",
                on_condition=SqlComparisonExpression.create(
                    left_expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="cte_source_0_alias", column_name="cte_source_0__col_1")
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="right_source_alias", column_name="right_source__col_1")
                    ),
                ),
                join_type=SqlJoinType.INNER,
            ),
        ),
        group_bys=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="cte_source_0_alias", column_name="cte_source_0__col_0")
                ),
                column_alias="top_level__col_0",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="right_source_alias", column_name="right_source__col_1")
                ),
                column_alias="top_level__col_1",
            ),
        ),
        cte_sources=(
            SqlCteNode.create(
                cte_alias="cte_source_0",
                select_statement=SqlSelectStatementNode.create(
                    description="CTE source 0",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_0")
                            ),
                            column_alias="cte_source_0__col_0",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_1")
                            ),
                            column_alias="cte_source_0__col_1",
                        ),
                    ),
                    from_source=SqlSelectStatementNode.create(
                        description="CTE source 0 sub-query",
                        select_columns=(
                            SqlSelectColumn(
                                expr=SqlColumnReferenceExpression.create(
                                    col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_0")
                                ),
                                column_alias="cte_source_0_subquery__col_0",
                            ),
                            SqlSelectColumn(
                                expr=SqlColumnReferenceExpression.create(
                                    col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_0")
                                ),
                                column_alias="cte_source_0_subquery__col_1",
                            ),
                        ),
                        from_source=SqlTableNode.create(
                            sql_table=SqlTable(schema_name="test_schema", table_name="test_table")
                        ),
                        from_source_alias="test_table_alias",
                    ),
                    from_source_alias="cte_source_0_subquery",
                ),
            ),
            SqlCteNode.create(
                cte_alias="cte_source_1",
                select_statement=SqlSelectStatementNode.create(
                    description="CTE source 1",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_0")
                            ),
                            column_alias="cte_source_1__col_0",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_1")
                            ),
                            column_alias="cte_source_1__col_1",
                        ),
                    ),
                    from_source=SqlSelectStatementNode.create(
                        description="CTE source 1 sub-query",
                        select_columns=(
                            SqlSelectColumn(
                                expr=SqlColumnReferenceExpression.create(
                                    col_ref=SqlColumnReference(
                                        table_alias="cte_source_0_alias", column_name="cte_source_0__col_0"
                                    )
                                ),
                                column_alias="cte_source_1_subquery__col_0",
                            ),
                            SqlSelectColumn(
                                expr=SqlColumnReferenceExpression.create(
                                    col_ref=SqlColumnReference(
                                        table_alias="cte_source_0_alias", column_name="cte_source_0__col_0"
                                    )
                                ),
                                column_alias="cte_source_1_subquery__col_1",
                            ),
                        ),
                        from_source=SqlTableNode.create(
                            sql_table=SqlTable(schema_name=None, table_name="cte_source_0")
                        ),
                        from_source_alias="cte_source_0_alias",
                    ),
                    from_source_alias="cte_source_1_subquery",
                ),
            ),
        ),
    )


def test_reduce_cte(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sub_query_reducer: SqlRewritingSubQueryReducer,
    sql_plan_renderer: DefaultSqlQueryPlanRenderer,
    base_select_statement_to_reduce: SqlSelectStatementNode,
) -> None:
    """Tests that the SELECT statements in CTEs are reduced."""
    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=sub_query_reducer,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=base_select_statement_to_reduce,
    )


def test_reduce_cte_with_use_column_alias_in_group_bys(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_plan_renderer: DefaultSqlQueryPlanRenderer,
    base_select_statement_to_reduce: SqlSelectStatementNode,
) -> None:
    """This tests reducing with the `use_column_alias_in_group_bys` flag set."""
    sub_query_reducer = SqlRewritingSubQueryReducer(use_column_alias_in_group_bys=True)
    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=sub_query_reducer,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=base_select_statement_to_reduce,
    )
