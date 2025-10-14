from __future__ import annotations

import logging

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
from metricflow_semantics.toolkit.string_helpers import mf_dedent

from metricflow.sql.optimizer.rewriting_sub_query_reducer import SqlRewritingSubQueryReducer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer, SqlPlanRenderer
from metricflow.sql.sql_cte_node import SqlCteNode
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.optimizer.check_optimizer import assert_optimizer_result_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def sub_query_reducer() -> SqlRewritingSubQueryReducer:  # noqa: D103
    return SqlRewritingSubQueryReducer()


@pytest.fixture
def sql_plan_renderer() -> SqlPlanRenderer:  # noqa: D103
    return DefaultSqlPlanRenderer()


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
    sql_plan_renderer: DefaultSqlPlanRenderer,
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
    sql_plan_renderer: DefaultSqlPlanRenderer,
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


def test_cte_in_subquery_not_reduced(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_plan_renderer: DefaultSqlPlanRenderer,
) -> None:
    """This tests that in cases where there is a CTE defined in a sub-query, it is not reduced."""
    from_sub_query_ctes = (
        SqlCteNode.create(
            cte_alias="cte_source",
            select_statement=SqlSelectStatementNode.create(
                description="CTE source",
                select_columns=(
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_0")
                        ),
                        column_alias="cte_source__col_0",
                    ),
                ),
                from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="test_schema", table_name="test_table")),
                from_source_alias="test_table_alias",
            ),
        ),
    )

    top_level_select = SqlSelectStatementNode.create(
        description="top_level_select",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="from_source_alias", column_name="from_source__col_0")
                ),
                column_alias="top_level__col_0",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="from_sub_query",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="from_source_alias", column_name="cte_source__col_0")
                    ),
                    column_alias="from_source__col_0",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_source")),
            from_source_alias="from_source_alias",
            cte_sources=from_sub_query_ctes,
        ),
        from_source_alias="from_source_alias",
        cte_sources=(),
    )

    sub_query_reducer = SqlRewritingSubQueryReducer()
    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=sub_query_reducer,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=top_level_select,
        expectation_description=mf_dedent(
            """
            A sub-query containing CTEs should not result in the top-level query being merged with the sub-query. For
            this case, the SQL should be the same before / after optimization.
            """
        ),
    )
