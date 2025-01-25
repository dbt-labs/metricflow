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
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer
from metricflow.sql.sql_cte_node import SqlCteNode
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.optimizer.check_optimizer import assert_optimizer_result_snapshot_equal


@pytest.fixture
def sql_plan_renderer() -> DefaultSqlPlanRenderer:  # noqa: D103
    return DefaultSqlPlanRenderer()


def test_table_alias_simplification(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_plan_renderer: DefaultSqlPlanRenderer,
) -> None:
    """Tests that table aliases in the SELECT statement of a CTE are removed when not needed."""
    select_statement = SqlSelectStatementNode.create(
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
    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=SqlTableAliasSimplifier(),
        sql_plan_renderer=sql_plan_renderer,
        select_statement=select_statement,
    )


def test_table_alias_no_simplification(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_plan_renderer: DefaultSqlPlanRenderer,
) -> None:
    """Tests that table aliases in the SELECT statement of a CTE are not removed when required."""
    select_statement = SqlSelectStatementNode.create(
        description="Top-level SELECT",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="cte_source_0_alias", column_name="cte_source_0__col_0")
                ),
                column_alias="top_level__col_0",
            ),
        ),
        from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_source_0")),
        from_source_alias="cte_source_0_alias",
        cte_sources=(
            SqlCteNode.create(
                cte_alias="cte_source_0",
                select_statement=SqlSelectStatementNode.create(
                    description="CTE source 0",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="from_source_alias", column_name="col_0")
                            ),
                            column_alias="cte_source_0__col_0",
                        ),
                    ),
                    from_source=SqlTableNode.create(
                        sql_table=SqlTable(schema_name="test_schema", table_name="test_table_0")
                    ),
                    from_source_alias="from_source_alias",
                    join_descs=(
                        SqlJoinDescription(
                            right_source=SqlTableNode.create(
                                sql_table=SqlTable(schema_name="test_schema", table_name="test_table_1")
                            ),
                            right_source_alias="right_source_alias",
                            on_condition=SqlComparisonExpression.create(
                                left_expr=SqlColumnReferenceExpression.create(
                                    col_ref=SqlColumnReference(table_alias="from_source_alias", column_name="col_0")
                                ),
                                comparison=SqlComparison.EQUALS,
                                right_expr=SqlColumnReferenceExpression.create(
                                    col_ref=SqlColumnReference(table_alias="right_source_alias", column_name="col_0")
                                ),
                            ),
                            join_type=SqlJoinType.INNER,
                        ),
                    ),
                ),
            ),
        ),
    )
    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=SqlTableAliasSimplifier(),
        sql_plan_renderer=sql_plan_renderer,
        select_statement=select_statement,
    )
