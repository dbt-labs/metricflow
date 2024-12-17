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

from metricflow.sql.optimizer.column_pruner import SqlColumnPrunerOptimizer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer, SqlQueryPlanRenderer
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
def column_pruner() -> SqlColumnPrunerOptimizer:  # noqa: D103
    return SqlColumnPrunerOptimizer()


@pytest.fixture
def sql_plan_renderer() -> SqlQueryPlanRenderer:  # noqa: D103
    return DefaultSqlQueryPlanRenderer()


def test_no_pruning(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    sql_plan_renderer: DefaultSqlQueryPlanRenderer,
) -> None:
    """Tests a case where no pruning should occur for a CTE."""
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
                                col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_0")
                            ),
                            column_alias="cte_source_0__col_0",
                        ),
                    ),
                    from_source=SqlTableNode.create(
                        sql_table=SqlTable(schema_name="test_schema", table_name="test_table")
                    ),
                    from_source_alias="test_table_alias",
                ),
            ),
        ),
    )
    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=column_pruner,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=select_statement,
    )


def test_simple_pruning(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    sql_plan_renderer: DefaultSqlQueryPlanRenderer,
) -> None:
    """Tests the simplest case of pruning a CTE where a query depends on a CTE, and that CTE is pruned."""
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
                                col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_0")
                            ),
                            column_alias="cte_source_0__col_0",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_0")
                            ),
                            column_alias="cte_source_0__col_1",
                        ),
                    ),
                    from_source=SqlTableNode.create(
                        sql_table=SqlTable(schema_name="test_schema", table_name="test_table")
                    ),
                    from_source_alias="test_table_alias",
                ),
            ),
        ),
    )
    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=column_pruner,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=select_statement,
    )


def test_nested_pruning(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    sql_plan_renderer: DefaultSqlQueryPlanRenderer,
) -> None:
    """Tests the case of pruning a CTE where a query depends on a CTE, and that CTE depends on another CTE."""
    select_statement = SqlSelectStatementNode.create(
        description="Top-level SELECT",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="cte_source_1_alias", column_name="cte_source_1__col_0")
                ),
                column_alias="top_level__col_0",
            ),
        ),
        from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_source_1")),
        from_source_alias="cte_source_1_alias",
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
                    from_source=SqlTableNode.create(
                        sql_table=SqlTable(schema_name="test_schema", table_name="test_table")
                    ),
                    from_source_alias="test_table_alias",
                ),
            ),
            SqlCteNode.create(
                cte_alias="cte_source_1",
                select_statement=SqlSelectStatementNode.create(
                    description="CTE source 1",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="cte_source_0_alias", column_name="cte_source_0__col_0"
                                )
                            ),
                            column_alias="cte_source_1__col_0",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="cte_source_0_alias", column_name="cte_source_0__col_0"
                                )
                            ),
                            column_alias="cte_source_1__col_1",
                        ),
                    ),
                    from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_source_0")),
                    from_source_alias="cte_source_0_alias",
                ),
            ),
        ),
    )

    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=column_pruner,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=select_statement,
    )


def test_multi_child_pruning(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    sql_plan_renderer: DefaultSqlQueryPlanRenderer,
) -> None:
    """Tests the case of pruning a CTE where difference sources depend on the same CTE."""
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
                right_source=SqlSelectStatementNode.create(
                    description="Joined sub-query",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="cte_source_0_alias_in_right_source", column_name="cte_source_0__col_0"
                                )
                            ),
                            column_alias="right_source__col_0",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="cte_source_0_alias_in_right_source", column_name="cte_source_0__col_1"
                                )
                            ),
                            column_alias="right_source__col_1",
                        ),
                    ),
                    from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_source_0")),
                    from_source_alias="cte_source_0_alias_in_right_source",
                ),
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
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_1")
                            ),
                            column_alias="cte_source_0__col_2",
                        ),
                    ),
                    from_source=SqlTableNode.create(
                        sql_table=SqlTable(schema_name="test_schema", table_name="test_table")
                    ),
                    from_source_alias="test_table_alias",
                ),
            ),
        ),
    )

    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=column_pruner,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=select_statement,
    )
