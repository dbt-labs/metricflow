from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
    SqlStringExpression,
)
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.string_helpers import mf_dedent

from metricflow.sql.optimizer.column_pruning.column_pruner import SqlColumnPrunerOptimizer
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
def column_pruner() -> SqlColumnPrunerOptimizer:  # noqa: D103
    return SqlColumnPrunerOptimizer()


@pytest.fixture
def sql_plan_renderer() -> SqlPlanRenderer:  # noqa: D103
    return DefaultSqlPlanRenderer()


def test_no_pruning(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    sql_plan_renderer: DefaultSqlPlanRenderer,
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
    sql_plan_renderer: DefaultSqlPlanRenderer,
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
    sql_plan_renderer: DefaultSqlPlanRenderer,
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
    sql_plan_renderer: DefaultSqlPlanRenderer,
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


def test_common_cte_aliases_in_nested_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    sql_plan_renderer: DefaultSqlPlanRenderer,
) -> None:
    """Test the case where a CTE defined in the top-level SELECT has the same name as a CTE in a sub-query ."""
    top_level_select_ctes = (
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
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_1")
                        ),
                        column_alias="cte_source__col_1",
                    ),
                ),
                from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="test_schema", table_name="test_table")),
                from_source_alias="test_table_alias",
            ),
        ),
    )
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
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="test_table_alias", column_name="col_1")
                        ),
                        column_alias="cte_source__col_1",
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
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="right_source_alias", column_name="right_source__col_1")
                ),
                column_alias="top_level__col_1",
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
        join_descs=(
            SqlJoinDescription(
                right_source=SqlSelectStatementNode.create(
                    description="joined_sub_query",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="from_source_alias", column_name="cte_source__col_1"
                                )
                            ),
                            column_alias="right_source__col_1",
                        ),
                    ),
                    from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_source")),
                    from_source_alias="from_source_alias",
                ),
                right_source_alias="right_source_alias",
                on_condition=SqlComparisonExpression.create(
                    left_expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="from_source_alias", column_name="from_source__col_0")
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="right_source_alias", column_name="right_source__col_1")
                    ),
                ),
                join_type=SqlJoinType.INNER,
            ),
        ),
        cte_sources=top_level_select_ctes,
    )

    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=column_pruner,
        sql_plan_renderer=sql_plan_renderer,
        select_statement=top_level_select,
        expectation_description=mf_dedent(
            """
            In the `from_sub_query`, there is a reference to `cte_source__col_0` in a CTE named `cte_source`. Since
            `from_sub_query` redefines `cte_source`, the column pruner should retain that column in the CTE defined
            in `from_sub_query` but remove the column from the CTE defined in `top_level_select`.
            """
        ),
    )


def test_string_expression(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    sql_plan_renderer: DefaultSqlPlanRenderer,
) -> None:
    """Test a string expression that references a column in the cte."""
    select_statement = SqlSelectStatementNode.create(
        description="Top-level SELECT",
        select_columns=(
            SqlSelectColumn(
                expr=SqlStringExpression.create(sql_expr="cte_source_0__col_0", used_columns=("cte_source_0__col_0",)),
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
        expectation_description="`cte_source_0__col_01` should be retained in the CTE.",
    )


def test_column_reference_expression(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    sql_plan_renderer: DefaultSqlPlanRenderer,
) -> None:
    """Test a column reference expression that does not specify a table alias."""
    select_statement = SqlSelectStatementNode.create(
        description="Top-level SELECT",
        select_columns=(
            SqlSelectColumn(
                expr=SqlStringExpression.create(sql_expr="cte_source_0__col_0", used_columns=("cte_source_0__col_0",)),
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
        expectation_description="`cte_source_0__col_01` should be retained in the CTE.",
    )
