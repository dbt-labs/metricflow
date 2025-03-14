from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
    SqlIsNullExpression,
    SqlStringExpression,
)
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.sql.optimizer.column_pruning.column_pruner import SqlColumnPrunerOptimizer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer, SqlPlanRenderer
from metricflow.sql.sql_plan import (
    SqlPlanNode,
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.compare_sql_plan import assert_default_rendered_sql_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def column_pruner() -> SqlColumnPrunerOptimizer:  # noqa: D103
    return SqlColumnPrunerOptimizer()


@pytest.fixture
def sql_plan_renderer() -> SqlPlanRenderer:  # noqa: D103
    return DefaultSqlPlanRenderer()


@pytest.fixture
def base_select_statement() -> SqlSelectStatementNode:
    """SELECT statement used to build test cases.

    In the initial case, all columns are selected from both from_source and joined_source. Test cases will generally
    change the selected columns to see if the sources are pruned appropriately.

    -- test0
    SELECT
      from_source.col0 AS from_source_col0
      , from_source.col1 AS from_source_col1
      , from_source.join_col AS from_source_join_col
      , joined_source.col0 AS joined_source_col0
      , joined_source.col1 AS joined_source_col1
      , joined_source.join_col AS joined_source_join_col
    FROM (
      -- from_source
      SELECT
        from_source_table.col0 AS col0
        , from_source_table.col1 AS col1
        , from_source_table.join_col AS join_col
      FROM demo.from_source_table from_source_table
    ) from_source
    JOIN (
      -- joined_source
      SELECT
        joined_source_table.col0 AS col0
        , joined_source_table.col1 AS col1
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
                    col_ref=SqlColumnReference(table_alias="from_source", column_name="col1")
                ),
                column_alias="from_source_col1",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="from_source", column_name="join_col")
                ),
                column_alias="from_source_join_col",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="joined_source", column_name="col0")
                ),
                column_alias="joined_source_col0",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="joined_source", column_name="col1")
                ),
                column_alias="joined_source_col1",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="joined_source", column_name="join_col")
                ),
                column_alias="joined_source_join_col",
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
                            column_name="col1",
                        )
                    ),
                    column_alias="col1",
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
                                    column_name="col1",
                                )
                            ),
                            column_alias="col1",
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


def test_no_pruning(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where no pruning should occur."""
    logger.debug(LazyFormat("Pruning select statement", base_select_statement=base_select_statement.structure_text()))

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=base_select_statement,
        plan_id="before_pruning",
    )
    column_pruned_select_node = column_pruner.optimize(base_select_statement)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=column_pruned_select_node,
        plan_id="after_pruning",
    )


def test_prune_from_source(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where columns should be pruned from the FROM clause."""
    select_statement_with_some_from_source_column_removed = SqlSelectStatementNode.create(
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
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="joined_source", column_name="col1")
                ),
                column_alias="joined_source_col1",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="joined_source", column_name="join_col")
                ),
                column_alias="joined_source_join_col",
            ),
        ),
        from_source=base_select_statement.from_source,
        from_source_alias=base_select_statement.from_source_alias,
        join_descs=base_select_statement.join_descs,
        group_bys=base_select_statement.group_bys,
        order_bys=base_select_statement.order_bys,
        where=base_select_statement.where,
    )

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=select_statement_with_some_from_source_column_removed,
        plan_id="before_pruning",
    )

    column_pruned_select_node = column_pruner.optimize(select_statement_with_some_from_source_column_removed)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=column_pruned_select_node,
        plan_id="after_pruning",
    )


def test_prune_joined_source(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where columns should be pruned from the JOIN clause."""
    select_statement_with_some_joined_source_column_removed = SqlSelectStatementNode.create(
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
                    col_ref=SqlColumnReference(table_alias="from_source", column_name="col1")
                ),
                column_alias="from_source_col1",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="from_source", column_name="join_col")
                ),
                column_alias="from_source_join_col",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="joined_source", column_name="col0")
                ),
                column_alias="joined_source_col0",
            ),
        ),
        from_source=base_select_statement.from_source,
        from_source_alias=base_select_statement.from_source_alias,
        join_descs=base_select_statement.join_descs,
        group_bys=base_select_statement.group_bys,
        order_bys=base_select_statement.order_bys,
        where=base_select_statement.where,
    )

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=select_statement_with_some_joined_source_column_removed,
        plan_id="before_pruning",
    )

    column_pruned_select_node = column_pruner.optimize(select_statement_with_some_joined_source_column_removed)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=column_pruned_select_node,
        plan_id="after_pruning",
    )


def test_dont_prune_if_in_where(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests that columns aren't pruned from parent sources if columns are used in a where."""
    select_statement_with_other_exprs = SqlSelectStatementNode.create(
        description="test0",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="from_source", column_name="col0")
                ),
                column_alias="from_source_col0",
            ),
        ),
        from_source=base_select_statement.from_source,
        from_source_alias=base_select_statement.from_source_alias,
        join_descs=base_select_statement.join_descs,
        where=SqlIsNullExpression.create(
            SqlColumnReferenceExpression.create(
                col_ref=SqlColumnReference(table_alias="from_source", column_name="col1")
            )
        ),
        group_bys=base_select_statement.group_bys,
        order_bys=base_select_statement.order_bys,
    )

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=select_statement_with_other_exprs,
        plan_id="before_pruning",
    )

    column_pruned_select_node = column_pruner.optimize(select_statement_with_other_exprs)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=column_pruned_select_node,
        plan_id="after_pruning",
    )


def test_dont_prune_with_str_expr(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests that columns aren't pruned from parent sources if there's a string expression in the select."""
    select_statement_with_other_exprs = SqlSelectStatementNode.create(
        description="test0",
        select_columns=(
            SqlSelectColumn(
                expr=SqlStringExpression.create("from_source.col0", requires_parenthesis=False),
                column_alias="some_string_expr",
            ),
        ),
        from_source=base_select_statement.from_source,
        from_source_alias=base_select_statement.from_source_alias,
        join_descs=base_select_statement.join_descs,
        where=base_select_statement.where,
        group_bys=base_select_statement.group_bys,
        order_bys=base_select_statement.order_bys,
    )

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=select_statement_with_other_exprs,
        plan_id="before_pruning",
    )

    column_pruned_select_node = column_pruner.optimize(select_statement_with_other_exprs)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=column_pruned_select_node,
        plan_id="after_pruning",
    )


def test_prune_with_str_expr(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests that columns are from parent sources if there's a string expression in the select with known cols."""
    select_statement_with_other_exprs = SqlSelectStatementNode.create(
        description="test0",
        select_columns=(
            SqlSelectColumn(
                expr=SqlStringExpression.create("from_source.col0", requires_parenthesis=False, used_columns=("col0",)),
                column_alias="some_string_expr",
            ),
        ),
        from_source=base_select_statement.from_source,
        from_source_alias=base_select_statement.from_source_alias,
        join_descs=base_select_statement.join_descs,
        where=base_select_statement.where,
        group_bys=base_select_statement.group_bys,
        order_bys=base_select_statement.order_bys,
    )

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=select_statement_with_other_exprs,
        plan_id="before_pruning",
    )

    column_pruned_select_node = column_pruner.optimize(select_statement_with_other_exprs)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=column_pruned_select_node,
        plan_id="after_pruning",
    )


@pytest.fixture
def string_select_statement() -> SqlSelectStatementNode:
    """Test case for propagating a column used in a string expression.

    -- test0
    SELECT
      col0 AS from_source_col0
    FROM (
      -- from_source
      SELECT
        from_source_table.col0 AS col0
        , from_source_table.col1 AS col1
        , from_source_table.join_col AS join_col
      FROM demo.from_source_table from_source_table
    ) from_source
    JOIN (
      -- joined_source
      SELECT
        joined_source_table.col2 AS col2
        , joined_source_table.col3 AS col3
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
                expr=SqlStringExpression.create(sql_expr="col0", used_columns=("col0",)),
                column_alias="from_source_col0",
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
                            column_name="col1",
                        )
                    ),
                    column_alias="col1",
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
                                    column_name="col2",
                                )
                            ),
                            column_alias="col0",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="joined_source_table",
                                    column_name="col3",
                                )
                            ),
                            column_alias="col1",
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


def test_prune_str_expr(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    string_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where a string expr in a node results in the parent being pruned properly."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=string_select_statement,
        plan_id="before_pruning",
    )

    column_pruned_select_node = column_pruner.optimize(string_select_statement)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=column_pruned_select_node,
        plan_id="after_pruning",
    )


@pytest.fixture
def grandparent_pruning_select_statement() -> SqlSelectStatementNode:
    """Test case testing pruning of grandparents in a select statement.

    The string expression for col0 does not have "used_columns" set, so the pruning path is different from the previous
    case.

    -- src2
    SELECT
      col0 AS col0
    FROM (
      -- src1
      SELECT
        src1.col0 AS col0
        , src1.col1 AS col1
      FROM (
        -- from_source1
        SELECT
          src0.col0 AS col0
          , src0.col1 AS col1
          , src0.col2 AS col2
        FROM demo.src0 src0
      ) src1
    ) src2
    """
    return SqlSelectStatementNode.create(
        description="src2",
        select_columns=(
            SqlSelectColumn(
                expr=SqlStringExpression.create(sql_expr="col0"),
                column_alias="col0",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="src1",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(
                            table_alias="src1",
                            column_name="col0",
                        )
                    ),
                    column_alias="col0",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(
                            table_alias="src1",
                            column_name="col1",
                        )
                    ),
                    column_alias="col1",
                ),
            ),
            from_source=SqlSelectStatementNode.create(
                description="src0",
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
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(
                                table_alias="src0",
                                column_name="col2",
                            )
                        ),
                        column_alias="col2",
                    ),
                ),
                from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="src0")),
                from_source_alias="src0",
            ),
            from_source_alias="src1",
        ),
        from_source_alias="src2",
    )


def test_prune_grandparents(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    grandparent_pruning_select_statement: SqlPlanNode,
) -> None:
    """Tests a case where a string expr in a node prevents the parent from being pruned, but prunes grandparents."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=grandparent_pruning_select_statement,
        plan_id="before_pruning",
    )

    column_pruned_select_node = column_pruner.optimize(grandparent_pruning_select_statement)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=column_pruned_select_node,
        plan_id="after_pruning",
    )


@pytest.fixture
def join_grandparent_pruning_select_statement() -> SqlSelectStatementNode:
    """SELECT statement used to build test cases.

    In the initial case, all columns are selected from both from_source and joined_source. Test cases will generally
    change the selected columns to see if the sources are pruned appropriately.

    -- src4
    SELECT
      col0 AS col0
    FROM src3
    JOIN (
      -- joined_source
      SELECT
        src1.col0 AS col0
        sc1.join_col AS join_col
      FROM (
        SELECT
          src0.col0 AS col0
          , src0.col1 AS col1
          , src0.join_col AS join_col
        FROM src0
      ) src1
    ) src4
    ON
      src3.join_col = src4.join_col
    """
    return SqlSelectStatementNode.create(
        description="4",
        select_columns=(
            SqlSelectColumn(
                expr=SqlStringExpression.create(sql_expr="col0"),
                column_alias="col0",
            ),
        ),
        from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="from_source_table")),
        from_source_alias="src3",
        join_descs=(
            SqlJoinDescription(
                right_source=SqlSelectStatementNode.create(
                    description="src1",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="src1",
                                    column_name="col0",
                                )
                            ),
                            column_alias="col0",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(
                                    table_alias="src1",
                                    column_name="join_col",
                                )
                            ),
                            column_alias="join_col",
                        ),
                    ),
                    from_source=SqlSelectStatementNode.create(
                        description="src0",
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
                            SqlSelectColumn(
                                expr=SqlColumnReferenceExpression.create(
                                    col_ref=SqlColumnReference(
                                        table_alias="src0",
                                        column_name="join_col",
                                    )
                                ),
                                column_alias="join_col",
                            ),
                        ),
                        from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="src0")),
                        from_source_alias="src0",
                    ),
                    from_source_alias="src1",
                ),
                right_source_alias="src4",
                on_condition=SqlComparisonExpression.create(
                    left_expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="src3", column_name="join_col")
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="src4", column_name="join_col")
                    ),
                ),
                join_type=SqlJoinType.INNER,
            ),
        ),
    )


def test_prune_grandparents_in_join_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
    join_grandparent_pruning_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests pruning grandparents of a join query."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=join_grandparent_pruning_select_statement,
        plan_id="before_pruning",
    )

    column_pruned_select_node = column_pruner.optimize(join_grandparent_pruning_select_statement)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=column_pruned_select_node,
        plan_id="after_pruning",
    )


def test_prune_distinct_select(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_pruner: SqlColumnPrunerOptimizer,
) -> None:
    """Test that distinct select node shouldn't be pruned."""
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

    logger.debug(LazyFormat("Pruning select statement", select_statement=select_node.structure_text()))

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=select_node,
        plan_id="before_pruning",
    )

    column_pruner.optimize(select_node)
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=select_node,
        plan_id="after_pruning",
    )
