from __future__ import annotations

from typing import List

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_exprs import (
    SqlCastToTimestampExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringLiteralExpression,
)
from metricflow.sql.sql_plan import (
    SqlJoinDescription,
    SqlOrderByDescription,
    SqlSelectColumn,
    SqlSelectStatementNode,
    SqlTableFromClauseNode,
)
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.sql.compare_sql_plan import assert_rendered_sql_equal


def test_cast_to_timestamp(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the cast to timestamp expression in a query."""
    select_columns = [
        SqlSelectColumn(
            expr=SqlCastToTimestampExpression(
                arg=SqlStringLiteralExpression(
                    literal_value="2020-01-01",
                )
            ),
            column_alias="col0",
        ),
    ]

    from_source = SqlTableFromClauseNode(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        select_node=SqlSelectStatementNode(
            description="Test Cast to Timestamp Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            joins_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


def test_generate_uuid(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the generate uuid expression in a query."""
    select_columns = [
        SqlSelectColumn(
            expr=SqlGenerateUuidExpression(),
            column_alias="uuid",
        ),
    ]
    from_source = SqlTableFromClauseNode(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"

    assert_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        select_node=SqlSelectStatementNode(
            description="Test Generate UUID Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


def test_continuous_percentile_expr(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the continuous percentile expression in a query."""
    if not sql_client.sql_query_plan_renderer.expr_renderer.can_render_percentile_function(
        SqlPercentileFunctionType.CONTINUOUS
    ):
        pytest.skip("Warehouse does not support continuous percentile expressions")

    select_columns = [
        SqlSelectColumn(
            expr=SqlPercentileExpression(
                order_by_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5, function_type=SqlPercentileFunctionType.CONTINUOUS
                ),
            ),
            column_alias="col0_percentile",
        ),
    ]

    from_source = SqlTableFromClauseNode(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        select_node=SqlSelectStatementNode(
            description="Test Continuous Percentile Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            joins_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


def test_discrete_percentile_expr(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the discrete percentile expression in a query."""
    if not sql_client.sql_query_plan_renderer.expr_renderer.can_render_percentile_function(
        SqlPercentileFunctionType.DISCRETE
    ):
        pytest.skip("Warehouse does not support discrete percentile expressions")

    select_columns = [
        SqlSelectColumn(
            expr=SqlPercentileExpression(
                order_by_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5, function_type=SqlPercentileFunctionType.DISCRETE
                ),
            ),
            column_alias="col0_percentile",
        ),
    ]

    from_source = SqlTableFromClauseNode(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        select_node=SqlSelectStatementNode(
            description="Test Discrete Percentile Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            joins_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


def test_approximate_continuous_percentile_expr(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the approximate continuous percentile expression in a query."""
    if not sql_client.sql_query_plan_renderer.expr_renderer.can_render_percentile_function(
        SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
    ):
        pytest.skip("Warehouse does not support approximate_continuous percentile expressions")

    select_columns = [
        SqlSelectColumn(
            expr=SqlPercentileExpression(
                order_by_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
                ),
            ),
            column_alias="col0_percentile",
        ),
    ]

    from_source = SqlTableFromClauseNode(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        select_node=SqlSelectStatementNode(
            description="Test Approximate Continuous Percentile Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            joins_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


def test_approximate_discrete_percentile_expr(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the approximate discrete percentile expression in a query."""
    if not sql_client.sql_query_plan_renderer.expr_renderer.can_render_percentile_function(
        SqlPercentileFunctionType.APPROXIMATE_DISCRETE
    ):
        pytest.skip("Warehouse does not support percentile expressions")

    select_columns = [
        SqlSelectColumn(
            expr=SqlPercentileExpression(
                order_by_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_DISCRETE
                ),
            ),
            column_alias="col0_percentile",
        ),
    ]

    from_source = SqlTableFromClauseNode(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        select_node=SqlSelectStatementNode(
            description="Test Approximate Discrete Percentile Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            joins_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )
