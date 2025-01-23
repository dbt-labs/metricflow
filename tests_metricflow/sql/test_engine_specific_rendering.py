from __future__ import annotations

from typing import List

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlCastToTimestampExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringExpression,
    SqlStringLiteralExpression,
)
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlOrderByDescription, SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.compare_sql_plan import assert_rendered_sql_equal


@pytest.mark.sql_engine_snapshot
def test_cast_to_timestamp(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the cast to timestamp expression in a query."""
    select_columns = [
        SqlSelectColumn(
            expr=SqlCastToTimestampExpression.create(
                arg=SqlStringLiteralExpression.create(
                    literal_value="2020-01-01",
                )
            ),
            column_alias="col0",
        ),
    ]

    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="Test Cast to Timestamp Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_generate_uuid(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the generate uuid expression in a query."""
    select_columns = [
        SqlSelectColumn(
            expr=SqlGenerateUuidExpression.create(),
            column_alias="uuid",
        ),
    ]
    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="Test Generate UUID Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_continuous_percentile_expr(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the continuous percentile expression in a query."""
    if not sql_client.sql_plan_renderer.expr_renderer.can_render_percentile_function(
        SqlPercentileFunctionType.CONTINUOUS
    ):
        pytest.skip("Warehouse does not support continuous percentile expressions")

    select_columns = [
        SqlSelectColumn(
            expr=SqlPercentileExpression.create(
                order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5, function_type=SqlPercentileFunctionType.CONTINUOUS
                ),
            ),
            column_alias="col0_percentile",
        ),
    ]

    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="Test Continuous Percentile Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_discrete_percentile_expr(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the discrete percentile expression in a query."""
    if not sql_client.sql_plan_renderer.expr_renderer.can_render_percentile_function(
        SqlPercentileFunctionType.DISCRETE
    ):
        pytest.skip("Warehouse does not support discrete percentile expressions")

    select_columns = [
        SqlSelectColumn(
            expr=SqlPercentileExpression.create(
                order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5, function_type=SqlPercentileFunctionType.DISCRETE
                ),
            ),
            column_alias="col0_percentile",
        ),
    ]

    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="Test Discrete Percentile Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_approximate_continuous_percentile_expr(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the approximate continuous percentile expression in a query."""
    if not sql_client.sql_plan_renderer.expr_renderer.can_render_percentile_function(
        SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
    ):
        pytest.skip("Warehouse does not support approximate_continuous percentile expressions")

    select_columns = [
        SqlSelectColumn(
            expr=SqlPercentileExpression.create(
                order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
                ),
            ),
            column_alias="col0_percentile",
        ),
    ]

    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="Test Approximate Continuous Percentile Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_approximate_discrete_percentile_expr(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the approximate discrete percentile expression in a query."""
    if not sql_client.sql_plan_renderer.expr_renderer.can_render_percentile_function(
        SqlPercentileFunctionType.APPROXIMATE_DISCRETE
    ):
        pytest.skip("Warehouse does not support percentile expressions")

    select_columns = [
        SqlSelectColumn(
            expr=SqlPercentileExpression.create(
                order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_DISCRETE
                ),
            ),
            column_alias="col0_percentile",
        ),
    ]

    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="Test Approximate Discrete Percentile Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_add_time_expr(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Tests rendering of the SqlAddTimeExpr in a query."""
    select_columns = [
        SqlSelectColumn(
            expr=SqlAddTimeExpression.create(
                arg=SqlStringLiteralExpression.create(
                    "2020-01-01",
                ),
                count_expr=SqlStringExpression.create(
                    "1",
                ),
                granularity=TimeGranularity.QUARTER,
            ),
            column_alias="add_time",
        ),
    ]

    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="foo", table_name="bar"))
    from_source_alias = "a"

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="Test Add Time Expression",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )
