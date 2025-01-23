from __future__ import annotations

import logging
from typing import List

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.sql.sql_exprs import (
    SqlAggregateFunctionExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
    SqlFunction,
    SqlStringExpression,
)
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable, SqlTableType
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_ctas_node import SqlCreateTableAsNode
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlOrderByDescription, SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.compare_sql_plan import assert_rendered_sql_equal

logger = logging.getLogger(__name__)


@pytest.mark.sql_engine_snapshot
def test_component_rendering(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Checks that all components of SELECT query are rendered for the 0, 1, >1 component count cases."""
    # Test single SELECT column
    select_columns = [
        SqlSelectColumn(
            expr=SqlAggregateFunctionExpression.create(
                sql_function=SqlFunction.SUM, sql_function_args=[SqlStringExpression.create("1")]
            ),
            column_alias="bookings",
        ),
    ]

    from_source = SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings"))

    from_source = from_source
    from_source_alias = "a"
    joins_descs: List[SqlJoinDescription] = []
    where = None
    group_bys: List[SqlSelectColumn] = []
    order_bys: List[SqlOrderByDescription] = []

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="test0",
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

    # Test multiple select column
    select_columns.extend(
        [
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(SqlColumnReference("b", "country")),
                column_alias="user__country",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(SqlColumnReference("c", "country")),
                column_alias="listing__country",
            ),
        ]
    )

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="test1",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan1",
        sql_client=sql_client,
    )

    # Test single join
    joins_descs.append(
        SqlJoinDescription(
            right_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="dim_users")),
            right_source_alias="b",
            on_condition=SqlComparisonExpression.create(
                left_expr=SqlColumnReferenceExpression.create(SqlColumnReference("a", "user_id")),
                comparison=SqlComparison.EQUALS,
                right_expr=SqlColumnReferenceExpression.create(SqlColumnReference("b", "user_id")),
            ),
            join_type=SqlJoinType.LEFT_OUTER,
        )
    )

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="test2",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan2",
        sql_client=sql_client,
    )

    # Test multiple join
    joins_descs.append(
        SqlJoinDescription(
            right_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="dim_listings")),
            right_source_alias="c",
            on_condition=SqlComparisonExpression.create(
                left_expr=SqlColumnReferenceExpression.create(SqlColumnReference("a", "user_id")),
                comparison=SqlComparison.EQUALS,
                right_expr=SqlColumnReferenceExpression.create(SqlColumnReference("c", "user_id")),
            ),
            join_type=SqlJoinType.LEFT_OUTER,
        )
    )

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="test3",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan3",
        sql_client=sql_client,
    )

    # Test single group by
    group_bys.append(
        SqlSelectColumn(
            expr=SqlColumnReferenceExpression.create(SqlColumnReference("b", "country")),
            column_alias="user__country",
        ),
    )

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="test4",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan4",
        sql_client=sql_client,
    )

    # Test multiple group bys
    group_bys.append(
        SqlSelectColumn(
            expr=SqlColumnReferenceExpression.create(SqlColumnReference("c", "country")),
            column_alias="listing__country",
        ),
    )

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="test5",
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            join_descs=tuple(joins_descs),
            where=where,
            group_bys=tuple(group_bys),
            order_bys=tuple(order_bys),
        ),
        plan_id="plan5",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_render_where(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="test0",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="a", column_name="booking_value")
                    ),
                    column_alias="booking_value",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="a",
            where=SqlComparisonExpression.create(
                left_expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="a", column_name="booking_value")
                ),
                comparison=SqlComparison.GREATER_THAN,
                right_expr=SqlStringExpression.create(
                    sql_expr="100",
                    requires_parenthesis=False,
                    used_columns=(),
                ),
            ),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_render_order_by(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="test0",
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
            order_bys=(
                SqlOrderByDescription(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="a", column_name="booking_value")
                    ),
                    desc=False,
                ),
                SqlOrderByDescription(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="a", column_name="bookings")
                    ),
                    desc=True,
                ),
            ),
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_render_limit(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="test0",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="a", column_name="bookings")
                    ),
                    column_alias="bookings",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="a",
            limit=1,
        ),
        plan_id="plan0",
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_render_create_table_as(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    select_node = SqlSelectStatementNode.create(
        description="select_0",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="a", column_name="bookings")
                ),
                column_alias="bookings",
            ),
        ),
        from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
        from_source_alias="a",
        limit=1,
    )
    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlCreateTableAsNode.create(
            sql_table=SqlTable(
                schema_name="schema_name",
                table_name="table_name",
                table_type=SqlTableType.TABLE,
            ),
            parent_node=select_node,
        ),
        plan_id="create_table_as",
        sql_client=sql_client,
    )
    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlCreateTableAsNode.create(
            sql_table=SqlTable(
                schema_name="schema_name",
                table_name="table_name",
                table_type=SqlTableType.VIEW,
            ),
            parent_node=select_node,
        ),
        plan_id="create_view_as",
        sql_client=sql_client,
    )
