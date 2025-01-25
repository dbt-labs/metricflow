from __future__ import annotations

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
    SqlStringLiteralExpression,
)
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.sql.optimizer.rewriting_sub_query_reducer import SqlRewritingSubQueryReducer
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlOrderByDescription, SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.compare_sql_plan import assert_default_rendered_sql_equal


@pytest.fixture
def base_select_statement() -> SqlSelectStatementNode:
    """SELECT statement used to build test cases.

    -- src3
    SELECT
      SUM(src2.col0) AS bookings
      src2.col1 AS ds
      -- src2
      SELECT
        src1.bookings AS bookings
        , src1.ds AS ds
      FROM (
        -- src1
        SELECT
          1 AS bookings
          , src0.ds AS ds
        FROM demo.fct_bookings src0
        LIMIT 2
      ) src1
      WHERE src1.ds >= "2020-01-01"
      LIMIT 1
    ) src2
    WHERE src2.ds <= "2020-01-05"
    GROUP BY src2.ds
    ORDER BY src2.ds
    """
    return SqlSelectStatementNode.create(
        description="src3",
        select_columns=(
            SqlSelectColumn(
                expr=SqlAggregateFunctionExpression.create(
                    sql_function=SqlFunction.SUM,
                    sql_function_args=[
                        SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="src2", column_name="bookings")
                        )
                    ],
                ),
                column_alias="bookings",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src2", column_name="ds")
                ),
                column_alias="ds",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
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
                limit=2,
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
            limit=1,
        ),
        from_source_alias="src2",
        group_bys=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias="src2",
                        column_name="ds",
                    )
                ),
                column_alias="ds",
            ),
        ),
        where=SqlComparisonExpression.create(
            left_expr=SqlColumnReferenceExpression.create(
                SqlColumnReference(
                    table_alias="src2",
                    column_name="ds",
                )
            ),
            comparison=SqlComparison.LESS_THAN_OR_EQUALS,
            right_expr=SqlStringLiteralExpression.create("2020-01-05"),
        ),
        order_bys=(
            SqlOrderByDescription(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias="src2",
                        column_name="ds",
                    )
                ),
                desc=False,
            ),
        ),
    )


def test_reduce_sub_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where an outer query should be reduced into its inner query with merged LIMIT expressions."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=base_select_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlRewritingSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=sub_query_reducer.optimize(base_select_statement),
        plan_id="after_reducing",
    )


@pytest.fixture
def join_select_statement() -> SqlSelectStatementNode:
    """SELECT statement with a join used to build test cases.

    SELECT
      SUM(bookings_src.bookings) AS bookings
      listings_src.country_latest AS listing__country_latest
      bookings_src.ds AS ds
    FROM (
      SELECT
        fct_bookings_src.booking AS bookings
        , 1 AS ds
        , fct_bookings_src.listing_id AS listing
      FROM demo.fct_bookings fct_bookings_src
      WHERE fct_bookings_src.ds >= "2020-01-01"
    ) bookings_src
    JOIN (
      SELECT
        dim_listings_src.country_latest AS country_latest
        , dim_listings_src.listing_id AS listing
      FROM demo.dim_listings dim_listings_src
    ) listings_src
    ON bookings_src.listing = listings_src.listing
    GROUP BY bookings_src.ds
    """
    return SqlSelectStatementNode.create(
        description="query",
        select_columns=(
            SqlSelectColumn(
                expr=SqlAggregateFunctionExpression.create(
                    sql_function=SqlFunction.SUM,
                    sql_function_args=[
                        SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="bookings_src", column_name="bookings")
                        )
                    ],
                ),
                column_alias="bookings",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="listings_src", column_name="country_latest")
                ),
                column_alias="listing__country_latest",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="bookings_src", column_name="ds")
                ),
                column_alias="ds",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="bookings_src",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="fct_bookings_src", column_name="booking")
                    ),
                    column_alias="bookings",
                ),
                SqlSelectColumn(
                    expr=SqlStringExpression.create(sql_expr="1", requires_parenthesis=False, used_columns=()),
                    column_alias="ds",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="fct_bookings_src", column_name="listing_id")
                    ),
                    column_alias="listing",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="fct_bookings_src",
        ),
        from_source_alias="bookings_src",
        join_descs=(
            SqlJoinDescription(
                right_source=SqlSelectStatementNode.create(
                    description="listings_src",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="dim_listings_src", column_name="country")
                            ),
                            column_alias="country_latest",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="dim_listings_src", column_name="listing_id")
                            ),
                            column_alias="listing",
                        ),
                    ),
                    from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="dim_listings")),
                    from_source_alias="dim_listings_src",
                ),
                right_source_alias="listings_src",
                on_condition=SqlComparisonExpression.create(
                    left_expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(table_alias="bookings_src", column_name="listing"),
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(table_alias="listings_src", column_name="listing"),
                    ),
                ),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        group_bys=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias="bookings_src",
                        column_name="ds",
                    )
                ),
                column_alias="ds",
            ),
        ),
        where=SqlComparisonExpression.create(
            left_expr=SqlColumnReferenceExpression.create(
                SqlColumnReference(
                    table_alias="bookings_src",
                    column_name="ds",
                )
            ),
            comparison=SqlComparison.LESS_THAN_OR_EQUALS,
            right_expr=SqlStringLiteralExpression.create("2020-01-05"),
        ),
    )


def test_reduce_join(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    join_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where reducing occurs on a JOIN."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=join_select_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlRewritingSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=sub_query_reducer.optimize(join_select_statement),
        plan_id="after_reducing",
    )


@pytest.fixture
def colliding_select_statement() -> SqlSelectStatementNode:
    """SELECT statement that would cause a collision of table aliases if reduced.

    SELECT
      SUM(bookings_src.bookings) AS bookings
      listings_src.country_latest AS listing__country_latest
      bookings_src.ds AS ds
    FROM (
      SELECT
        colliding_alias.booking AS bookings
        , colliding_alias.ds AS ds
        , colliding_alias.listing_id AS listing
      FROM demo.fct_bookings colliding_alias
      WHERE fct_bookings_src.ds >= "2020-01-01"
    ) bookings_src
    JOIN (
      SELECT
        colliding_alias.country_latest AS country_latest
        , colliding_alias.listing_id AS listing
      FROM demo.dim_listings colliding_alias
    ) listings_src
    ON bookings_src.listing = listings_src.listing
    GROUP BY bookings_src.ds
    """
    return SqlSelectStatementNode.create(
        description="query",
        select_columns=(
            SqlSelectColumn(
                expr=SqlAggregateFunctionExpression.create(
                    sql_function=SqlFunction.SUM,
                    sql_function_args=[
                        SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="bookings_src", column_name="bookings")
                        )
                    ],
                ),
                column_alias="bookings",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="listings_src", column_name="listing__country_latest")
                ),
                column_alias="listing__country_latest",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="bookings_src", column_name="ds")
                ),
                column_alias="ds",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="bookings_src",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="colliding_alias", column_name="booking")
                    ),
                    column_alias="bookings",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="colliding_alias", column_name="ds")
                    ),
                    column_alias="ds",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="colliding_alias", column_name="listing_id")
                    ),
                    column_alias="listing",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="colliding_alias",
        ),
        from_source_alias="bookings_src",
        join_descs=(
            SqlJoinDescription(
                right_source=SqlSelectStatementNode.create(
                    description="listings_src",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="colliding_alias", column_name="country")
                            ),
                            column_alias="country_latest",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="colliding_alias", column_name="listing_id")
                            ),
                            column_alias="listing",
                        ),
                    ),
                    from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="dim_listings")),
                    from_source_alias="colliding_alias",
                ),
                right_source_alias="listings_src",
                on_condition=SqlComparisonExpression.create(
                    left_expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(table_alias="bookings_src", column_name="listing"),
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(table_alias="listings_src", column_name="listing"),
                    ),
                ),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        group_bys=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias="bookings_src",
                        column_name="ds",
                    )
                ),
                column_alias="ds",
            ),
        ),
        where=SqlComparisonExpression.create(
            left_expr=SqlColumnReferenceExpression.create(
                SqlColumnReference(
                    table_alias="bookings_src",
                    column_name="ds",
                )
            ),
            comparison=SqlComparison.LESS_THAN_OR_EQUALS,
            right_expr=SqlStringLiteralExpression.create("2020-01-05"),
        ),
    )


def test_colliding_alias(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    colliding_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where reducing occurs on a JOIN."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=colliding_select_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlRewritingSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=sub_query_reducer.optimize(colliding_select_statement),
        plan_id="after_reducing",
    )


@pytest.fixture
def reduce_all_join_select_statement() -> SqlSelectStatementNode:
    """SELECT statement with a join where all sources can be reduced.

    SELECT
      SUM(bookings_src.bookings) AS bookings
      listings_src1.country_latest AS listing__country_latest
      listings_src2.capacity_latest AS listing__capacity_latest
      bookings_src.ds AS ds
    FROM (
      SELECT
        fct_bookings_src.booking AS bookings
        , fct_bookings_src.ds AS ds
        , fct_bookings_src.listing_id AS listing
      FROM demo.fct_bookings fct_bookings_src
      WHERE fct_bookings_src.ds >= "2020-01-01"
    ) bookings_src
    JOIN (
      SELECT
        dim_listings_src1.country_latest AS country_latest
        , dim_listings_src1.listing_id AS listing
      FROM demo.dim_listings dim_listings_src1
    ) listings_src1
    ON bookings_src.listing = listings_src.listing
    JOIN (
      SELECT
        dim_listings_src2.country_latest AS capacity_latest
        , dim_listings_src2.listing_id AS listing
      FROM demo.dim_listings dim_listings_src2
    ) listings_src2
    ON listing_src1.listing = listings_src2.listing
    GROUP BY bookings_src.ds, listings_src1.country_latest, listings_src2.capacity_latest
    """
    return SqlSelectStatementNode.create(
        description="query",
        select_columns=(
            SqlSelectColumn(
                expr=SqlAggregateFunctionExpression.create(
                    sql_function=SqlFunction.SUM,
                    sql_function_args=[
                        SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="bookings_src", column_name="bookings")
                        )
                    ],
                ),
                column_alias="bookings",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="listings_src1", column_name="country_latest")
                ),
                column_alias="listing__country_latest",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="listings_src2", column_name="capacity_latest")
                ),
                column_alias="listing__capacity_latest",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="bookings_src", column_name="ds")
                ),
                column_alias="ds",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="bookings_src",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="fct_bookings_src", column_name="booking")
                    ),
                    column_alias="bookings",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="fct_bookings_src", column_name="ds")
                    ),
                    column_alias="ds",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="fct_bookings_src", column_name="listing_id")
                    ),
                    column_alias="listing",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="fct_bookings_src",
        ),
        from_source_alias="bookings_src",
        join_descs=(
            SqlJoinDescription(
                right_source=SqlSelectStatementNode.create(
                    description="listings_src1",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="dim_listings_src1", column_name="country")
                            ),
                            column_alias="country_latest",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="dim_listings_src1", column_name="listing_id")
                            ),
                            column_alias="listing",
                        ),
                    ),
                    from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="dim_listings")),
                    from_source_alias="dim_listings_src1",
                ),
                right_source_alias="listings_src1",
                on_condition=SqlComparisonExpression.create(
                    left_expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(table_alias="bookings_src", column_name="listing"),
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(table_alias="listings_src1", column_name="listing"),
                    ),
                ),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
            SqlJoinDescription(
                right_source=SqlSelectStatementNode.create(
                    description="listings_src2",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="dim_listings_src2", column_name="capacity")
                            ),
                            column_alias="capacity_latest",
                        ),
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="dim_listings_src2", column_name="listing_id")
                            ),
                            column_alias="listing",
                        ),
                    ),
                    from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="dim_listings")),
                    from_source_alias="dim_listings_src2",
                ),
                right_source_alias="listings_src2",
                on_condition=SqlComparisonExpression.create(
                    left_expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(table_alias="listings_src1", column_name="listing"),
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(table_alias="listings_src2", column_name="listing"),
                    ),
                ),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        group_bys=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias="bookings_src",
                        column_name="ds",
                    )
                ),
                column_alias="ds",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias="listings_src1",
                        column_name="country_latest",
                    )
                ),
                column_alias="listing__country_latest",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias="listings_src2",
                        column_name="capacity_latest",
                    )
                ),
                column_alias="listing__capacity_latest",
            ),
        ),
    )


def test_reduce_all_join_sources(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    reduce_all_join_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where reducing occurs all all sources on a JOIN."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=reduce_all_join_select_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlRewritingSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=sub_query_reducer.optimize(reduce_all_join_select_statement),
        plan_id="after_reducing",
    )


@pytest.fixture
def reducing_join_statement() -> SqlSelectStatementNode:
    """SELECT statement used to build test cases.

    -- query
    SELECT
      src2.bookings AS bookings
      , src3.listings AS listings
    FROM (
      -- src2
      SELECT
        SUM(src1.bookings) AS bookings
      FROM (
        -- src1
        SELECT
          1 AS bookings
        FROM demo.fct_bookings src0
      ) src1
    ) src2
    CROSS JOIN (
      -- src4
      SELECT
        SUM(src4.listings) AS listings
      FROM demo.fct_listings src4
    ) src3
    """
    return SqlSelectStatementNode.create(
        description="query",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src2", column_name="bookings")
                ),
                column_alias="bookings",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src3", column_name="listings")
                ),
                column_alias="listings",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="src2",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlAggregateFunctionExpression.create(
                        sql_function=SqlFunction.SUM,
                        sql_function_args=[
                            SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="src1", column_name="bookings")
                            )
                        ],
                    ),
                    column_alias="bookings",
                ),
            ),
            from_source=SqlSelectStatementNode.create(
                description="src1",
                select_columns=(
                    SqlSelectColumn(
                        expr=SqlStringExpression.create(sql_expr="1", requires_parenthesis=False, used_columns=()),
                        column_alias="bookings",
                    ),
                ),
                from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
                from_source_alias="src0",
            ),
            from_source_alias="src1",
        ),
        from_source_alias="src2",
        join_descs=(
            SqlJoinDescription(
                right_source=SqlSelectStatementNode.create(
                    description="src4",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlAggregateFunctionExpression.create(
                                sql_function=SqlFunction.SUM,
                                sql_function_args=[
                                    SqlColumnReferenceExpression.create(
                                        col_ref=SqlColumnReference(table_alias="src4", column_name="listings")
                                    )
                                ],
                            ),
                            column_alias="listings",
                        ),
                    ),
                    from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_listings")),
                    from_source_alias="src4",
                ),
                right_source_alias="src3",
                on_condition=None,
                join_type=SqlJoinType.CROSS_JOIN,
            ),
        ),
    )


def test_reducing_join_statement(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    reducing_join_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where a join query should not reduce an aggregate."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=reducing_join_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlRewritingSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=sub_query_reducer.optimize(reducing_join_statement),
        plan_id="after_reducing",
    )


@pytest.fixture
def reducing_join_left_node_statement() -> SqlSelectStatementNode:
    """SELECT statement used to build test cases.

    -- query
    SELECT
      src2.bookings AS bookings
      , src3.listings AS listings
    FROM (
      -- src2
      SELECT
        SUM(src1.bookings) AS bookings
      FROM (
        -- src1
        SELECT
          1 AS bookings
        FROM demo.fct_bookings src0
      ) src1
    ) src2
    CROSS JOIN (
      -- src4
      SELECT
        SUM(src4.listings) AS listings
      FROM demo.fct_listings src4
    ) src3
    """
    return SqlSelectStatementNode.create(
        description="query",
        select_columns=(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src2", column_name="bookings")
                ),
                column_alias="bookings",
            ),
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    col_ref=SqlColumnReference(table_alias="src3", column_name="listings")
                ),
                column_alias="listings",
            ),
        ),
        from_source=SqlSelectStatementNode.create(
            description="src4",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlAggregateFunctionExpression.create(
                        sql_function=SqlFunction.SUM,
                        sql_function_args=[
                            SqlColumnReferenceExpression.create(
                                col_ref=SqlColumnReference(table_alias="src4", column_name="listings")
                            )
                        ],
                    ),
                    column_alias="listings",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_listings")),
            from_source_alias="src4",
        ),
        from_source_alias="src2",
        join_descs=(
            SqlJoinDescription(
                right_source=SqlSelectStatementNode.create(
                    description="src2",
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlAggregateFunctionExpression.create(
                                sql_function=SqlFunction.SUM,
                                sql_function_args=[
                                    SqlColumnReferenceExpression.create(
                                        col_ref=SqlColumnReference(table_alias="src1", column_name="bookings")
                                    )
                                ],
                            ),
                            column_alias="bookings",
                        ),
                    ),
                    from_source=SqlSelectStatementNode.create(
                        description="src1",
                        select_columns=(
                            SqlSelectColumn(
                                expr=SqlStringExpression.create(
                                    sql_expr="1", requires_parenthesis=False, used_columns=()
                                ),
                                column_alias="bookings",
                            ),
                        ),
                        from_source=SqlTableNode.create(
                            sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")
                        ),
                        from_source_alias="src0",
                    ),
                    from_source_alias="src1",
                ),
                right_source_alias="src3",
                on_condition=None,
                join_type=SqlJoinType.CROSS_JOIN,
            ),
        ),
    )


def test_reducing_join_left_node_statement(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    reducing_join_left_node_statement: SqlSelectStatementNode,
) -> None:
    """Tests a case where a join query should not reduced an aggregate."""
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=reducing_join_left_node_statement,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlRewritingSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=sub_query_reducer.optimize(reducing_join_left_node_statement),
        plan_id="after_reducing",
    )


def test_rewriting_distinct_select_node_is_not_reduced(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests to ensure distinct select node doesn't get overwritten."""
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
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=select_node,
        plan_id="before_reducing",
    )

    sub_query_reducer = SqlRewritingSubQueryReducer()

    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=sub_query_reducer.optimize(select_node),
        plan_id="after_reducing",
    )
