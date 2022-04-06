from _pytest.fixtures import FixtureRequest
from typing import List
from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_exprs import (
    SqlCastToTimestampExpression,
    SqlStringLiteralExpression,
)
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
    SqlTableFromClauseNode,
    SqlSelectStatementNode,
    SqlOrderByDescription,
    SqlJoinDescription,
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

    from_source = from_source
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
