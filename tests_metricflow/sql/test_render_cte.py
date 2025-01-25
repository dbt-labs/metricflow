from __future__ import annotations

import logging

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

from metricflow.sql.sql_cte_node import SqlCteNode
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.sql.compare_sql_plan import assert_default_rendered_sql_equal

logger = logging.getLogger(__name__)


def test_render_cte(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    assert_default_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_plan_node=SqlSelectStatementNode.create(
            description="cte_test",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="cte_0", column_name="col_0")
                    ),
                    column_alias="col_0",
                ),
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        col_ref=SqlColumnReference(table_alias="cte_1", column_name="col_1")
                    ),
                    column_alias="col_1",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_0")),
            from_source_alias="cte_0",
            join_descs=(
                SqlJoinDescription(
                    right_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name="cte_1")),
                    right_source_alias="cte_1",
                    join_type=SqlJoinType.LEFT_OUTER,
                    on_condition=SqlComparisonExpression.create(
                        left_expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="cte_0", column_name="col_0")
                        ),
                        comparison=SqlComparison.EQUALS,
                        right_expr=SqlColumnReferenceExpression.create(
                            col_ref=SqlColumnReference(table_alias="cte_1", column_name="col_1")
                        ),
                    ),
                ),
            ),
            cte_sources=(
                SqlCteNode.create(
                    select_statement=SqlSelectStatementNode.create(
                        description="cte_select_0",
                        select_columns=(
                            SqlSelectColumn(
                                expr=SqlColumnReferenceExpression.create(
                                    col_ref=SqlColumnReference(table_alias="cte_source_table_0", column_name="col_0")
                                ),
                                column_alias="col_0",
                            ),
                        ),
                        from_source=SqlTableNode.create(
                            sql_table=SqlTable(schema_name="demo", table_name="cte_source_table_0")
                        ),
                        from_source_alias="cte_source_table_0",
                    ),
                    cte_alias="cte_0",
                ),
                SqlCteNode.create(
                    select_statement=SqlSelectStatementNode.create(
                        description="cte_select_1",
                        select_columns=(
                            SqlSelectColumn(
                                expr=SqlColumnReferenceExpression.create(
                                    col_ref=SqlColumnReference(table_alias="cte_source_table_1", column_name="col_1")
                                ),
                                column_alias="col_1",
                            ),
                        ),
                        from_source=SqlTableNode.create(
                            sql_table=SqlTable(schema_name="demo", table_name="cte_source_table_1")
                        ),
                        from_source_alias="cte_source_table_1",
                    ),
                    cte_alias="cte_1",
                ),
            ),
        ),
        plan_id="plan_0",
    )
