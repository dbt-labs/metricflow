from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.sql.optimizer.table_alias_simplifier import SqlTableAliasSimplifier
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer, SqlQueryPlanRenderer
from metricflow.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
)
from metricflow.sql.sql_plan import (
    SqlJoinDescription,
    SqlSelectColumn,
    SqlSelectStatementNode,
    SqlTableNode,
)
from tests_metricflow.sql.compare_sql_plan import assert_default_rendered_sql_equal
from tests_metricflow.sql.optimizer.check_optimizer import assert_optimizer_result_snapshot_equal


@pytest.fixture
def sql_plan_renderer() -> DefaultSqlQueryPlanRenderer:  # noqa: D103
    return DefaultSqlQueryPlanRenderer()


def test_table_alias_simplification(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_plan_renderer: DefaultSqlQueryPlanRenderer,
    base_select_statement: SqlSelectStatementNode,
) -> None:
    """Tests that table aliases are removed when not needed in CTEs."""

    assert_optimizer_result_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        optimizer=SqlTableAliasSimplifier(),
        sql_plan_renderer=sql_plan_renderer,
        select_statement=base_select_statement,
    )
