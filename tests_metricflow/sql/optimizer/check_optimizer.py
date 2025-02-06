from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal

from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlPlanOptimizer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_plan import SqlPlan, SqlSelectStatementNode

logger = logging.getLogger(__name__)


def assert_optimizer_result_snapshot_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    optimizer: SqlPlanOptimizer,
    sql_plan_renderer: SqlQueryPlanRenderer,
    select_statement: SqlSelectStatementNode,
) -> None:
    """Helper to assert that the SQL snapshot of the optimizer result is the same as the stored one."""
    sql_before_optimizing = sql_plan_renderer.render_sql_query_plan(SqlPlan(select_statement)).sql
    logger.debug(
        LazyFormat(
            "Optimizing SELECT statement",
            select_statement=select_statement.structure_text(),
            sql_before_optimizing=sql_before_optimizing,
        )
    )

    column_pruned_select_node = optimizer.optimize(select_statement)
    sql_after_optimizing = sql_plan_renderer.render_sql_query_plan(SqlPlan(column_pruned_select_node)).sql
    logger.debug(
        LazyFormat(
            "Optimized SQL",
            sql_before_optimizing=sql_before_optimizing,
            sql_after_optimizing=sql_after_optimizing,
        )
    )
    snapshot_str = "\n".join(
        [
            "optimizer:",
            indent(optimizer.__class__.__name__),
            "",
            "sql_before_optimizing:",
            indent(sql_before_optimizing),
            "",
            "sql_after_optimizing:",
            indent(sql_after_optimizing),
        ]
    )
    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="result",
        snapshot_str=snapshot_str,
    )
