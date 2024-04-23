from __future__ import annotations

from dataclasses import dataclass

from metricflow.execution.execution_plan import ExecutionPlan
from metricflow.plan_conversion.convert_to_sql_plan import ConvertToSqlPlanResult
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderResult


@dataclass(frozen=True)
class ConvertToExecutionPlanResult:
    """A result object for returning the results of converting a dataflow plan into an execution plan."""

    convert_to_sql_plan_result: ConvertToSqlPlanResult
    render_sql_result: SqlPlanRenderResult
    execution_plan: ExecutionPlan
