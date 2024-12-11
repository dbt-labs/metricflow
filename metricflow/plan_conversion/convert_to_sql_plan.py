from __future__ import annotations

from dataclasses import dataclass

from metricflow_semantics.instances import InstanceSet

from metricflow.sql.sql_plan import SqlPlan


@dataclass(frozen=True)
class ConvertToSqlPlanResult:
    """Result object for returning the results of converting to a `SqlQueryPlan`."""

    instance_set: InstanceSet
    sql_plan: SqlPlan
