from __future__ import annotations

from abc import ABC, abstractmethod

from metricflow.sql.sql_plan import SqlQueryPlanNode


class SqlQueryPlanOptimizer(ABC):
    """Optimize the SQL query plan in some way.

    e.g. a column pruner that removes unnecessary select columns in sub-queries.
    """

    @abstractmethod
    def optimize(self, node: SqlQueryPlanNode) -> SqlQueryPlanNode:  # noqa :D
        pass
