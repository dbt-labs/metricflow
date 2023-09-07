from __future__ import annotations

from abc import ABC, abstractmethod

from metricflow.dataflow.dataflow_plan import DataflowPlan


class DataflowPlanOptimizer(ABC):
    """Converts one dataflow plan into another dataflow plan that is more optimal in some way (e.g. performance)."""

    @abstractmethod
    def optimize(self, dataflow_plan: DataflowPlan) -> DataflowPlan:  # noqa: D
        raise NotImplementedError
