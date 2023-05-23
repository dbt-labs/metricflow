from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic

from metricflow.dataflow.dataflow_plan import DataflowPlan, SourceDataSetT


class DataflowPlanOptimizer(Generic[SourceDataSetT], ABC):
    """Converts one dataflow plan into another dataflow plan that is more optimal in some way (e.g. performance)."""

    @abstractmethod
    def optimize(self, dataflow_plan: DataflowPlan[SourceDataSetT]) -> DataflowPlan[SourceDataSetT]:  # noqa: D
        raise NotImplementedError
