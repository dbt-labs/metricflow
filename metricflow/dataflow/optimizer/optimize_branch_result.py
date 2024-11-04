from __future__ import annotations

import logging
from dataclasses import dataclass

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OptimizeBranchResult:
    """Result object that represents an optimized branch in the dataflow DAG."""

    optimized_branch: DataflowPlanNode
