from __future__ import annotations

from enum import Enum
from typing import List, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow.dataflow.optimizer.dataflow_plan_optimizer import DataflowPlanOptimizer
from metricflow.dataflow.optimizer.predicate_pushdown_optimizer import PredicatePushdownOptimizer
from metricflow.dataflow.optimizer.source_scan.source_scan_optimizer import SourceScanOptimizer


class DataflowPlanOptimization(Enum):
    """Enumeration of optimization types available for execution."""

    PREDICATE_PUSHDOWN = "predicate_pushdown"
    SOURCE_SCAN = "source_scan"


class DataflowPlanOptimizerFactory:
    """Factory class for initializing an enumerated set of optimizers.

    This allows us to centralize initialization and, most importantly, share class instances with cached high cost
    processing between the DataflowPlanBuilder and the optimizer instances requiring that functionality.
    """

    def get_optimizers(self, optimizations: Sequence[DataflowPlanOptimization]) -> Sequence[DataflowPlanOptimizer]:
        """Initializes and returns a sequence of optimizers matching the input optimization requests."""
        optimizers: List[DataflowPlanOptimizer] = []
        for optimization in optimizations:
            if optimization is DataflowPlanOptimization.SOURCE_SCAN:
                optimizers.append(SourceScanOptimizer())
            elif optimization is DataflowPlanOptimization.PREDICATE_PUSHDOWN:
                optimizers.append(PredicatePushdownOptimizer())
            else:
                assert_values_exhausted(optimization)

        return tuple(optimizers)
