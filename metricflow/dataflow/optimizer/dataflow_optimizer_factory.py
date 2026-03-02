from __future__ import annotations

from collections.abc import Set
from enum import Enum
from typing import FrozenSet, List, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow.dataflow.optimizer.dataflow_plan_optimizer import DataflowPlanOptimizer
from metricflow.dataflow.optimizer.source_scan.source_scan_optimizer import SourceScanOptimizer
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor


class DataflowPlanOptimization(Enum):
    """Enumeration of optimization types available for execution.

    Values indicate order of application. Passthrough metric evaluation is applied first as the metric evaluation plan
    is used to generate the initial dataflow plan. The resulting dataflow plan can be fed into the source scan
    optimizer.
    """

    PASSTHROUGH_METRIC_EVALUATION = 0
    SOURCE_SCAN = 1

    @staticmethod
    def all_optimizations() -> FrozenSet[DataflowPlanOptimization]:
        """Convenience method for getting a set of all available optimizations."""
        return frozenset(
            (
                DataflowPlanOptimization.PASSTHROUGH_METRIC_EVALUATION,
                DataflowPlanOptimization.SOURCE_SCAN,
            )
        )

    @staticmethod
    def enabled_optimizations() -> FrozenSet[DataflowPlanOptimization]:
        """Set of DataflowPlanOptimization that are currently enabled.

        Predicate pushdown optimizer is currently disabled.
        """
        return frozenset((DataflowPlanOptimization.SOURCE_SCAN,))


class DataflowPlanOptimizerFactory:
    """Factory class for initializing an enumerated set of optimizers.

    This allows us to centralize initialization and, most importantly, share class instances with cached high cost
    processing between the DataflowPlanBuilder and the optimizer instances requiring that functionality.
    """

    def __init__(self, node_data_set_resolver: DataflowNodeToSqlSubqueryVisitor) -> None:
        """Initializer.

        This collects all of the initialization requirements for the optimizers it manages.
        """
        self._node_data_set_resolver = node_data_set_resolver

    def get_optimizers(self, optimizations: Set[DataflowPlanOptimization]) -> Sequence[DataflowPlanOptimizer]:
        """Initializes and returns a sequence of optimizers matching the input optimization requests."""
        optimizers: List[DataflowPlanOptimizer] = []
        for optimization in sorted(list(optimizations), key=lambda x: x.value):
            if optimization is DataflowPlanOptimization.PASSTHROUGH_METRIC_EVALUATION:
                # This optimization is handled through a separate step in plan generation.
                pass
            elif optimization is DataflowPlanOptimization.SOURCE_SCAN:
                optimizers.append(SourceScanOptimizer())
            else:
                assert_values_exhausted(optimization)

        return tuple(optimizers)
