from __future__ import annotations

from enum import Enum
from typing import FrozenSet, List, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.optimizer.dataflow_plan_optimizer import DataflowPlanOptimizer
from metricflow.dataflow.optimizer.predicate_pushdown_optimizer import PredicatePushdownOptimizer


class DataflowPlanOptimization(Enum):
    """Enumeration of optimization types available for execution.

    Values indicate order of application. We apply the source scan optimizer first because it reduces input branches,
    making for maximally parsimonious queries prior to application of predicate pushdown. Note this is safe only
    because the SourceScanOptimizer combines from the CombineAggregatedOutputNode, and will only combine branches
    from there to source if they are functionally identical (i.e., they have all of the same WhereConstraintNode
    configurations).
    """

    PREDICATE_PUSHDOWN = 1

    @staticmethod
    def all_optimizations() -> FrozenSet[DataflowPlanOptimization]:
        """Convenience method for getting a set of all available optimizations."""
        return frozenset((DataflowPlanOptimization.PREDICATE_PUSHDOWN,))

    @staticmethod
    def enabled_optimizations() -> FrozenSet[DataflowPlanOptimization]:
        """Set of DataflowPlanOptimization that are currently enabled.

        Predicate pushdown optimizer is currently disabled.
        """
        return frozenset()


class DataflowPlanOptimizerFactory:
    """Factory class for initializing an enumerated set of optimizers.

    This allows us to centralize initialization and, most importantly, share class instances with cached high cost
    processing between the DataflowPlanBuilder and the optimizer instances requiring that functionality.
    """

    def __init__(self, node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver) -> None:
        """Initializer.

        This collects all initialization requirements for the optimizers that this manages.
        """
        self._node_data_set_resolver = node_data_set_resolver

    def get_optimizers(self, optimizations: FrozenSet[DataflowPlanOptimization]) -> Sequence[DataflowPlanOptimizer]:
        """Initializes and returns a sequence of optimizers matching the input optimization request."""
        optimizers: List[DataflowPlanOptimizer] = []
        for optimization in sorted(list(optimizations), key=lambda x: x.value):
            if optimization is DataflowPlanOptimization.PREDICATE_PUSHDOWN:
                optimizers.append(PredicatePushdownOptimizer(self._node_data_set_resolver))
            else:
                assert_values_exhausted(optimization)

        return tuple(optimizers)
