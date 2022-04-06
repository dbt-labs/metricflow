"""This module helps to figure out the computational cost for executing a dataflow plan.

There may be multiple possible dataflow plans to realize a set of measures and dimensions (or rather any set of metric
definition instances) because data sets could include an overlapping set of measures and dimensions.

Knowing the cost of a dataflow plan can be used to order the possible plans for optimal execution.
"""


from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Sequence, Generic

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
    SourceDataSetT,
    DataflowPlanNodeVisitor,
    FilterElementsNode,
    WriteToResultDataframeNode,
    OrderByLimitNode,
    ComputeMetricsNode,
    AggregateMeasuresNode,
    JoinAggregatedMeasuresByGroupByColumnsNode,
    JoinOverTimeRangeNode,
    JoinToBaseOutputNode,
    ReadSqlSourceNode,
    WhereConstraintNode,
    CombineMetricsNode,
    ConstrainTimeRangeNode,
    WriteToResultTableNode,
)


class DataflowPlanNodeCost(ABC):
    """Represents the cost to compute the data flow up to a given node."""

    def __lt__(self, other: Any) -> bool:  # type: ignore
        """Implement < so that lists with this can be sorted."""
        if not isinstance(other, DataflowPlanNodeCost):
            return NotImplemented
        return self.as_int < other.as_int

    @property
    @abstractmethod
    def as_int(self) -> int:
        """The cost as an integer for ordering."""
        pass


@dataclass(frozen=True)
class DefaultCost(DataflowPlanNodeCost):
    """Simple cost model where the cost is the number joins * 10 + the number of aggregations"""

    num_joins: int = 0
    num_aggregations: int = 0

    @property
    def as_int(self) -> int:  # noqa: D
        return self.num_joins * 10 + self.num_aggregations

    @staticmethod
    def sum(costs: Sequence[DefaultCost]) -> DefaultCost:  # noqa: D
        return DefaultCost(
            num_joins=sum([x.num_joins for x in costs]),
            num_aggregations=sum([x.num_aggregations for x in costs]),
        )


class DataflowPlanNodeCostFunction(Generic[SourceDataSetT], ABC):
    """A function that calculates the cost for computing the dataflow up to a given node."""

    @abstractmethod
    def calculate_cost(self, node: DataflowPlanNode[SourceDataSetT]) -> DataflowPlanNodeCost:
        """Return the cost for calculating the given dataflow up to the given node."""
        pass


class DefaultCostFunction(
    Generic[SourceDataSetT],
    DataflowPlanNodeCostFunction[SourceDataSetT],
    DataflowPlanNodeVisitor[SourceDataSetT, DefaultCost],
):
    """Cost function using the default cost."""

    def calculate_cost(self, node: DataflowPlanNode[SourceDataSetT]) -> DataflowPlanNodeCost:  # noqa: D
        return node.accept(self)

    def visit_source_node(self, node: ReadSqlSourceNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        # Base case.
        return DefaultCost(num_joins=0, num_aggregations=0)

    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        parent_costs = [x.accept(self) for x in node.parent_nodes]

        # Add number of joins to the cost.
        node_cost = DefaultCost(num_joins=len(node.join_targets))
        return DefaultCost.sum(parent_costs + [node_cost])

    def visit_join_aggregated_measures_by_groupby_columns_node(  # noqa: D
        self, node: JoinAggregatedMeasuresByGroupByColumnsNode[SourceDataSetT]
    ) -> DefaultCost:
        parent_costs = [x.accept(self) for x in node.parent_nodes]

        # This node does N-1 joins to link its N parents together
        num_joins = len(node.parent_nodes) - 1
        node_cost = DefaultCost(num_joins=num_joins)
        return DefaultCost.sum(parent_costs + [node_cost])

    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        parent_costs = [x.accept(self) for x in node.parent_nodes]

        # Add the number of aggregations to the cost
        node_cost = DefaultCost(num_aggregations=1)
        return DefaultCost.sum(parent_costs + [node_cost])

    def visit_compute_metrics_node(self, node: ComputeMetricsNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        return DefaultCost.sum([x.accept(self) for x in node.parent_nodes])

    def visit_order_by_limit_node(self, node: OrderByLimitNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        return DefaultCost.sum([x.accept(self) for x in node.parent_nodes])

    def visit_where_constraint_node(self, node: WhereConstraintNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        return DefaultCost.sum([x.accept(self) for x in node.parent_nodes])

    def visit_write_to_result_dataframe_node(  # noqa: D
        self, node: WriteToResultDataframeNode[SourceDataSetT]
    ) -> DefaultCost:
        return DefaultCost.sum([x.accept(self) for x in node.parent_nodes])

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        return DefaultCost.sum([x.accept(self) for x in node.parent_nodes])

    def visit_pass_elements_filter_node(self, node: FilterElementsNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        return DefaultCost.sum([x.accept(self) for x in node.parent_nodes])

    def visit_combine_metrics_node(self, node: CombineMetricsNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        return DefaultCost.sum([x.accept(self) for x in node.parent_nodes])

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        return DefaultCost.sum([x.accept(self) for x in node.parent_nodes])

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode[SourceDataSetT]) -> DefaultCost:  # noqa: D
        parent_costs = [x.accept(self) for x in node.parent_nodes]

        # Add the number of aggregations to the cost (eg 1 per unit time)
        node_cost = DefaultCost(num_aggregations=1)
        return DefaultCost.sum(parent_costs + [node_cost])
