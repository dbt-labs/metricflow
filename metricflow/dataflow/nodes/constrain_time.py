from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.aggregate_simple_metric_inputs import DataflowPlanNode


@dataclass(frozen=True, eq=False)
class ConstrainTimeRangeNode(DataflowPlanNode):
    """Constrains the time range of the input data set.

    For example, if the input data set had "sales by date", then this would restrict the data set so that it only
    includes sales for a specific range of dates.
    """

    time_range_constraint: TimeRangeConstraint

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        time_range_constraint: TimeRangeConstraint,
    ) -> ConstrainTimeRangeNode:
        return ConstrainTimeRangeNode(
            parent_nodes=(parent_node,),
            time_range_constraint=time_range_constraint,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_CONSTRAIN_TIME_RANGE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_constrain_time_range_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return (
            f"Constrain Time Range to [{self.time_range_constraint.start_time.isoformat()}, "
            f"{self.time_range_constraint.end_time.isoformat()}]"
        )

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("time_range_start", self.time_range_constraint.start_time.isoformat()),
            DisplayedProperty("time_range_end", self.time_range_constraint.end_time.isoformat()),
        )

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__) and self.time_range_constraint == other_node.time_range_constraint

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> ConstrainTimeRangeNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return ConstrainTimeRangeNode(
            parent_nodes=tuple(new_parent_nodes),
            time_range_constraint=self.time_range_constraint,
        )
