from __future__ import annotations

from typing import Sequence

from metricflow.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow.dag.mf_dag import DisplayedProperty
from metricflow.dataflow.dataflow_plan import BaseOutput, DataflowPlanNode, DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.aggregate_measures import AggregatedMeasuresOutput
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.visitor import VisitorOutputT


class ConstrainTimeRangeNode(AggregatedMeasuresOutput, BaseOutput):
    """Constrains the time range of the input data set.

    For example, if the input data set had "sales by date", then this would restrict the data set so that it only
    includes sales for a specific range of dates.
    """

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput,
        time_range_constraint: TimeRangeConstraint,
    ) -> None:
        self._time_range_constraint = time_range_constraint
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D
        return StaticIdPrefix.DATAFLOW_NODE_CONSTRAIN_TIME_RANGE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_constrain_time_range_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return (
            f"Constrain Time Range to [{self.time_range_constraint.start_time.isoformat()}, "
            f"{self.time_range_constraint.end_time.isoformat()}]"
        )

    @property
    def time_range_constraint(self) -> TimeRangeConstraint:  # noqa: D
        return self._time_range_constraint

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D
        return tuple(super().displayed_properties) + (
            DisplayedProperty("time_range_start", self.time_range_constraint.start_time.isoformat()),
            DisplayedProperty("time_range_end", self.time_range_constraint.end_time.isoformat()),
        )

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__) and self.time_range_constraint == other_node.time_range_constraint

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> ConstrainTimeRangeNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return ConstrainTimeRangeNode(
            parent_node=new_parent_nodes[0],
            time_range_constraint=self.time_range_constraint,
        )
