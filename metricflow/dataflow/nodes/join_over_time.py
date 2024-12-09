from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.protocols import MetricTimeWindow
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class JoinOverTimeRangeNode(DataflowPlanNode):
    """A node that allows for cumulative metric computation by doing a self join across a cumulative date range.

    Attributes:
        queried_agg_time_dimension_specs: Time dimension specs that will be selected from the time spine table.
        window: Time window to join over.
        grain_to_date: Indicates time range should start from the beginning of this time granularity (e.g., month to day).
        time_range_constraint: Time range to aggregate over.
    """

    queried_agg_time_dimension_specs: Tuple[TimeDimensionSpec, ...]
    window: Optional[MetricTimeWindow]
    grain_to_date: Optional[TimeGranularity]
    time_range_constraint: Optional[TimeRangeConstraint]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        queried_agg_time_dimension_specs: Tuple[TimeDimensionSpec, ...],
        window: Optional[MetricTimeWindow] = None,
        grain_to_date: Optional[TimeGranularity] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> JoinOverTimeRangeNode:
        if window and grain_to_date:
            raise RuntimeError(
                f"This node cannot be initialized with both window and grain_to_date set. This configuration should "
                f"have been prevented by model validation. window: {window}. grain_to_date: {grain_to_date}."
            )
        return JoinOverTimeRangeNode(
            parent_nodes=(parent_node,),
            queried_agg_time_dimension_specs=queried_agg_time_dimension_specs,
            window=window,
            grain_to_date=grain_to_date,
            time_range_constraint=time_range_constraint,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_SELF_OVER_TIME_RANGE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_join_over_time_range_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Join Self Over Time Range"""

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        displayed_properties = tuple(super().displayed_properties)
        displayed_properties += (
            DisplayedProperty("queried_agg_time_dimension_specs", self.queried_agg_time_dimension_specs),
        )
        if self.window:
            displayed_properties += (DisplayedProperty("window", self.window),)
        if self.grain_to_date:
            displayed_properties += (DisplayedProperty("grain_to_date", self.grain_to_date),)
        if self.time_range_constraint:
            displayed_properties += (DisplayedProperty("time_range_constraint", self.time_range_constraint),)
        return displayed_properties

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.grain_to_date == self.grain_to_date
            and other_node.window == self.window
            and other_node.time_range_constraint == self.time_range_constraint
            and other_node.queried_agg_time_dimension_specs == self.queried_agg_time_dimension_specs
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> JoinOverTimeRangeNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return JoinOverTimeRangeNode.create(
            parent_node=new_parent_nodes[0],
            window=self.window,
            grain_to_date=self.grain_to_date,
            time_range_constraint=self.time_range_constraint,
            queried_agg_time_dimension_specs=self.queried_agg_time_dimension_specs,
        )
