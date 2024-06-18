from __future__ import annotations

from typing import Optional, Sequence

from dbt_semantic_interfaces.protocols import MetricTimeWindow
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty, NodeId
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.specs.spec_classes import TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor


class JoinOverTimeRangeNode(DataflowPlanNode):
    """A node that allows for cumulative metric computation by doing a self join across a cumulative date range."""

    def __init__(
        self,
        parent_node: DataflowPlanNode,
        queried_agg_time_dimension_specs: Sequence[TimeDimensionSpec],
        window: Optional[MetricTimeWindow],
        grain_to_date: Optional[TimeGranularity],
        node_id: Optional[NodeId] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> None:
        """Constructor.

        Args:
            parent_node: node with standard output
            window: time window to join over
            grain_to_date: indicates time range should start from the beginning of this time granularity
            (eg month to day)
            node_id: Override the node ID with this value
            time_range_constraint: time range to aggregate over
            queried_agg_time_dimension_specs: time dimension specs that will be selected from time spine table
        """
        if window and grain_to_date:
            raise RuntimeError(
                f"This node cannot be initialized with both window and grain_to_date set. This configuration should "
                f"have been prevented by model validation. window: {window}. grain_to_date: {grain_to_date}."
            )
        self._parent_node = parent_node
        self._grain_to_date = grain_to_date
        self._window = window
        self.time_range_constraint = time_range_constraint
        self.queried_agg_time_dimension_specs = queried_agg_time_dimension_specs

        # Doing a list comprehension throws a type error, so doing it this way.
        parent_nodes: Sequence[DataflowPlanNode] = (self._parent_node,)
        super().__init__(node_id=node_id or self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_SELF_OVER_TIME_RANGE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_join_over_time_range_node(self)

    @property
    def grain_to_date(self) -> Optional[TimeGranularity]:  # noqa: D102
        return self._grain_to_date

    @property
    def description(self) -> str:  # noqa: D102
        return """Join Self Over Time Range"""

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self._parent_node

    @property
    def window(self) -> Optional[MetricTimeWindow]:  # noqa: D102
        return self._window

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
        return JoinOverTimeRangeNode(
            parent_node=new_parent_nodes[0],
            window=self.window,
            grain_to_date=self.grain_to_date,
            time_range_constraint=self.time_range_constraint,
            queried_agg_time_dimension_specs=self.queried_agg_time_dimension_specs,
        )
