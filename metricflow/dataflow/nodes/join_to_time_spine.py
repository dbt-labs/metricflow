from __future__ import annotations

from abc import ABC
from typing import Optional, Sequence

from dbt_semantic_interfaces.protocols import MetricTimeWindow
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.specs.spec_classes import TimeDimensionSpec
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor


class JoinToTimeSpineNode(DataflowPlanNode, ABC):
    """Join parent dataset to time spine dataset."""

    def __init__(
        self,
        parent_node: DataflowPlanNode,
        requested_agg_time_dimension_specs: Sequence[TimeDimensionSpec],
        use_custom_agg_time_dimension: bool,
        join_type: SqlJoinType,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        offset_window: Optional[MetricTimeWindow] = None,
        offset_to_grain: Optional[TimeGranularity] = None,
    ) -> None:
        """Constructor.

        Args:
            parent_node: Node that returns desired dataset to join to time spine.
            requested_agg_time_dimension_specs: Time dimensions requested in query.
            use_custom_agg_time_dimension: Indicates if agg_time_dimension should be used in join. If false, uses metric_time.
            time_range_constraint: Time range to constrain the time spine to.
            offset_window: Time window to offset the parent dataset by when joining to time spine.
            offset_to_grain: Granularity period to offset the parent dataset to when joining to time spine.

        Passing both offset_window and offset_to_grain not allowed.
        """
        assert not (
            offset_window and offset_to_grain
        ), "Can't set both offset_window and offset_to_grain when joining to time spine. Choose one or the other."
        assert (
            len(requested_agg_time_dimension_specs) > 0
        ), "Must have at least one value in requested_agg_time_dimension_specs for JoinToTimeSpineNode."

        self._parent_node = parent_node
        self._requested_agg_time_dimension_specs = tuple(requested_agg_time_dimension_specs)
        self._use_custom_agg_time_dimension = use_custom_agg_time_dimension
        self._offset_window = offset_window
        self._offset_to_grain = offset_to_grain
        self._time_range_constraint = time_range_constraint
        self._join_type = join_type

        super().__init__(node_id=self.create_unique_id(), parent_nodes=(self._parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_TO_TIME_SPINE_ID_PREFIX

    @property
    def requested_agg_time_dimension_specs(self) -> Sequence[TimeDimensionSpec]:
        """Time dimension specs to use when creating time spine table."""
        return self._requested_agg_time_dimension_specs

    @property
    def use_custom_agg_time_dimension(self) -> bool:
        """Whether or not metric_time was included in the query."""
        return self._use_custom_agg_time_dimension

    @property
    def time_range_constraint(self) -> Optional[TimeRangeConstraint]:
        """Time range constraint to apply when querying time spine table."""
        return self._time_range_constraint

    @property
    def offset_window(self) -> Optional[MetricTimeWindow]:
        """Time range constraint to apply when querying time spine table."""
        return self._offset_window

    @property
    def offset_to_grain(self) -> Optional[TimeGranularity]:
        """Time range constraint to apply when querying time spine table."""
        return self._offset_to_grain

    @property
    def join_type(self) -> SqlJoinType:
        """Join type to use when joining to time spine."""
        return self._join_type

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_join_to_time_spine_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Join to Time Spine Dataset"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("requested_agg_time_dimension_specs", self._requested_agg_time_dimension_specs),
            DisplayedProperty("use_custom_agg_time_dimension", self._use_custom_agg_time_dimension),
            DisplayedProperty("time_range_constraint", self._time_range_constraint),
            DisplayedProperty("offset_window", self._offset_window),
            DisplayedProperty("offset_to_grain", self._offset_to_grain),
            DisplayedProperty("join_type", self._join_type),
        )

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.time_range_constraint == self.time_range_constraint
            and other_node.offset_window == self.offset_window
            and other_node.offset_to_grain == self.offset_to_grain
            and other_node.requested_agg_time_dimension_specs == self.requested_agg_time_dimension_specs
            and other_node.use_custom_agg_time_dimension == self.use_custom_agg_time_dimension
            and other_node.join_type == self.join_type
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> JoinToTimeSpineNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return JoinToTimeSpineNode(
            parent_node=new_parent_nodes[0],
            requested_agg_time_dimension_specs=self.requested_agg_time_dimension_specs,
            use_custom_agg_time_dimension=self.use_custom_agg_time_dimension,
            time_range_constraint=self.time_range_constraint,
            offset_window=self.offset_window,
            offset_to_grain=self.offset_to_grain,
            join_type=self.join_type,
        )
