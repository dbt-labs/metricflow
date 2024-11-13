from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Optional, Sequence

from dbt_semantic_interfaces.protocols import MetricTimeWindow
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class JoinToTimeSpineNode(DataflowPlanNode, ABC):
    """Join parent dataset to time spine dataset.

    Attributes:
        requested_agg_time_dimension_specs: Time dimensions requested in the query.
        use_custom_agg_time_dimension: Indicates if agg_time_dimension should be used in join. If false, uses metric_time.
        join_type: Join type to use when joining to time spine.
        time_range_constraint: Time range to constrain the time spine to.
        offset_window: Time window to offset the parent dataset by when joining to time spine.
        offset_to_grain: Granularity period to offset the parent dataset to when joining to time spine.
    """

    requested_agg_time_dimension_specs: Sequence[TimeDimensionSpec]
    use_custom_agg_time_dimension: bool
    join_type: SqlJoinType
    time_range_constraint: Optional[TimeRangeConstraint]
    offset_window: Optional[MetricTimeWindow]
    offset_to_grain: Optional[TimeGranularity]
    time_spine_filters: Optional[Sequence[WhereFilterSpec]] = None

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

        assert not (
            self.offset_window and self.offset_to_grain
        ), "Can't set both offset_window and offset_to_grain when joining to time spine. Choose one or the other."
        assert (
            len(self.requested_agg_time_dimension_specs) > 0
        ), "Must have at least one value in requested_agg_time_dimension_specs for JoinToTimeSpineNode."

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        requested_agg_time_dimension_specs: Sequence[TimeDimensionSpec],
        use_custom_agg_time_dimension: bool,
        join_type: SqlJoinType,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        offset_window: Optional[MetricTimeWindow] = None,
        offset_to_grain: Optional[TimeGranularity] = None,
        time_spine_filters: Optional[Sequence[WhereFilterSpec]] = None,
    ) -> JoinToTimeSpineNode:
        return JoinToTimeSpineNode(
            parent_nodes=(parent_node,),
            requested_agg_time_dimension_specs=tuple(requested_agg_time_dimension_specs),
            use_custom_agg_time_dimension=use_custom_agg_time_dimension,
            join_type=join_type,
            time_range_constraint=time_range_constraint,
            offset_window=offset_window,
            offset_to_grain=offset_to_grain,
            time_spine_filters=time_spine_filters,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_TO_TIME_SPINE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_join_to_time_spine_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Join to Time Spine Dataset"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        props = tuple(super().displayed_properties) + (
            DisplayedProperty("requested_agg_time_dimension_specs", self.requested_agg_time_dimension_specs),
            DisplayedProperty("use_custom_agg_time_dimension", self.use_custom_agg_time_dimension),
            DisplayedProperty("join_type", self.join_type),
        )
        if self.offset_window:
            props += (DisplayedProperty("offset_window", self.offset_window),)
        if self.offset_to_grain:
            props += (DisplayedProperty("offset_to_grain", self.offset_to_grain),)
        if self.time_range_constraint:
            props += (DisplayedProperty("time_range_constraint", self.time_range_constraint),)
        if self.time_spine_filters:
            props += (
                DisplayedProperty(
                    "time_spine_filters", [time_spine_filter.where_sql for time_spine_filter in self.time_spine_filters]
                ),
            )
        return props

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.time_range_constraint == self.time_range_constraint
            and other_node.offset_window == self.offset_window
            and other_node.offset_to_grain == self.offset_to_grain
            and other_node.requested_agg_time_dimension_specs == self.requested_agg_time_dimension_specs
            and other_node.use_custom_agg_time_dimension == self.use_custom_agg_time_dimension
            and other_node.join_type == self.join_type
            and other_node.time_spine_filters == self.time_spine_filters
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> JoinToTimeSpineNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return JoinToTimeSpineNode.create(
            parent_node=new_parent_nodes[0],
            requested_agg_time_dimension_specs=self.requested_agg_time_dimension_specs,
            use_custom_agg_time_dimension=self.use_custom_agg_time_dimension,
            time_range_constraint=self.time_range_constraint,
            offset_window=self.offset_window,
            offset_to_grain=self.offset_to_grain,
            join_type=self.join_type,
            time_spine_filters=self.time_spine_filters,
        )
