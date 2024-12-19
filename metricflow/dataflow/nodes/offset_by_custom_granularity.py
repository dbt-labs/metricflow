from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Optional, Sequence

from dbt_semantic_interfaces.protocols.metric import MetricTimeWindow
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.custom_granularity_bounds import CustomGranularityBoundsNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode


@dataclass(frozen=True, eq=False)
class OffsetByCustomGranularityNode(DataflowPlanNode, ABC):
    """For a given custom grain, offset its base grain by the requested number of custom grain periods.

    Only accepts CustomGranularityBoundsNode as parent node.
    """

    offset_window: MetricTimeWindow
    required_time_spine_specs: Sequence[TimeDimensionSpec]
    custom_granularity_bounds_node: CustomGranularityBoundsNode
    filter_elements_node: FilterElementsNode

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()

    @staticmethod
    def create(  # noqa: D102
        custom_granularity_bounds_node: CustomGranularityBoundsNode,
        filter_elements_node: FilterElementsNode,
        offset_window: MetricTimeWindow,
        required_time_spine_specs: Sequence[TimeDimensionSpec],
    ) -> OffsetByCustomGranularityNode:
        return OffsetByCustomGranularityNode(
            parent_nodes=(custom_granularity_bounds_node, filter_elements_node),
            custom_granularity_bounds_node=custom_granularity_bounds_node,
            filter_elements_node=filter_elements_node,
            offset_window=offset_window,
            required_time_spine_specs=required_time_spine_specs,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_OFFSET_BY_CUSTOMG_GRANULARITY_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_offset_by_custom_granularity_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Offset Base Granularity By Custom Granularity Period(s)"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("offset_window", self.offset_window),
            DisplayedProperty("required_time_spine_specs", self.required_time_spine_specs),
        )

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.offset_window == self.offset_window
            and other_node.required_time_spine_specs == self.required_time_spine_specs
        )

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> OffsetByCustomGranularityNode:
        custom_granularity_bounds_node: Optional[CustomGranularityBoundsNode] = None
        filter_elements_node: Optional[FilterElementsNode] = None
        for parent_node in new_parent_nodes:
            if isinstance(parent_node, CustomGranularityBoundsNode):
                custom_granularity_bounds_node = parent_node
            elif isinstance(parent_node, FilterElementsNode):
                filter_elements_node = parent_node
        assert custom_granularity_bounds_node and filter_elements_node, (
            "Can't rewrite OffsetByCustomGranularityNode because the node requires a CustomGranularityBoundsNode and a "
            f"FilterElementsNode as parents. Instead, got: {new_parent_nodes}"
        )

        return OffsetByCustomGranularityNode(
            parent_nodes=tuple(new_parent_nodes),
            custom_granularity_bounds_node=custom_granularity_bounds_node,
            filter_elements_node=filter_elements_node,
            offset_window=self.offset_window,
            required_time_spine_specs=self.required_time_spine_specs,
        )
