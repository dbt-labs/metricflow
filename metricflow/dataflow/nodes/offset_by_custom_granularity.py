from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Sequence

from dbt_semantic_interfaces.protocols.metric import MetricTimeWindow
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.custom_granularity_bounds import CustomGranularityBoundsNode


@dataclass(frozen=True, eq=False)
class OffsetByCustomGranularityNode(DataflowPlanNode, ABC):
    """For a given custom grain, offset its base grain by the requested number of custom grain periods.

    Only accepts CustomGranularityBoundsNode as parent node.
    """

    offset_window: MetricTimeWindow
    required_time_spine_specs: Sequence[TimeDimensionSpec]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1
        if not isinstance(self.parent_node, CustomGranularityBoundsNode):
            raise RuntimeError(
                "OffsetByCustomGranularityNode only accepts CustomGranularityBoundsNode as a parent node."
            )

    @staticmethod
    def create(  # noqa: D102
        parent_node: CustomGranularityBoundsNode,
        offset_window: MetricTimeWindow,
        required_time_spine_specs: Sequence[TimeDimensionSpec],
    ) -> OffsetByCustomGranularityNode:
        return OffsetByCustomGranularityNode(
            parent_nodes=(parent_node,),
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

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.offset_window == self.offset_window
            and other_node.required_time_spine_specs == self.required_time_spine_specs
        )

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> OffsetByCustomGranularityNode:
        assert len(new_parent_nodes) == 1
        return OffsetByCustomGranularityNode(
            parent_nodes=tuple(new_parent_nodes),
            offset_window=self.offset_window,
            required_time_spine_specs=self.required_time_spine_specs,
        )
