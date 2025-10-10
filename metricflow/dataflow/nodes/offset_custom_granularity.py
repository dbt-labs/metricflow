from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Sequence

from dbt_semantic_interfaces.protocols.metric import MetricTimeWindow
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class OffsetCustomGranularityNode(DataflowPlanNode, ABC):
    """Offset a custom grain by the requested number of periods.

    Used to build the time spine node when querying a metric with a custom offset window, when the query requires ONLY the same
    grain as is used in the offset window. The node will output offset columns for all custom grain specs requested and a
    non-offset column for the base grain. The base grain is needed to join to the source node.
    """

    offset_window: MetricTimeWindow
    required_time_spine_specs: Sequence[TimeDimensionSpec]
    time_spine_node: DataflowPlanNode

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        if self.offset_window.is_standard_granularity:
            raise ValueError(
                LazyFormat(
                    "OffsetBaseGrainByCustomGrainNode should only be used for custom grain offset windows.",
                    offset_window=self.offset_window,
                )
            )

    @staticmethod
    def create(  # noqa: D102
        time_spine_node: DataflowPlanNode,
        offset_window: MetricTimeWindow,
        required_time_spine_specs: Sequence[TimeDimensionSpec],
    ) -> OffsetCustomGranularityNode:
        return OffsetCustomGranularityNode(
            parent_nodes=(time_spine_node,),
            time_spine_node=time_spine_node,
            offset_window=offset_window,
            required_time_spine_specs=required_time_spine_specs,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_OFFSET_CUSTOM_GRANULARITY_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_offset_custom_granularity_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Offset Custom Granularity"

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
    ) -> OffsetCustomGranularityNode:
        assert len(new_parent_nodes) == 1
        return OffsetCustomGranularityNode(
            parent_nodes=tuple(new_parent_nodes),
            time_spine_node=new_parent_nodes[0],
            offset_window=self.offset_window,
            required_time_spine_specs=self.required_time_spine_specs,
        )
