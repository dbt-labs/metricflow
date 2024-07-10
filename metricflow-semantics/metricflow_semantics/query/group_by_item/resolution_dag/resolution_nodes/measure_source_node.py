from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeSet,
    GroupByItemResolutionNodeVisitor,
)
from metricflow_semantics.visitor import VisitorOutputT


@dataclass(frozen=True)
class MeasureGroupByItemSourceNode(GroupByItemResolutionNode):
    """Outputs group-by-items for a measure.

    Attributes:
        measure_reference: Get the group-by items for this measure.
        child_metric_reference: The metric that uses this measure.
    """

    measure_reference: MeasureReference
    child_metric_reference: MetricReference

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 0

    @staticmethod
    def create(  # noqa: D102
        measure_reference: MeasureReference,
        child_metric_reference: MetricReference,
    ) -> MeasureGroupByItemSourceNode:
        return MeasureGroupByItemSourceNode(
            parent_nodes=(),
            measure_reference=measure_reference,
            child_metric_reference=child_metric_reference,
        )

    @override
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_measure_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output group-by-items available for this measure."

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.MEASURE_GROUP_BY_ITEM_RESOLUTION_NODE

    @property
    @override
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return tuple(super().displayed_properties) + (
            DisplayedProperty(
                key="measure_reference",
                value=str(self.measure_reference),
            ),
            DisplayedProperty(
                key="child_metric_reference",
                value=str(self.child_metric_reference),
            ),
        )

    @property
    @override
    def ui_description(self) -> str:
        return f"Measure({repr(self.measure_reference.element_name)})"

    @override
    def _self_set(self) -> GroupByItemResolutionNodeSet:
        return GroupByItemResolutionNodeSet(measure_nodes=(self,))
