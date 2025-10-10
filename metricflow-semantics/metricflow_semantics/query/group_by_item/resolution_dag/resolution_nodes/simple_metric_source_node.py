from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.query.group_by_item.resolution_dag.input_metric_location import InputMetricDefinitionLocation
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeSet,
    GroupByItemResolutionNodeVisitor,
)
from metricflow_semantics.toolkit.visitor import VisitorOutputT


@dataclass(frozen=True)
class SimpleMetricGroupByItemSourceNode(GroupByItemResolutionNode):
    """Outputs group-by-items for a simple metric.

    Attributes:
        metric_reference: Get the group-by items for this simple metric.
        metric_input_location: Optional[InputMetricDefinitionLocation]
    """

    metric_reference: MetricReference
    metric_input_location: Optional[InputMetricDefinitionLocation]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 0

    @staticmethod
    def create(  # noqa: D102
        simple_metric_reference: MetricReference, metric_input_location: Optional[InputMetricDefinitionLocation]
    ) -> SimpleMetricGroupByItemSourceNode:
        return SimpleMetricGroupByItemSourceNode(
            parent_nodes=(),
            metric_reference=simple_metric_reference,
            metric_input_location=metric_input_location,
        )

    @override
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_simple_metric_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output group-by-items available for this simple metric."

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.SIMPLE_METRIC_GROUP_BY_ITEM_RESOLUTION_NODE

    @property
    @override
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return tuple(super().displayed_properties) + (
            DisplayedProperty(
                key="metric_reference",
                value=str(self.metric_reference),
            ),
        )

    @property
    @override
    def ui_description(self) -> str:
        return f"SimpleMetric({repr(self.metric_reference.element_name)})"

    @override
    def _self_set(self) -> GroupByItemResolutionNodeSet:
        return GroupByItemResolutionNodeSet(simple_metric_nodes=(self,))
