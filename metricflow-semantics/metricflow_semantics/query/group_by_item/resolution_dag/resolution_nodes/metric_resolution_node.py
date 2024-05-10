from __future__ import annotations

from typing import Optional, Sequence, Union

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import Self, override

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.query.group_by_item.resolution_dag.input_metric_location import InputMetricDefinitionLocation
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeSet,
    GroupByItemResolutionNodeVisitor,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.measure_source_node import (
    MeasureGroupByItemSourceNode,
)
from metricflow_semantics.visitor import VisitorOutputT


class MetricGroupByItemResolutionNode(GroupByItemResolutionNode):
    """Outputs group-by-items relevant to a metric based on the input group-by-items."""

    def __init__(
        self,
        metric_reference: MetricReference,
        metric_input_location: Optional[InputMetricDefinitionLocation],
        parent_nodes: Sequence[Union[MeasureGroupByItemSourceNode, Self]],
    ) -> None:
        """Initializer.

        Args:
            metric_reference: The metric that this represents.
            metric_input_location: If this is an input metric for a derived metric, the location within the derived
            metric definition.
            parent_nodes: The parent nodes of this metric.
        """
        self._metric_reference = metric_reference
        self._metric_input_location = metric_input_location
        self._parent_nodes = parent_nodes
        super().__init__()

    @override
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_metric_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output group-by-items available for this metric."

    @property
    @override
    def parent_nodes(self) -> Sequence[Union[MeasureGroupByItemSourceNode, Self]]:
        return self._parent_nodes

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.METRIC_GROUP_BY_ITEM_RESOLUTION_NODE

    @property
    @override
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return tuple(super().displayed_properties) + (
            DisplayedProperty(
                key="metric_reference",
                value=str(self._metric_reference),
            ),
        )

    @property
    def metric_reference(self) -> MetricReference:  # noqa: D102
        return self._metric_reference

    @property
    def metric_input_location(self) -> Optional[InputMetricDefinitionLocation]:  # noqa: D102
        return self._metric_input_location

    @property
    @override
    def ui_description(self) -> str:
        if self._metric_input_location is None:
            return f"Metric({repr(self._metric_reference.element_name)})"
        return (
            f"Metric({repr(self._metric_reference.element_name)}, "
            f"input_metric_index={self._metric_input_location.input_metric_list_index})"
        )

    @override
    def _self_set(self) -> GroupByItemResolutionNodeSet:
        return GroupByItemResolutionNodeSet(metric_nodes=(self,))
