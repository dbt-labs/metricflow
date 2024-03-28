from __future__ import annotations

from typing import Sequence

from metricflow.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow.dag.mf_dag import DisplayedProperty
from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    ComputedMetricsOutput,
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)
from metricflow.specs.specs import MetricSpec
from metricflow.visitor import VisitorOutputT


class ComputeMetricsNode(ComputedMetricsOutput):
    """A node that computes metrics from input measures. Dimensions / entities are passed through."""

    def __init__(
        self, parent_node: BaseOutput, metric_specs: Sequence[MetricSpec], for_group_by_source_node: bool = False
    ) -> None:
        """Constructor.

        Args:
            parent_node: Node where data is coming from.
            metric_specs: The specs for the metrics that this should compute.
            for_group_by_source_node: Whether the node is part of a dataflow plan used for a group by source node.
        """
        self._parent_node = parent_node
        self._metric_specs = tuple(metric_specs)
        self._for_group_by_source_node = for_group_by_source_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(self._parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_COMPUTE_METRICS_ID_PREFIX

    @property
    def for_group_by_source_node(self) -> bool:
        """Whether or not this node is part of a dataflow plan used for a group by source node."""
        return self._for_group_by_source_node

    @property
    def metric_specs(self) -> Sequence[MetricSpec]:
        """The metric instances that this node is supposed to compute and should have in the output."""
        return self._metric_specs

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_compute_metrics_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Compute Metrics via Expressions"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        displayed_properties = tuple(super().displayed_properties) + tuple(
            DisplayedProperty("metric_spec", metric_spec) for metric_spec in self._metric_specs
        )
        if self.for_group_by_source_node:
            displayed_properties += (DisplayedProperty("for_group_by_source_node", self.for_group_by_source_node),)
        return displayed_properties

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D102
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        if not isinstance(other_node, self.__class__):
            return False

        if other_node.metric_specs != self.metric_specs:
            return False

        return (
            isinstance(other_node, self.__class__)
            and other_node.metric_specs == self.metric_specs
            and other_node.for_group_by_source_node == self.for_group_by_source_node
        )

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> ComputeMetricsNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return ComputeMetricsNode(
            parent_node=new_parent_nodes[0],
            metric_specs=self.metric_specs,
            for_group_by_source_node=self.for_group_by_source_node,
        )
