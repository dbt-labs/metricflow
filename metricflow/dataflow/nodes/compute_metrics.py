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

    def __init__(self, parent_node: BaseOutput, metric_specs: Sequence[MetricSpec]) -> None:  # noqa: D
        """Constructor.

        Args:
            parent_node: Node where data is coming from.
            metric_specs: The specs for the metrics that this should compute.
        """
        self._parent_node = parent_node
        self._metric_specs = tuple(metric_specs)
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(self._parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D
        return StaticIdPrefix.DATAFLOW_NODE_COMPUTE_METRICS_ID_PREFIX

    @property
    def metric_specs(self) -> Sequence[MetricSpec]:  # noqa: D
        """The metric instances that this node is supposed to compute and should have in the output."""
        return self._metric_specs

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_compute_metrics_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Compute Metrics via Expressions"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D
        return tuple(super().displayed_properties) + tuple(
            DisplayedProperty("metric_spec", metric_spec) for metric_spec in self._metric_specs
        )

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        if not isinstance(other_node, self.__class__):
            return False

        if other_node.metric_specs != self.metric_specs:
            return False

        return isinstance(other_node, self.__class__) and other_node.metric_specs == self.metric_specs

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> ComputeMetricsNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return ComputeMetricsNode(
            parent_node=new_parent_nodes[0],
            metric_specs=self.metric_specs,
        )
