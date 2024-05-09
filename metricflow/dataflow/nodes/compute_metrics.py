from __future__ import annotations

from typing import Sequence, Set, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.spec_classes import LinkableInstanceSpec, MetricSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)


class ComputeMetricsNode(DataflowPlanNode):
    """A node that computes metrics from input measures. Dimensions / entities are passed through."""

    def __init__(
        self,
        parent_node: DataflowPlanNode,
        metric_specs: Sequence[MetricSpec],
        aggregated_to_elements: Set[LinkableInstanceSpec],
        for_group_by_source_node: bool = False,
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
        self._aggregated_to_elements = aggregated_to_elements
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
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        if not isinstance(other_node, self.__class__):
            return False

        if other_node.metric_specs != self.metric_specs:
            return False

        return (
            isinstance(other_node, self.__class__)
            and other_node.metric_specs == self.metric_specs
            and other_node.aggregated_to_elements == self.aggregated_to_elements
            and other_node.for_group_by_source_node == self.for_group_by_source_node
        )

    def can_combine(self, other_node: ComputeMetricsNode) -> Tuple[bool, str]:
        """Check certain node attributes against another node to determine if the two can be combined.

        Return a bool and a string reason for the failure to combine (if applicable) to be used in logging for
        ComputeMetricsBranchCombiner.
        """
        if not other_node.aggregated_to_elements == self.aggregated_to_elements:
            return False, "nodes are aggregated to different elements"

        if other_node.for_group_by_source_node != self.for_group_by_source_node:
            return False, "one node is a group by metric source node"

        return True, ""

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> ComputeMetricsNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return ComputeMetricsNode(
            parent_node=new_parent_nodes[0],
            metric_specs=self.metric_specs,
            for_group_by_source_node=self.for_group_by_source_node,
            aggregated_to_elements=self._aggregated_to_elements,
        )

    @property
    def aggregated_to_elements(self) -> Set[LinkableInstanceSpec]:  # noqa: D102
        return self._aggregated_to_elements
