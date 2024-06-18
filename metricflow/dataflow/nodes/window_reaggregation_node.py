from __future__ import annotations

from typing import Sequence, Set

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.spec_classes import InstanceSpec, LinkableInstanceSpec, MetricSpec, TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode


class WindowReaggregationNode(DataflowPlanNode):
    """A node that re-aggregates metrics using window functions.

    Currently used for calculating cumulative metrics at various granularities.
    """

    def __init__(  # noqa: D107
        self,
        parent_node: ComputeMetricsNode,
        metric_spec: MetricSpec,
        order_by_spec: TimeDimensionSpec,
        partition_by_specs: Sequence[InstanceSpec],
    ) -> None:
        if order_by_spec in partition_by_specs:
            raise ValueError(
                "Order by spec found in parition by specs for WindowAggregationNode. This indicates internal misconfiguration"
                f" because reaggregation should not be needed in this circumstance. Order by spec: {order_by_spec}; "
                f"Partition by specs:{partition_by_specs}"
            )

        self.parent_node = parent_node
        self.metric_spec = metric_spec
        self.order_by_spec = order_by_spec
        self.partition_by_specs = partition_by_specs

        super().__init__(node_id=self.create_unique_id(), parent_nodes=(self.parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_WINDOW_REAGGREGATION_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_window_reaggregation_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Re-aggregate Metrics via Window Functions"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("metric_spec", self.metric_spec),
            DisplayedProperty("order_by_spec", self.order_by_spec),
            DisplayedProperty("partition_by_specs", self.partition_by_specs),
        )

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.parent_node == self.parent_node
            and other_node.metric_spec == self.metric_spec
            and other_node.order_by_spec == self.order_by_spec
            and other_node.partition_by_specs == self.partition_by_specs
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> WindowReaggregationNode:  # noqa: D102
        assert len(new_parent_nodes) == 1, "WindowReaggregationNode cannot accept multiple parents."
        new_parent_node = new_parent_nodes[0]
        assert isinstance(
            new_parent_node, ComputeMetricsNode
        ), "WindowReaggregationNode can only have ComputeMetricsNode as parent node."
        return WindowReaggregationNode(
            parent_node=new_parent_node,
            metric_spec=self.metric_spec,
            order_by_spec=self.order_by_spec,
            partition_by_specs=self.partition_by_specs,
        )

    @property
    def aggregated_to_elements(self) -> Set[LinkableInstanceSpec]:  # noqa: D102
        return set()
