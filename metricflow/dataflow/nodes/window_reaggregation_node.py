from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Set

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode


@dataclass(frozen=True, eq=False)
class WindowReaggregationNode(DataflowPlanNode):
    """A node that re-aggregates metrics using window functions.

    Currently used for calculating cumulative metrics at various granularities.

    Attributes:
        metric_spec: Specification of the metric to be re-aggregated.
        order_by_spec: Specification of the time dimension to order by.
        partition_by_specs: Specifications of the instances to partition by.
    """

    metric_spec: MetricSpec
    order_by_spec: TimeDimensionSpec
    partition_by_specs: Sequence[InstanceSpec]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1
        if self.order_by_spec in self.partition_by_specs:
            raise ValueError(
                "Order by spec found in partition by specs for WindowAggregationNode. This indicates internal misconfiguration"
                f" because reaggregation should not be needed in this circumstance. Order by spec: {self.order_by_spec}; "
                f"Partition by specs:{self.partition_by_specs}"
            )

    @staticmethod
    def create(  # noqa: D102
        parent_node: ComputeMetricsNode,
        metric_spec: MetricSpec,
        order_by_spec: TimeDimensionSpec,
        partition_by_specs: Sequence[InstanceSpec],
    ) -> WindowReaggregationNode:
        return WindowReaggregationNode(
            parent_nodes=(parent_node,),
            metric_spec=metric_spec,
            order_by_spec=order_by_spec,
            partition_by_specs=partition_by_specs,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_WINDOW_REAGGREGATION_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_window_reaggregation_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Re-aggregate Metrics via Window Functions"""

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

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
            and other_node.parent_nodes == self.parent_nodes
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
        return WindowReaggregationNode.create(
            parent_node=new_parent_node,
            metric_spec=self.metric_spec,
            order_by_spec=self.order_by_spec,
            partition_by_specs=self.partition_by_specs,
        )

    @property
    def aggregated_to_elements(self) -> Set[LinkableInstanceSpec]:  # noqa: D102
        return set()
