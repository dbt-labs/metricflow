from __future__ import annotations

from typing import Sequence, Set, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.spec_classes import LinkableInstanceSpec, MetricSpec, TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)


# TODO: Rename, lolll
class WindowReaggregationNode(DataflowPlanNode):
    """A node that re-aggregates metrics across time dimensions using window functions.

    Used for calculating cumulative metrics at various granularities. Other dimensions / entities are passed through.
    """

    def __init__(
        self,
        parent_node: ComputeMetricsNode,
    ) -> None:
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(self._parent_node,))

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
        displayed_properties = tuple(super().displayed_properties)
        return displayed_properties

    @property
    def parent_node(self) -> ComputeMetricsNode:  # noqa: D102
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__) and other_node.parent_node == self.parent_node

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> ComputeMetricsNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return WindowReaggregationNode(parent_node=new_parent_nodes[0])

    @property
    def aggregated_to_elements(self) -> Set[LinkableInstanceSpec]:  # noqa: D102
        return ()
