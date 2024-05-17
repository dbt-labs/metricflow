from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.references import TimeDimensionReference
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor


class MetricTimeDimensionTransformNode(DataflowPlanNode):
    """A node transforms the input data set so that it contains the metric time dimension and relevant measures.

    The metric time dimension is used later to aggregate all measures in the data set.

    Input: a data set containing measures along with the associated aggregation time dimension.

    Output: a data set similar to the input data set, but includes the configured aggregation time dimension as the
    metric time dimension and only contains measures that are defined to use it.
    """

    def __init__(  # noqa: D107
        self,
        parent_node: DataflowPlanNode,
        aggregation_time_dimension_reference: TimeDimensionReference,
    ) -> None:
        self._aggregation_time_dimension_reference = aggregation_time_dimension_reference
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_SET_MEASURE_AGGREGATION_TIME

    @property
    def aggregation_time_dimension_reference(self) -> TimeDimensionReference:
        """The time dimension that measures in the input should be aggregated to."""
        return self._aggregation_time_dimension_reference

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_metric_time_dimension_transform_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"Metric Time Dimension '{self.aggregation_time_dimension_reference.element_name}'" ""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("aggregation_time_dimension", self.aggregation_time_dimension_reference.element_name),
        )

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.aggregation_time_dimension_reference == self.aggregation_time_dimension_reference
        )

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> MetricTimeDimensionTransformNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return MetricTimeDimensionTransformNode(
            parent_node=new_parent_nodes[0],
            aggregation_time_dimension_reference=self.aggregation_time_dimension_reference,
        )
