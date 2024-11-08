from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from dbt_semantic_interfaces.references import TimeDimensionReference
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class MetricTimeDimensionTransformNode(DataflowPlanNode):
    """A node transforms the input data set so that it contains the metric time dimension and relevant measures.

    The metric time dimension is used later to aggregate all measures in the data set.

    Input: a data set containing measures along with the associated aggregation time dimension.

    Output: a data set similar to the input data set, but includes the configured aggregation time dimension as the
    metric time dimension and only contains measures that are defined to use it.

    Attributes:
        aggregation_time_dimension_reference: The time dimension that measures in the input should be aggregated to.
    """

    aggregation_time_dimension_reference: TimeDimensionReference

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        aggregation_time_dimension_reference: TimeDimensionReference,
    ) -> MetricTimeDimensionTransformNode:
        return MetricTimeDimensionTransformNode(
            parent_nodes=(parent_node,),
            aggregation_time_dimension_reference=aggregation_time_dimension_reference,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_SET_MEASURE_AGGREGATION_TIME

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_metric_time_dimension_transform_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"Metric Time Dimension '{self.aggregation_time_dimension_reference.element_name}'"

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("aggregation_time_dimension", self.aggregation_time_dimension_reference.element_name),
        )

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.aggregation_time_dimension_reference == self.aggregation_time_dimension_reference
        )

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> MetricTimeDimensionTransformNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return MetricTimeDimensionTransformNode.create(
            parent_node=new_parent_nodes[0],
            aggregation_time_dimension_reference=self.aggregation_time_dimension_reference,
        )
