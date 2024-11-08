from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.specs.measure_spec import MetricInputMeasureSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class AggregateMeasuresNode(DataflowPlanNode):
    """A node that aggregates the measures by the associated group by elements.

    In the event that one or more of the aggregated input measures has an alias assigned to it, any output query
    resulting from an operation on this node must apply the alias and transform the measure instances accordingly,
    otherwise this join could produce a query with two identically named measure columns with, e.g., different
    constraints applied to the measure.

    The input measure specs are required for downstream nodes to be aware of any input measures with
    user-provided aliases, such as we might encounter with constrained and unconstrained versions of the
    same input measure.
    """

    metric_input_measure_specs: Tuple[MetricInputMeasureSpec, ...]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode, metric_input_measure_specs: Sequence[MetricInputMeasureSpec]
    ) -> AggregateMeasuresNode:
        return AggregateMeasuresNode(
            parent_nodes=(parent_node,), metric_input_measure_specs=tuple(metric_input_measure_specs)
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_AGGREGATE_MEASURES_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_aggregate_measures_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Aggregate Measures"""

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.metric_input_measure_specs == self.metric_input_measure_specs
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> AggregateMeasuresNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return AggregateMeasuresNode(
            parent_nodes=tuple(new_parent_nodes),
            metric_input_measure_specs=self.metric_input_measure_specs,
        )
