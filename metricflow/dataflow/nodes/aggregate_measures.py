from __future__ import annotations

from abc import ABC
from typing import Sequence, Tuple

from metricflow.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow.dataflow.dataflow_plan import BaseOutput, DataflowPlanNode, DataflowPlanNodeVisitor
from metricflow.specs.specs import MetricInputMeasureSpec
from metricflow.visitor import VisitorOutputT


class AggregatedMeasuresOutput(BaseOutput, ABC):
    """A node that outputs data where the measures are aggregated.

    The measures are aggregated with respect to the present entities and dimensions.
    """

    pass


class AggregateMeasuresNode(AggregatedMeasuresOutput):
    """A node that aggregates the measures by the associated group by elements.

    In the event that one or more of the aggregated input measures has an alias assigned to it, any output query
    resulting from an operation on this node must apply the alias and transform the measure instances accordingly,
    otherwise this join could produce a query with two identically named measure columns with, e.g., different
    constraints applied to the measure.
    """

    def __init__(
        self,
        parent_node: BaseOutput,
        metric_input_measure_specs: Sequence[MetricInputMeasureSpec],
    ) -> None:
        """Initializer for AggregateMeasuresNode.

        The input measure specs are required for downstream nodes to be aware of any input measures with
        user-provided aliases, such as we might encounter with constrained and unconstrained versions of the
        same input measure.
        """
        self._parent_node = parent_node
        self._metric_input_measure_specs = tuple(metric_input_measure_specs)

        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._parent_node])

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D
        return StaticIdPrefix.DATAFLOW_NODE_AGGREGATE_MEASURES_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_aggregate_measures_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Aggregate Measures"""

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    @property
    def metric_input_measure_specs(self) -> Tuple[MetricInputMeasureSpec, ...]:
        """Iterable of specs for measure inputs to downstream metrics.

        Used for assigning aliases to output columns produced by aggregated measures.
        """
        return self._metric_input_measure_specs

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return (
            isinstance(other_node, self.__class__)
            and other_node.metric_input_measure_specs == self.metric_input_measure_specs
        )

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> AggregateMeasuresNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return AggregateMeasuresNode(
            parent_node=new_parent_nodes[0],
            metric_input_measure_specs=self.metric_input_measure_specs,
        )
