from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Set, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class ComputeMetricsNode(DataflowPlanNode):
    """A node that computes metrics from input simple-metric inputs. Dimensions / entities are passed through.

    Attributes:
        metric_specs: The specs for the metrics that this should compute.
        for_group_by_source_node: Whether the node is part of a dataflow plan used for a group by source node.
    """

    metric_specs: Tuple[MetricSpec, ...]
    for_group_by_source_node: bool
    _aggregated_to_elements: Tuple[LinkableInstanceSpec, ...]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()

        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        metric_specs: Sequence[MetricSpec],
        aggregated_to_elements: Set[LinkableInstanceSpec],
        for_group_by_source_node: bool = False,
    ) -> ComputeMetricsNode:
        return ComputeMetricsNode(
            parent_nodes=(parent_node,),
            metric_specs=tuple(metric_specs),
            _aggregated_to_elements=tuple(aggregated_to_elements),
            for_group_by_source_node=for_group_by_source_node,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_COMPUTE_METRICS_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_compute_metrics_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Compute Metrics via Expressions"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        displayed_properties = tuple(super().displayed_properties) + tuple(
            DisplayedProperty("metric_spec", metric_spec) for metric_spec in self.metric_specs
        )
        if self.for_group_by_source_node:
            displayed_properties += (DisplayedProperty("for_group_by_source_node", self.for_group_by_source_node),)
        return displayed_properties

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

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

        alias_to_metric_spec = {spec.alias: spec for spec in self.metric_specs if spec.alias is not None}

        for spec in other_node.metric_specs:
            if (
                spec.alias is not None
                and spec.alias in alias_to_metric_spec
                and alias_to_metric_spec[spec.alias] != spec
            ):
                return (
                    False,
                    f"Alias '{spec.alias}' is defined in both nodes but it refers to different things in each of them",
                )

        return True, ""

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> ComputeMetricsNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return ComputeMetricsNode.create(
            parent_node=new_parent_nodes[0],
            metric_specs=self.metric_specs,
            for_group_by_source_node=self.for_group_by_source_node,
            aggregated_to_elements=self.aggregated_to_elements,
        )

    @property
    @override
    def aggregated_to_elements(self) -> Set[LinkableInstanceSpec]:
        return set(self._aggregated_to_elements)
