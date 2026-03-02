from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional

from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.comparable import ComparisonKey
from metricflow_semantics.toolkit.mf_graph.mf_graph import MetricFlowGraphEdge
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from typing_extensions import override

from metricflow.metric_evaluation.plan.me_nodes import MetricQueryNode

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class MetricQueryDependencyEdge(MetricFlowGraphEdge[MetricQueryNode]):
    """Describes a dependency of a metric query node.

    For a node that represents a query to compute a derived metric (the target node), the source nodes represent the
    queries that compute the input metrics. The edge points from the target node (the derived metric) to one of the
    source node (one of the input metrics) to be analogous to the way that a derived metric definition lists the input
    metrics.

    Since derived metrics can have multiple input metrics, the edge also describes the specific
    metric-to-input-metric dependency. For example, if a node A computes `bookings_per_listing` from source nodes
    B (computes `bookings`) and C (computes `listings`), the following edges capture the dependencies:

        A (target_node_output_spec: `bookings_per_listing`) -> B (source_node_output_spec: `bookings`)
        A (target_node_output_spec: `bookings_per_listing`) -> C (source_node_output_spec: `listings`)

    For the above case, nodes B and C are the dependencies of node A.
    """

    # A metric that is output by the target node.
    target_node_output_spec: MetricSpec
    # For the above, the associated dependency from the source node.
    source_node_output_spec: MetricSpec

    @staticmethod
    def create(
        target_node: MetricQueryNode,
        target_node_output_spec: MetricSpec,
        source_node: MetricQueryNode,
        source_node_output_spec: MetricSpec,
    ) -> MetricQueryDependencyEdge:
        """Create an edge from a target node to one of its source dependency nodes."""
        return MetricQueryDependencyEdge(
            tail_node=target_node,
            target_node_output_spec=target_node_output_spec,
            head_node=source_node,
            source_node_output_spec=source_node_output_spec,
        )

    @property
    @override
    def inverse(self) -> MetricQueryDependencyEdge:
        raise NotImplementedError("The inverse graph is not yet implemented.")

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.tail_node, self.target_node_output_spec, self.head_node, self.source_node_output_spec)

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "edge": f"{self.tail_node.node_descriptor.node_name} -> {self.head_node.node_descriptor.node_name}",
                "target_node_output_spec": self.target_node_output_spec,
                "source_node_output_spec": self.source_node_output_spec,
            },
        )

    @cached_property
    def source_node(self) -> MetricQueryNode:
        """Return the dependency node for this edge.

        This is the same value as `head_node`, but with naming specific to metric dependencies.
        """
        return self.head_node

    @cached_property
    def target_node(self) -> MetricQueryNode:
        """Return the node that depends on `source_node`.

        This is the same value as `tail_node`, but with naming specific to metric dependencies.
        """
        return self.tail_node
