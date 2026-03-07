from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_graph.graph_id import MetricFlowGraphId, SequentialGraphId
from metricflow_semantics.toolkit.mf_graph.mf_graph import MetricFlowGraph
from metricflow_semantics.toolkit.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.toolkit.mf_graph.path_finding.pathfinder import MetricFlowPathfinder
from metricflow_semantics.toolkit.mf_graph.path_finding.weight_function import EdgeCountWeightFunction
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from typing_extensions import override

from metricflow.metric_evaluation.plan.me_edges import MetricQueryDependencyEdge
from metricflow.metric_evaluation.plan.me_labels import TopLevelQueryLabel
from metricflow.metric_evaluation.plan.me_nodes import (
    ConversionMetricQueryNode,
    CumulativeMetricQueryNode,
    DerivedMetricsQueryNode,
    MetricQueryNode,
    MetricQueryNodeVisitor,
    SimpleMetricsQueryNode,
    TopLevelQueryNode,
)
from metricflow.metric_evaluation.plan.me_path import MutableMetricEvaluationPath

logger = logging.getLogger(__name__)


class MetricEvaluationPlan(MetricFlowGraph[MetricQueryNode, MetricQueryDependencyEdge], ABC):
    """A graph that describes how metrics should be evaluated in a query for metrics / group-by items.

    Since metrics can be defined using other metrics, this models the recursive dependency relationship.

    The nodes in the graph are queries that compute or passthrough metrics, and the edges point from a query (the
    target node) to its dependencies (the source nodes). The edges point in this way to be analogous to how metrics
    are defined in the configuration (a derived metric lists the input metrics).

    Using the SQL query analogy, if a given node represents a top-level query, the successor nodes represent its
    subqueries.

    Alternative name might be `MetricQueryPlan` or `MetricQueryGraph`, but `MetricQuery` might be confused with the
    overall plan for an MF query.
    """

    MAX_METRIC_DEFINITION_RECURSION_DEPTH = 100

    def validate(self) -> None:
        """Validate that all nodes and edges satisfy metric-evaluation invariants."""
        # Check that the plan has exactly one top-level query node.
        top_level_query_nodes = self.nodes_with_labels(TopLevelQueryLabel.get_instance())
        if len(top_level_query_nodes) != 1:
            raise MetricFlowInternalError(
                LazyFormat(
                    "The metric evaluation plan does not have exactly 1 top-level query node. This is a bug in the "
                    "planner",
                    top_level_query_nodes=top_level_query_nodes,
                )
            )

        # Check invariants for each edge.
        passthrough_metric_spec_validator = MetricEvaluationPlan._PassthroughMetricSpecValidator(self)
        for node in self.nodes:
            # For all nodes that specify this node as a source (the target nodes of this source), check that the edge
            # to this node references a valid output of this node.
            output_metric_specs = node.output_metric_specs
            for target_edge in self.target_edges(node):
                if target_edge.source_node_output_spec not in output_metric_specs:
                    raise ValueError(
                        LazyFormat(
                            "An edge from the target node to a source node states that the source node"
                            " outputs a spec that is not described by the output specs of the source node. This"
                            " indicates incorrect graph construction.",
                            source_node=node,
                            target_edge=target_edge,
                        )
                    )

            # For edges from this node to its source nodes, check that the edge to the source node reflects one of the
            # specs output by this node.
            for source_edge in self.source_edges(node):
                if source_edge.target_node_output_spec not in output_metric_specs:
                    raise ValueError(
                        LazyFormat(
                            "An edge from the target node to a source node states that the target node"
                            " outputs a spec that is not described by the output specs of the target node."
                            " This indicates incorrect graph construction.",
                            target_node=node,
                            source_edge=source_edge,
                        )
                    )

            # Check passthrough metrics of a node matches the data in the edges.
            node.accept(passthrough_metric_spec_validator)

    def source_edges(self, target_node: MetricQueryNode) -> OrderedSet[MetricQueryDependencyEdge]:
        """Return all edges to the source nodes for the given node."""
        return self.edges_with_tail_node(target_node)

    def target_edges(self, source_node: MetricQueryNode) -> OrderedSet[MetricQueryDependencyEdge]:
        """Return all edges from nodes that specify the given node as the source node."""
        return self.edges_with_head_node(source_node)

    def source_nodes(self, node: MetricQueryNode) -> OrderedSet[MetricQueryNode]:
        """Return the source nodes for the given node."""
        return self.successors(node)

    def target_nodes(self, node: MetricQueryNode) -> OrderedSet[MetricQueryNode]:
        """Return all nodes that specify the given node as a source."""
        return self.predecessors(node)

    def nodes_in_dfs_order(self) -> OrderedSet[MetricQueryNode]:
        """Return all nodes in depth-first order.

        This order helps readability as it shows the source nodes before the target node.
        """
        nodes_in_dfs_order: MutableOrderedSet[MetricQueryNode] = MutableOrderedSet()

        pathfinder: MetricFlowPathfinder[
            MetricQueryNode, MetricQueryDependencyEdge, MutableMetricEvaluationPath
        ] = MetricFlowPathfinder()
        weight_function: EdgeCountWeightFunction[
            MetricQueryNode, MetricQueryDependencyEdge, MutableMetricEvaluationPath
        ] = EdgeCountWeightFunction()

        root_nodes = tuple(node for node in self.nodes if len(self.target_nodes(node)) == 0)

        if len(root_nodes) == 0:
            return FrozenOrderedSet()

        for root_node in root_nodes:
            mutable_path = MutableMetricEvaluationPath.create(root_node)
            # Collect nodes in DFS order
            for path in pathfinder.find_paths_dfs(
                graph=self,
                initial_path=mutable_path,
                target_nodes=None,
                weight_function=weight_function,
                max_path_weight=MetricEvaluationPlan.MAX_METRIC_DEFINITION_RECURSION_DEPTH,
            ):
                if len(path.nodes) == 0:
                    raise MetricFlowInternalError(
                        LazyFormat("DFS traversal should have yielded non-empty paths", path=path)
                    )

                nodes_in_dfs_order.add(path.nodes[-1])

        return nodes_in_dfs_order

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> str:
        # Print the nodes in DFS traversal order (e.g. nodes that compute simple metrics first) so that it's easier
        # to read.
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={"nodes_dfs": self.nodes_in_dfs_order(), "edges_dfs": self.edges},
        )

    class _PassthroughMetricSpecValidator(MetricQueryNodeVisitor[None]):
        """Helps check that all metrics that are passed through by a node are present in one of the node's sources."""

        def __init__(self, me_plan: MetricEvaluationPlan) -> None:
            self._me_plan = me_plan

        @override
        def visit_simple_metrics_query_node(self, node: SimpleMetricsQueryNode) -> None:
            # Queries for simple metrics do not pass through metrics.
            pass

        @override
        def visit_cumulative_metric_query_node(self, node: CumulativeMetricQueryNode) -> None:
            # Queries for a cumulative metric do not pass through metrics.
            pass

        @override
        def visit_conversion_metric_query_node(self, node: ConversionMetricQueryNode) -> None:
            # Queries for a conversion metric do not pass through metrics.
            pass

        @override
        def visit_derived_metrics_query_node(self, node: DerivedMetricsQueryNode) -> None:
            self._check_passthrough_metric_specs(node)

        @override
        def visit_top_level_query_node(self, node: TopLevelQueryNode) -> None:
            if len(node.passthrough_metric_specs) == 0:
                raise MetricFlowInternalError(
                    LazyFormat("A top-level query node must pass through at least one metric", node=node)
                )
            self._check_passthrough_metric_specs(node)

        def _check_passthrough_metric_specs(self, node: DerivedMetricsQueryNode | TopLevelQueryNode) -> None:
            """Check that passthrough metrics are sourced by one of this node's dependency edges."""
            passthrough_metric_specs = node.passthrough_metric_specs
            if len(passthrough_metric_specs) == 0:
                return

            source_edges = self._me_plan.edges_with_tail_node(node)
            source_output_specs = {source_edge.source_node_output_spec for source_edge in source_edges}

            for passthrough_metric_spec in passthrough_metric_specs:
                if passthrough_metric_spec not in source_output_specs:
                    raise ValueError(
                        LazyFormat(
                            "A passthrough metric is not present in an edge to a source node."
                            " This indicates incorrect plan construction.",
                            passthrough_metric_spec=passthrough_metric_spec,
                            source_edges=source_edges,
                        )
                    )


@dataclass
class MutableMetricEvaluationPlan(MetricEvaluationPlan, MutableGraph[MetricQueryNode, MetricQueryDependencyEdge]):
    """A mutable version of `MetricEvaluationPlan`."""

    @classmethod
    def create(cls, graph_id: Optional[MetricFlowGraphId] = None) -> MutableMetricEvaluationPlan:
        """Create an empty metric evaluation plan."""
        return MutableMetricEvaluationPlan(
            _graph_id=graph_id or SequentialGraphId.create(),
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            _label_to_nodes=defaultdict(MutableOrderedSet),
            _label_to_edges=defaultdict(MutableOrderedSet),
            _node_to_predecessor_nodes=defaultdict(MutableOrderedSet),
            _node_to_successor_nodes=defaultdict(MutableOrderedSet),
        )

    @override
    def intersection(
        self, other: MetricFlowGraph[MetricQueryNode, MetricQueryDependencyEdge]
    ) -> MutableMetricEvaluationPlan:
        """Return a graph containing only edges present in both graphs."""
        intersection_graph = MutableMetricEvaluationPlan.create(graph_id=self.graph_id)
        intersection_graph.add_edges(self._intersect_edges(other))
        return intersection_graph

    @override
    def inverse(self) -> MutableMetricEvaluationPlan:
        raise NotImplementedError("The inverse graph is not yet implemented.")

    @override
    def as_sorted(self) -> MutableMetricEvaluationPlan:
        """Return a copy with nodes and edges in deterministic sorted order."""
        updated_graph = MutableMetricEvaluationPlan.create(graph_id=self.graph_id)
        for node in sorted(self._nodes):
            updated_graph.add_node(node)

        for edge in sorted(self._edges):
            updated_graph.add_edge(edge)

        return updated_graph
