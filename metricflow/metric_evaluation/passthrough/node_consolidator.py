from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from collections.abc import Sequence
from typing import Optional

from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.syntactic_sugar import mf_first_item

from metricflow.metric_evaluation.plan.me_edges import MetricQueryDependencyEdge
from metricflow.metric_evaluation.plan.me_nodes import DerivedMetricsQueryNode, MetricQueryNode
from metricflow.metric_evaluation.plan.me_plan import MutableMetricEvaluationPlan
from metricflow.metric_evaluation.plan.query_element import MetricQueryPropertySet

logger = logging.getLogger(__name__)


class DerivedMetricsNodeConsolidator:
    """Consolidate multiple `DerivedMetricsQueryNode`s with common sources to a single node.

    Given `DerivedMetricsQueryNode`s A (computing `metric_0`) and B (computing `metric_1`) that have the same source
    nodes {C}, consolidate A and B to a new node D that computes both `metric_0` and `metric_1`.

    This consolidation helps to reduce the number of queries / joins in the evaluation plan.

    There are some cases where consolidation is intentionally skipped even if sources match,
    for example when a derived metric uses aliases.
    """

    def __init__(  # noqa: D107
        self,
        nodes_to_consolidate: OrderedSet[DerivedMetricsQueryNode],
        corresponding_source_edges: OrderedSet[MetricQueryDependencyEdge],
    ) -> None:
        self._nodes_to_consolidate = nodes_to_consolidate.as_frozen()

        # Build a subplan so graph helper methods (e.g. source node / edge lookups) can be used during consolidation.
        self._subplan = MutableMetricEvaluationPlan.create()
        self._subplan.add_edges(corresponding_source_edges)

        self._validate_constructor_inputs(corresponding_source_edges=corresponding_source_edges)

    def _validate_constructor_inputs(self, corresponding_source_edges: OrderedSet[MetricQueryDependencyEdge]) -> None:
        """Validate that node and edge inputs describe a valid pre-consolidation subplan."""
        # The target for all edges should be in `nodes_to_consolidate`.
        edges_with_invalid_targets: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet(
            edge for edge in corresponding_source_edges if edge.target_node not in self._nodes_to_consolidate
        )
        if edges_with_invalid_targets:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Source edges should reflect the dependencies of the given derived metrics nodes.",
                    edges_with_invalid_targets=edges_with_invalid_targets,
                    nodes_to_consolidate=self._nodes_to_consolidate,
                )
            )

        # The source for all edges should not be in `nodes_to_consolidate`.
        edges_with_invalid_sources: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet(
            edge for edge in corresponding_source_edges if edge.source_node in self._nodes_to_consolidate
        )
        if edges_with_invalid_sources:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Source edges should not have a source in the given set of derived metrics nodes.",
                    edges_with_invalid_sources=edges_with_invalid_sources,
                    derived_metric_nodes=self._nodes_to_consolidate,
                )
            )

        # Each node should compute one metric spec to reflect the pre-consolidated state.
        for node_to_consolidate in self._nodes_to_consolidate:
            if len(node_to_consolidate.computed_metric_specs) != 1:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Provided nodes should each compute exactly one derived metric.",
                        derived_metrics_node=node_to_consolidate,
                    )
                )

        # A missing source edge indicates invalid input and can produce incorrect consolidation.
        nodes_without_source_edges: MutableOrderedSet[DerivedMetricsQueryNode] = MutableOrderedSet(
            node for node in self._nodes_to_consolidate if len(self._subplan.source_edges(node)) == 0
        )
        if nodes_without_source_edges:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Every node to consolidate should have at least one corresponding source edge.",
                    nodes_without_source_edges=nodes_without_source_edges,
                    corresponding_source_edges=corresponding_source_edges,
                )
            )

    def consolidate_nodes(self) -> tuple[OrderedSet[DerivedMetricsQueryNode], OrderedSet[MetricQueryDependencyEdge]]:
        """Return the consolidated nodes and the new edges."""
        new_nodes: MutableOrderedSet[DerivedMetricsQueryNode] = MutableOrderedSet()
        new_edges: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet()

        grouped_nodes_by_key: defaultdict[NodeConsolidationKey, list[DerivedMetricsQueryNode]] = defaultdict(list)

        for node in self._nodes_to_consolidate:
            grouped_nodes_by_key[self._node_consolidation_key(node)].append(node)

        for consolidation_key, grouped_nodes in grouped_nodes_by_key.items():
            # Keep aliased metrics as a separate query.
            if consolidation_key.aliased_metric_spec is not None:
                new_nodes.update(grouped_nodes)
                new_edges.update(*(self._subplan.source_edges(node) for node in grouped_nodes))
                continue

            # Grouping guarantees all nodes here have matching source nodes and query properties.
            consolidated_node, edges_for_consolidated_node = self._consolidate_nodes_for_non_aliased_metrics(
                nodes=grouped_nodes, query_properties=consolidation_key.query_properties
            )
            new_nodes.add(consolidated_node)
            new_edges.update(edges_for_consolidated_node)

        return (new_nodes, new_edges)

    def _node_consolidation_key(self, node: DerivedMetricsQueryNode) -> NodeConsolidationKey:
        """Build the grouping key for a derived metric node."""
        # Constructor validation guarantees that each candidate node computes exactly one metric.
        computed_metric_spec = mf_first_item(node.computed_metric_specs)
        return NodeConsolidationKey(
            aliased_metric_spec=computed_metric_spec if computed_metric_spec.alias is not None else None,
            source_nodes=self._subplan.source_nodes(node).as_frozen(),
            query_properties=node.query_properties,
        )

    def _consolidate_nodes_for_non_aliased_metrics(
        self, nodes: Sequence[DerivedMetricsQueryNode], query_properties: MetricQueryPropertySet
    ) -> tuple[DerivedMetricsQueryNode, OrderedSet[MetricQueryDependencyEdge]]:
        """Consolidate the given nodes to a single node that computes all metrics.

        The nodes must have identical source nodes and cannot represent aliased metrics.
        """
        node_count = len(nodes)
        if node_count == 0:
            raise MetricFlowInternalError("No nodes passed in for consolidation.")
        if node_count == 1:
            node = nodes[0]
            edges = self._subplan.source_edges(node)
            logger.debug(LazyFormat("No-op consolidation with a single node", node=node, edges=edges))
            return node, edges

        # With shared sources, passthrough metrics should match. Using an intersection defensively.
        passthrough_metric_specs_intersection: MutableOrderedSet[MetricSpec] = MutableOrderedSet(
            nodes[0].passthrough_metric_specs
        )
        for node in nodes[1:]:
            passthrough_metric_specs_intersection.intersection_update(node.passthrough_metric_specs)

        for node in nodes:
            if node.passthrough_metric_specs != passthrough_metric_specs_intersection:
                logger.error(
                    LazyFormat(
                        "Expected all nodes to have the same passthrough metrics. Using an intersection to continue, "
                        "but this should be investigated.",
                        node=node,
                        passthrough_metric_specs_intersection=passthrough_metric_specs_intersection,
                    )
                )

        computed_metric_specs = FrozenOrderedSet(
            itertools.chain.from_iterable(node.computed_metric_specs for node in nodes)
        )

        consolidated_node = DerivedMetricsQueryNode.create(
            computed_metric_specs=computed_metric_specs,
            passthrough_metric_specs=passthrough_metric_specs_intersection,
            query_properties=query_properties,
        )

        original_source_edges: list[MetricQueryDependencyEdge] = []
        # Multiple input nodes can map to the same edge on the consolidated node so use an ordered set to dedup.
        # e.g.
        #   Pre-consolidation:
        #
        #   MetricQueryNode(['metric_0']) -> src
        #   MetricQueryNode(['metric_1']) -> src
        #
        #   Post-Consolidation:
        #   MetricQueryNode(['metric_0', 'metric_1']) -> src
        source_edges_for_consolidated_node: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet()
        for node in nodes:
            for original_source_edge in self._subplan.source_edges(node):
                original_source_edges.append(original_source_edge)
                source_edges_for_consolidated_node.add(
                    MetricQueryDependencyEdge.create(
                        target_node=consolidated_node,
                        target_node_output_spec=original_source_edge.target_node_output_spec,
                        source_node=original_source_edge.source_node,
                        source_node_output_spec=original_source_edge.source_node_output_spec,
                    )
                )

        logger.debug(
            LazyFormat(
                "Consolidated derived metric nodes",
                nodes=nodes,
                original_source_edges=original_source_edges,
                consolidated_node=consolidated_node,
                source_edges_for_consolidated_node=source_edges_for_consolidated_node,
            )
        )
        return consolidated_node, source_edges_for_consolidated_node.as_frozen()


@fast_frozen_dataclass()
class NodeConsolidationKey:
    """A key to use for grouping nodes for consolidation.

    This key should produce groups where the queries in each group have the same source nodes and query properties.
    In addition, each aliased metric should be in a separate group.
    """

    # `aliased_metric_spec` is only set for metrics with an alias. It ensures that each aliased metric maps to
    # a separate node.
    aliased_metric_spec: Optional[MetricSpec]
    source_nodes: FrozenOrderedSet[MetricQueryNode]
    query_properties: MetricQueryPropertySet
