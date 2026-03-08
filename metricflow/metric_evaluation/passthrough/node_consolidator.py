from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from collections.abc import Iterable, Sequence
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

    There are some cases where the consolidation is currently skipped even if the sources are the same.
    e.g. when a derived metric uses aliases.
    """

    def __init__(  # noqa: D107
        self,
        nodes_to_consolidate: OrderedSet[DerivedMetricsQueryNode],
        corresponding_source_edges: OrderedSet[MetricQueryDependencyEdge],
    ) -> None:
        # Check that the correct inputs have been passed in.

        # The target for all edges should be in `nodes_to_consolidate`.
        edges_with_invalid_targets: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet(
            edge for edge in corresponding_source_edges if edge.target_node not in nodes_to_consolidate
        )
        if edges_with_invalid_targets:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Source edges should reflect the dependencies of the given derived metrics nodes.",
                    edges_with_invalid_targets=edges_with_invalid_targets,
                    nodes_to_consolidate=nodes_to_consolidate,
                )
            )

        # The source for all edges should not be in `nodes_to_consolidate`.
        edges_with_invalid_sources: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet(
            edge for edge in corresponding_source_edges if edge.source_node in nodes_to_consolidate
        )

        for source_edge in corresponding_source_edges:
            if source_edge.source_node in nodes_to_consolidate:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Source edges should not have a source in the given set of derived metrics nodes.",
                        edges_with_invalid_sources=edges_with_invalid_sources,
                        derived_metric_nodes=nodes_to_consolidate,
                    )
                )

        # Each node should compute 1 metric spec to reflect the pre-consolidated state.
        for node_to_consolidate in nodes_to_consolidate:
            if len(node_to_consolidate.computed_metric_specs) != 1:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Provided nodes should each compute exactly one derived metric",
                        derived_metrics_node=node_to_consolidate,
                    )
                )

        self._nodes_to_consolidate = nodes_to_consolidate.as_frozen()

        # Create a subplan to be able to use graph convenience methods (e.g. get all sources of a given node).
        self._subplan = MutableMetricEvaluationPlan.create()
        self._subplan.add_edges(corresponding_source_edges)

    def consolidate_nodes(self) -> tuple[OrderedSet[DerivedMetricsQueryNode], OrderedSet[MetricQueryDependencyEdge]]:
        """Return the consolidated nodes and the new edges."""
        new_nodes: MutableOrderedSet[DerivedMetricsQueryNode] = MutableOrderedSet()
        new_edges: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet()

        consolidation_key_to_nodes: defaultdict[NodeConsolidationKey, list[DerivedMetricsQueryNode]] = defaultdict(list)

        for node in self._nodes_to_consolidate:
            # As per check in the initializer, each node to consolidate should only compute a single metric.
            metric_spec = mf_first_item(node.computed_metric_specs)

            consolidation_key_to_nodes[
                NodeConsolidationKey(
                    aliased_metric=metric_spec if metric_spec.alias is not None else None,
                    source_node_set=self._subplan.source_nodes(node).as_frozen(),
                    query_properties=node.query_properties,
                )
            ].append(node)

        for consolidation_key, nodes in consolidation_key_to_nodes.items():
            # Keep aliased metrics as a separate query.
            if consolidation_key.aliased_metric is not None:
                new_nodes.update(nodes)
                new_edges.update(*(self._subplan.source_edges(node) for node in nodes))
                continue

            # Following the grouping, all nodes in `nodes` have the same sources and the same query properties.
            # For nodes without aliased metrics and sharing the same sources, combine them into a single node
            # that computes the derived metrics in all of them.
            consolidated_node, edges_for_consolidated_node = self._consolidate_nodes_for_non_aliased_metrics(
                nodes=nodes, query_properties=consolidation_key.query_properties
            )
            new_nodes.add(consolidated_node)
            new_edges.update(edges_for_consolidated_node)

        return (new_nodes, new_edges)

    def _consolidate_nodes_for_non_aliased_metrics(
        self, nodes: Sequence[DerivedMetricsQueryNode], query_properties: MetricQueryPropertySet
    ) -> tuple[DerivedMetricsQueryNode, Iterable[MetricQueryDependencyEdge]]:
        """Consolidate the given nodes to a single node that computes all metrics.

        The nodes must have the same source nodes and can't represent aliased metrics.
        """
        node_count = len(nodes)
        if node_count == 0:
            raise MetricFlowInternalError("No nodes passed in for consolidation.")
        elif node_count == 1:
            node = nodes[0]
            edges = self._subplan.source_edges(node)
            logger.debug(LazyFormat("No-op consolidation with a single node", node=node, edges=edges))
            return node, edges

        # Since the sources are the same, the passthrough metrics should be the same. However, if not, the
        # intersection should be safe. Log an error if that's not the case.
        passthrough_metric_specs_intersection: Optional[MutableOrderedSet[MetricSpec]] = None

        for node in nodes:
            if passthrough_metric_specs_intersection is None:
                passthrough_metric_specs_intersection = MutableOrderedSet(node.passthrough_metric_specs)
            passthrough_metric_specs_intersection.intersection_update(node.passthrough_metric_specs)

        for node in nodes:
            if node.passthrough_metric_specs != passthrough_metric_specs_intersection:
                logger.error(
                    LazyFormat(
                        "Expected all nodes to have the same pass through metrics. This may not cause issues,"
                        " but this may be a bug and should be investigated.",
                        node=node,
                        passthrough_metric_specs_intersection=passthrough_metric_specs_intersection,
                    )
                )
        assert passthrough_metric_specs_intersection is not None

        computed_metric_specs = FrozenOrderedSet(
            itertools.chain.from_iterable(node.computed_metric_specs for node in nodes)
        )

        consolidated_node = DerivedMetricsQueryNode.create(
            computed_metric_specs=computed_metric_specs,
            passthrough_metric_specs=passthrough_metric_specs_intersection,
            query_properties=query_properties,
        )

        original_source_edges: list[MetricQueryDependencyEdge] = []
        source_edges_for_consolidated_node: list[MetricQueryDependencyEdge] = []
        for node in nodes:
            for original_source_edge in self._subplan.source_edges(node):
                original_source_edges.append(original_source_edge)
                source_edges_for_consolidated_node.append(
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
        return consolidated_node, source_edges_for_consolidated_node


@fast_frozen_dataclass()
class NodeConsolidationKey:
    """A key to use for grouping nodes for consolidation."""

    # `aliased_metric` is only set for metrics with an alias. It ensures that each aliased metric maps to
    # a separate node.
    aliased_metric: Optional[MetricSpec]
    source_node_set: FrozenOrderedSet[MetricQueryNode]
    query_properties: MetricQueryPropertySet
