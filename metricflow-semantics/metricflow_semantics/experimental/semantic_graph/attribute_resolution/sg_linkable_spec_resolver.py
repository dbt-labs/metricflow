from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Optional

from dbt_semantic_interfaces.references import MeasureReference, MetricReference

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.metricflow_exception import MetricflowInternalError
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_resolver import AttributeResolver
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import MeasureLabel, MetricLabel
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import MetricflowGraphPathFinder
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseLinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import LinkableSpecResolver

logger = logging.getLogger(__name__)


class SemanticGraphLinkableSpecResolver(LinkableSpecResolver):
    """An implementation of `LinkableSpecResolver` using the semantic graph."""

    def __init__(
        self,
        manifest_object_lookup: ManifestObjectLookup,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
    ) -> None:  # noqa: D107
        self._semantic_graph = semantic_graph
        self._attribute_resolver = AttributeResolver(
            manifest_object_lookup=manifest_object_lookup, semantic_graph=semantic_graph, path_finder=path_finder
        )

    def get_linkable_element_set_for_measure(
        self, measure_reference: MeasureReference, element_filter: Optional[LinkableElementFilter] = None
    ) -> BaseLinkableElementSet:
        matching_measure_nodes = self._semantic_graph.nodes_with_label(
            MeasureLabel.get_instance(measure_name=measure_reference.element_name)
        )
        if len(matching_measure_nodes) != 1:
            raise MetricflowInternalError(
                LazyFormat(
                    "Did not find exactly 1 node in the semantic graph for the given measure",
                    measure_reference=measure_reference,
                    matching_measure_nodes=matching_measure_nodes,
                )
            )

        measure_node = mf_first_item(matching_measure_nodes)

        result = self._attribute_resolver.resolve_annotated_specs(
            FrozenOrderedSet((measure_node,)), element_filter=element_filter
        )

        return result

    def get_linkable_elements_for_distinct_values_query(
        self, element_filter: LinkableElementFilter
    ) -> BaseLinkableElementSet:
        return self._attribute_resolver.resolve_metric_time_specs(element_filter=element_filter)

    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        element_filter: Optional[LinkableElementFilter] = None,
    ) -> BaseLinkableElementSet:
        all_metric_nodes: MutableOrderedSet[SemanticGraphNode] = MutableOrderedSet()

        # Handling old behavior - you can't use this method to get group-by metrics.
        without_any_of_set = frozenset((LinkableElementProperty.METRIC,))
        if element_filter is None or element_filter == LinkableElementFilter():
            element_filter = LinkableElementFilter(without_any_of=without_any_of_set)
        else:
            element_filter = element_filter.copy(without_any_of=element_filter.without_any_of.union(without_any_of_set))

        for metric_reference in metric_references:
            matching_metric_nodes = self._semantic_graph.nodes_with_label(
                MetricLabel.get_instance(metric_reference.element_name)
            )
            if len(matching_metric_nodes) != 1:
                raise MetricflowInternalError(
                    LazyFormat(
                        "Did not find exactly 1 node in the semantic graph for the given metric",
                        metric_reference=metric_reference,
                        matching_measure_nodes=matching_metric_nodes,
                    )
                )
            all_metric_nodes.add(mf_first_item(matching_metric_nodes))

        return self._attribute_resolver.resolve_annotated_specs(
            source_nodes=all_metric_nodes,
            element_filter=element_filter,
        )
