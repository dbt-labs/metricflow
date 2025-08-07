from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Optional

from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from typing_extensions import override

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.cache.mf_cache import ResultCache
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.metricflow_exception import MetricflowInternalError
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.annotated_spec_linkable_element_set import (
    AnnotatedSpecLinkableElementSet,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe import IndexedDunderName
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import (
    LocalModelLabel,
    MeasureLabel,
    MetricLabel,
    MetricTimeLabel,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import (
    SemanticGraph,
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.dunder_name_descriptor import DunderNameDescriptor
from metricflow_semantics.experimental.semantic_graph.trie_resolver.dunder_name_trie import (
    DunderNameTrie,
    MutableDunderNameTrie,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.group_by_metric_resolver import (
    GroupByMetricTrieResolver,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.simple_resolver import SimpleTrieResolver
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseLinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import LinkableSpecResolver

logger = logging.getLogger(__name__)


class SemanticGraphLinkableSpecResolver(LinkableSpecResolver):
    """An implementation of `LinkableSpecResolver` using the semantic graph."""

    def __init__(  # noqa: D107
        self,
        semantic_graph: SemanticGraph,
        manifest_object_lookup: ManifestObjectLookup,
        path_finder: MetricflowPathfinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
    ) -> None:
        self._semantic_graph = semantic_graph
        self._pathfinder = path_finder
        self._manifest_object_lookup = manifest_object_lookup
        self._simple_resolver = SimpleTrieResolver(semantic_graph, path_finder)
        self._simple_resolver_limit_one_model = SimpleTrieResolver(semantic_graph, path_finder, max_path_model_count=1)
        self._group_by_metric_resolver = GroupByMetricTrieResolver(semantic_graph, path_finder)
        self._local_model_nodes = self._semantic_graph.nodes_with_labels(LocalModelLabel.get_instance())
        self._metric_time_node_set = FrozenOrderedSet(
            (self._semantic_graph.node_with_label(MetricTimeLabel.get_instance()),)
        )

        self._result_cache_for_measure: ResultCache[
            tuple[MeasureReference, Optional[LinkableElementFilter]], BaseLinkableElementSet
        ] = ResultCache()

        self._result_cache_for_metrics: ResultCache[
            tuple[FrozenOrderedSet[MetricReference], Optional[LinkableElementFilter]], BaseLinkableElementSet
        ] = ResultCache()

        self._result_cache_for_distinct_values: ResultCache[
            tuple[Optional[LinkableElementFilter]], BaseLinkableElementSet
        ] = ResultCache()

    @override
    def get_linkable_element_set_for_measure(
        self, measure_reference: MeasureReference, element_filter: Optional[LinkableElementFilter] = None
    ) -> BaseLinkableElementSet:
        cache_key = (measure_reference, element_filter)
        cached_result = self._result_cache_for_measure.get(cache_key)
        if cached_result:
            return cached_result.value

        initial_traversal_profile = self._pathfinder.traversal_profile_snapshot

        matching_measure_nodes = self._semantic_graph.nodes_with_labels(
            MeasureLabel.get_instance(measure_name=measure_reference.element_name)
        )
        if len(matching_measure_nodes) != 1:
            raise MetricflowInternalError(
                LazyFormat(
                    "Did not find exactly 1 node in the semantic graph for the given measure",
                    measure_reference=measure_reference,
                    matching_measure_nodes=matching_measure_nodes,
                    measure_nodes=lambda: self._semantic_graph.nodes_with_labels(MeasureLabel.get_instance()),
                    graph_nodes=self._semantic_graph.nodes,
                )
            )

        measure_node = mf_first_item(matching_measure_nodes)
        source_nodes = FrozenOrderedSet((measure_node,))

        simple_trie = self._simple_resolver.resolve_trie(source_nodes, element_filter).dunder_name_trie
        group_by_metric_trie = self._group_by_metric_resolver.resolve_trie(
            source_nodes, element_filter
        ).dunder_name_trie

        logger.info(
            LazyFormat(
                "Logging traversal-profile delta:",
                delta=self._pathfinder.traversal_profile_snapshot.difference(initial_traversal_profile),
            )
        )

        return self._result_cache_for_measure.set_and_get(
            cache_key, AnnotatedSpecLinkableElementSet.create_from_trie(simple_trie, group_by_metric_trie)
        )

    @override
    def get_linkable_elements_for_distinct_values_query(
        self, element_filter: LinkableElementFilter
    ) -> BaseLinkableElementSet:
        cache_key = (element_filter,)
        cache_result = self._result_cache_for_distinct_values.get(cache_key)
        if cache_result:
            return cache_result.value
        tries_to_union: list[DunderNameTrie] = []
        for local_model_node in self._local_model_nodes:
            source_nodes = FrozenOrderedSet((local_model_node,))
            local_model_trie = self._simple_resolver_limit_one_model.resolve_trie(
                source_nodes, element_filter
            ).dunder_name_trie
            group_by_metric_trie = self._group_by_metric_resolver.resolve_trie(
                source_nodes, element_filter
            ).dunder_name_trie
            tries_to_union.append(local_model_trie)
            tries_to_union.append(group_by_metric_trie)

        metric_time_trie = self._simple_resolver.resolve_trie(
            self._metric_time_node_set, element_filter
        ).dunder_name_trie

        # Since `TimeEntitySubgraphGenerator` could add time grain nodes that are finer than the grain of time spine,
        # filter those out.
        int_value_of_min_time_spine_grain = self._manifest_object_lookup.min_time_grain_in_time_spine.to_int()
        filtered_items_from_metric_time_trie: list[tuple[IndexedDunderName, DunderNameDescriptor]] = []

        for indexed_dunder_name, descriptor in metric_time_trie.name_items():
            # Allow `metric_time` at grains >= min grain.
            if (
                descriptor.time_grain is not None
                and descriptor.time_grain.base_granularity.to_int() >= int_value_of_min_time_spine_grain
            ):
                filtered_items_from_metric_time_trie.append((indexed_dunder_name, descriptor))
            # Allow `metric_time` with a date part compatible with the min grain.
            elif (
                descriptor.date_part is not None and descriptor.date_part.to_int() >= int_value_of_min_time_spine_grain
            ):
                filtered_items_from_metric_time_trie.append((indexed_dunder_name, descriptor))

        filtered_metric_time_trie = MutableDunderNameTrie()
        filtered_metric_time_trie.add_name_items(filtered_items_from_metric_time_trie)

        tries_to_union.append(filtered_metric_time_trie)
        result_trie = MutableDunderNameTrie.union_merge_common(tries_to_union)

        return self._result_cache_for_distinct_values.set_and_get(
            cache_key, AnnotatedSpecLinkableElementSet.create_from_trie(result_trie)
        )

    @override
    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        element_filter: Optional[LinkableElementFilter] = None,
    ) -> BaseLinkableElementSet:
        if len(metric_references) == 0:
            return AnnotatedSpecLinkableElementSet()

        cache_key = (FrozenOrderedSet(sorted(metric_references)), element_filter)
        cache_result = self._result_cache_for_metrics.get(cache_key)
        if cache_result:
            return cache_result.value

        all_metric_nodes: MutableOrderedSet[SemanticGraphNode] = MutableOrderedSet()

        # Handling old behavior - you can't use this method to get group-by metrics.
        without_any_of_set = frozenset((LinkableElementProperty.METRIC,))
        if element_filter is None or element_filter == LinkableElementFilter():
            element_filter = LinkableElementFilter(without_any_of=without_any_of_set)
        else:
            element_filter = element_filter.copy(without_any_of=element_filter.without_any_of.union(without_any_of_set))

        for metric_reference in metric_references:
            matching_metric_nodes = self._semantic_graph.nodes_with_labels(
                MetricLabel.get_instance(metric_reference.element_name)
            )
            if len(matching_metric_nodes) != 1:
                raise MetricflowInternalError(
                    LazyFormat(
                        "Did not find exactly 1 node in the semantic graph for the given metric",
                        metric_reference=metric_reference,
                        matching_metric_nodes=matching_metric_nodes,
                        metric_nodes=self._semantic_graph.nodes_with_labels(MetricLabel.get_instance()),
                    )
                )
            all_metric_nodes.add(mf_first_item(matching_metric_nodes))

        simple_trie = self._simple_resolver.resolve_trie(all_metric_nodes, element_filter).dunder_name_trie
        group_by_metric_trie = self._group_by_metric_resolver.resolve_trie(
            all_metric_nodes, element_filter
        ).dunder_name_trie
        return self._result_cache_for_metrics.set_and_get(
            cache_key, AnnotatedSpecLinkableElementSet.create_from_trie(simple_trie, group_by_metric_trie)
        )
