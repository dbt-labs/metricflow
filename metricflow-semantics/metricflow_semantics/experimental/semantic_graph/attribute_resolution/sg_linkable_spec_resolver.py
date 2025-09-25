from __future__ import annotations

import logging
from collections import defaultdict
from typing import Iterable, Optional

from dbt_semantic_interfaces.references import ElementReference, MeasureReference, MetricReference
from typing_extensions import override

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.cache.mf_cache import ResultCache
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.metricflow_exception import MetricflowInternalError
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.annotated_spec_linkable_element_set import (
    GroupByItemSet,
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
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import (
    GroupByItemSetResolver,
)

logger = logging.getLogger(__name__)


class SemanticGraphGroupByItemSetResolver(GroupByItemSetResolver):
    """An implementation of `GroupByItemSetResolver` using the semantic graph."""

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

        self._result_cache_for_distinct_values: ResultCache[
            tuple[Optional[GroupByItemSetFilter]], BaseGroupByItemSet
        ] = ResultCache()

        self._result_cache_for_common_set: ResultCache[_CommonSetCacheKey, BaseGroupByItemSet] = ResultCache()

    @override
    def get_set_for_distinct_values_query(self, element_filter: GroupByItemSetFilter) -> BaseGroupByItemSet:
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
            cache_key, GroupByItemSet.create_from_trie(result_trie)
        )

    @override
    def get_common_set(
        self,
        measure_references: Iterable[MeasureReference] = (),
        metric_references: Iterable[MetricReference] = (),
        set_filter: Optional[GroupByItemSetFilter] = None,
    ) -> BaseGroupByItemSet:
        label_to_references: defaultdict[MetricflowGraphLabel, set[ElementReference]] = defaultdict(set)
        for measure_reference in measure_references:
            label_to_references[MeasureLabel.get_instance(measure_reference.element_name)].add(measure_reference)
        for metric_reference in metric_references:
            label_to_references[MetricLabel.get_instance(metric_reference.element_name)].add(metric_reference)

        # Sanity check.
        for label, references in label_to_references.items():
            assert len(references) == 1, LazyFormat(
                "Different references map to the same label.",
                references=references,
                label=label,
            )

        if len(label_to_references) == 0:
            return GroupByItemSet()

        node_labels: FrozenOrderedSet[MetricflowGraphLabel] = FrozenOrderedSet(sorted(label_to_references))
        cache_key = _CommonSetCacheKey(node_labels=node_labels, set_filter=set_filter)
        cached_result = self._result_cache_for_common_set.get(cache_key)
        if cached_result:
            return cached_result.value

        source_nodes: MutableOrderedSet[SemanticGraphNode] = MutableOrderedSet()

        for label in node_labels:
            matching_nodes = self._semantic_graph.nodes_with_labels(label)
            if len(matching_nodes) != 1:
                raise MetricflowInternalError(
                    LazyFormat(
                        "Did not find exactly 1 node in the semantic graph for the given label",
                        label=label,
                        associated_references=label_to_references.get(label),
                        matching_nodes=matching_nodes,
                        measure_nodes=lambda: self._semantic_graph.nodes_with_labels(MeasureLabel.get_instance()),
                        metric_nodes=lambda: self._semantic_graph.nodes_with_labels(MetricLabel.get_instance()),
                    )
                )

            source_nodes.add(mf_first_item(matching_nodes))

        simple_trie = self._simple_resolver.resolve_trie(source_nodes, set_filter).dunder_name_trie
        group_by_metric_trie = self._group_by_metric_resolver.resolve_trie(source_nodes, set_filter).dunder_name_trie
        return self._result_cache_for_common_set.set_and_get(
            cache_key, GroupByItemSet.create_from_trie(simple_trie, group_by_metric_trie)
        )


@fast_frozen_dataclass()
class _CommonSetCacheKey:
    node_labels: FrozenOrderedSet[MetricflowGraphLabel]
    set_filter: Optional[GroupByItemSetFilter]
