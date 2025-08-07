from __future__ import annotations

import logging
from collections.abc import Set
from typing import Optional

from typing_extensions import override

from metricflow_semantics.experimental.cache.mf_cache import ResultCache
from metricflow_semantics.experimental.metricflow_exception import MetricflowInternalError
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.mf_graph.path_finding.traversal_profile_differ import TraversalProfileDiffer
from metricflow_semantics.experimental.ordered_set import OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe import AttributeRecipe
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_weight import (
    AttributeRecipeWriterWeightFunction,
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
from metricflow_semantics.experimental.semantic_graph.trie_resolver.dunder_name_trie_resolver import (
    DunderNameTrieResolver,
    TrieCacheKey,
    TrieResolutionResult,
)
from metricflow_semantics.helpers.performance_helpers import ExecutionTimer
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS

logger = logging.getLogger(__name__)


class SimpleTrieResolver(DunderNameTrieResolver):
    """Resolves the dunder-name trie that represents the "simple" group-by items available for measures / metrics.

    The set of simple group-by items does not include group-by metrics Those are handled in a separate resolver.
    """

    def __init__(
        self,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowPathfinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
        max_path_model_count: Optional[int] = None,
    ) -> None:
        """Initializer.

        Args:
            semantic_graph: The semantic graph that should be traversed to resolve the available group-by items.
            path_finder: The pathfinder to use for traversal.
            max_path_model_count: To handle special cases, the number of semantic models in a path can be limited by
            setting this argument.
        """
        super().__init__(semantic_graph=semantic_graph, path_finder=path_finder)
        self._verbose_debug_logs = False
        self._result_cache: ResultCache[TrieCacheKey, DunderNameTrie] = ResultCache()
        self._max_path_model_count = max_path_model_count

    @override
    def resolve_trie(
        self, source_nodes: OrderedSet[SemanticGraphNode], element_filter: Optional[LinkableElementFilter]
    ) -> TrieResolutionResult:
        execution_timer = ExecutionTimer()
        traversal_stat_differ = TraversalProfileDiffer(self._path_finder)
        with execution_timer, traversal_stat_differ:
            result_trie = self._resolve_trie_for_source_nodes(source_nodes=source_nodes, element_filter=element_filter)

        return TrieResolutionResult(
            duration=execution_timer.total_duration,
            traversal_profile=traversal_stat_differ.profile_delta,
            dunder_name_trie=result_trie,
        )

    def _resolve_trie_for_source_nodes(
        self,
        source_nodes: OrderedSet[SemanticGraphNode],
        element_filter: Optional[LinkableElementFilter],
    ) -> DunderNameTrie:
        """Resolve the available group-by items for the given source nodes.

        The source nodes can be metric nodes, measure nodes, or local-model nodes. Similar to the query interface,
        the set of group-by items that are available for the source nodes is the intersection of the items that are
        available for each node.
        """
        # Find the set measures / local-model nodes that the given source nodes depend on. Generating the result for
        # the set of given source nodes requires intersecting the result produced from each node.
        find_descendants_result = self._path_finder.find_descendants(
            graph=self._semantic_graph,
            source_nodes=source_nodes,
            target_nodes=self._semantic_graph.nodes_with_labels(
                self._measure_label,
                self._local_model_label,
                self._metric_time_label,
            ),
            node_allow_set=self._semantic_graph.nodes_with_labels(
                self._metric_label,
                self._measure_label,
                self._local_model_label,
                self._metric_time_label,
            ),
            deny_labels={self._deny_visible_attributes_label},
        )

        # Match the behavior of a bug in the existing resolver in some cases with date part. This should be removed
        # after migration.
        if self._should_exclude_date_part(
            source_nodes=source_nodes, collected_labels=find_descendants_result.labels_collected_during_traversal
        ):
            if element_filter is None:
                element_filter = LinkableElementFilter()
            element_filter = element_filter.copy(
                without_any_of=element_filter.without_any_of.union((LinkableElementProperty.DATE_PART,))
            )

        result_intersection_source_nodes = tuple(find_descendants_result.reachable_target_nodes)
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Found result-intersection nodes.",
                    find_descendants_result=find_descendants_result,
                )
            )

        if len(result_intersection_source_nodes) == 0:
            raise MetricflowInternalError(
                LazyFormat("No applicable descendant nodes were found for intersection.", source_nodes=source_nodes)
            )

        result_trie = MutableDunderNameTrie.intersection_merge_common(
            tuple(
                self._resolve_trie_from_node(intersection_source_node, element_filter)
                for intersection_source_node in result_intersection_source_nodes
            )
        )

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Resolved intersection trie.",
                    result_trie=result_trie,
                )
            )
        return result_trie

    def _resolve_trie_from_node(
        self,
        source_node: SemanticGraphNode,
        element_filter: Optional[LinkableElementFilter],
    ) -> DunderNameTrie:
        source_node_labels = source_node.labels
        if self._local_model_label in source_node_labels or self._metric_time_label in source_node_labels:
            return self._resolve_trie_from_initial_path(
                initial_path=AttributeRecipeWriterPath.create(source_node),
                element_filter=element_filter,
            )

        # A measure node has 2 successors, a local-model node that represents where the measure is defined, and a
        # metric-time node. Since many measure nodes point to the same local-model node, generate and cache results
        # separately for each successor edge.
        elif self._measure_label in source_node_labels:
            successors = self._semantic_graph.successors(source_node)
            local_model_edge: Optional[SemanticGraphEdge] = None
            metric_time_edge: Optional[SemanticGraphEdge] = None
            edges_from_source_node = self._semantic_graph.edges_with_tail_node(source_node)
            for edge in edges_from_source_node:
                if self._local_model_label in edge.head_node.labels:
                    local_model_edge = edge
                elif self._metric_time_label in edge.head_node.labels:
                    metric_time_edge = edge

            if local_model_edge is None or metric_time_edge is None or len(successors) != 2:
                raise MetricflowInternalError(
                    LazyFormat(
                        "A measure node should have exactly 2 successors: a local-model node and a metric-time node"
                        "\nbut it does not.",
                        source_node=source_node,
                        source_node_labels=source_node_labels,
                        local_model_edge=local_model_edge,
                        metric_time_edge=metric_time_edge,
                        edges_from_source_node=edges_from_source_node,
                    )
                )
            result_from_local_model_node = self._resolve_trie_from_initial_path(
                initial_path=AttributeRecipeWriterPath.create(local_model_edge.head_node),
                element_filter=element_filter,
            )

            result_from_metric_time = self._resolve_trie_from_initial_path(
                initial_path=AttributeRecipeWriterPath.create_from_edge(metric_time_edge, 0),
                element_filter=element_filter,
            )
            union_result = MutableDunderNameTrie.union_exclude_common(
                (result_from_local_model_node, result_from_metric_time)
            )

            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Resolved trie union",
                        local_model=result_from_local_model_node.dunder_names(),
                        metric_time=result_from_metric_time.dunder_names(),
                        union_result=union_result.dunder_names(),
                    )
                )

            return MutableDunderNameTrie.union_exclude_common((result_from_local_model_node, result_from_metric_time))
        else:
            raise MetricflowInternalError(
                LazyFormat(
                    "The given node is not a supported source node for resolving attribute names",
                    source_node=source_node,
                )
            )

    def _resolve_trie_from_initial_path(
        self,
        initial_path: AttributeRecipeWriterPath,
        element_filter: Optional[LinkableElementFilter],
    ) -> DunderNameTrie:
        """Resolve the available group-by items using the given initial path."""
        cache_key = TrieCacheKey(key_nodes=tuple(initial_path.nodes), element_filter=element_filter)
        result = self._result_cache.get(cache_key)
        if result:
            return result.value

        target_nodes = self._semantic_graph.nodes_with_labels(self._group_by_attribute_label)
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Resolving names",
                    initial_path=initial_path,
                    element_filter=element_filter,
                )
            )

        result_trie = MutableDunderNameTrie()

        recipes: list[AttributeRecipe] = []
        for found_path in self._path_finder.find_paths_dfs(
            graph=self._semantic_graph,
            initial_path=initial_path,
            target_nodes=target_nodes,
            weight_function=AttributeRecipeWriterWeightFunction(
                element_filter, max_path_model_count=self._max_path_model_count
            ),
            max_path_weight=MAX_JOIN_HOPS,
            node_allow_set=None,
            node_deny_set=None,
        ):
            recipe = found_path.latest_recipe
            if self._verbose_debug_logs:
                logger.debug(LazyFormat("Found path to target node", path_nodes=found_path.nodes, recipe=recipe))
            if recipe is None:
                raise MetricflowInternalError(
                    LazyFormat(
                        "A path from a source node to a target node does not have a recipe. This indicates an error in "
                        "traversal."
                    )
                )
            recipes.append(recipe)

        result_trie.add_name_items(
            [
                (
                    recipe.indexed_dunder_name,
                    DunderNameDescriptor(
                        element_type=recipe.element_type,
                        time_grain=recipe.recipe_time_grain,
                        date_part=recipe.recipe_date_part,
                        element_properties=tuple(recipe.resolve_complete_properties()),
                        origin_model_ids=(recipe.joined_model_ids[-1],)
                        if recipe.joined_model_ids
                        else self._virtual_semantic_model_ids,
                        derived_from_model_ids=recipe.joined_model_ids
                        if recipe.joined_model_ids
                        else self._virtual_semantic_model_ids,
                        entity_key_queries_for_group_by_metric=(),
                    ),
                )
                for recipe in recipes
                if recipe.element_type is not None
            ]
        )

        return self._result_cache.set_and_get(cache_key, result_trie)

    def _should_exclude_date_part(
        self, source_nodes: Set[SemanticGraphNode], collected_labels: OrderedSet[MetricflowGraphLabel]
    ) -> bool:
        """To handle a bug, check if we should exclude date part from the query.

        This reproduces the logic for what seems like a bug in the current resolver. The date part is only not allowed
        if there's a cumulative metric in the query. A query for a derived metric that is based on a cumulative metric
        still can be queried by date part.
        """
        if self._deny_date_part_label not in collected_labels:
            return False

        for source_node in source_nodes:
            for successor_edge in self._semantic_graph.edges_with_tail_node(source_node):
                if self._cumulative_measure_label in successor_edge.labels:
                    return True
        return False
