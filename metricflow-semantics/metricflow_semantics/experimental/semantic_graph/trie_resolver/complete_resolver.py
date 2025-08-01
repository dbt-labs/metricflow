from __future__ import annotations

import logging
from typing import Optional

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.mf_graph.path_finding.traversal_profile_differ import TraversalProfileDiffer
from metricflow_semantics.experimental.ordered_set import OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import (
    SemanticGraph,
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.dunder_name_trie import MutableDunderNameTrie
from metricflow_semantics.experimental.semantic_graph.trie_resolver.dunder_name_trie_resolver import (
    DunderNameTrieResolver,
    TrieResolutionResult,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.group_by_metric_resolver import (
    GroupByMetricTrieResolver,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.simple_resolver import SimpleTrieResolver
from metricflow_semantics.helpers.performance_helpers import ExecutionTimer
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter

logger = logging.getLogger(__name__)


class CompleteTrieResolver(DunderNameTrieResolver):
    """A composite resolver that combines the output from `SimpleTrieResolver` and `GroupByMetricTrieResolver`."""

    def __init__(  # noqa: D107
        self,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowPathfinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
        max_path_model_count: Optional[int] = None,
    ) -> None:
        super().__init__(semantic_graph=semantic_graph, path_finder=path_finder)
        self._simple_resolver = SimpleTrieResolver(
            semantic_graph=semantic_graph, path_finder=path_finder, max_path_model_count=max_path_model_count
        )
        self._group_by_metric_resolver = GroupByMetricTrieResolver(
            semantic_graph=semantic_graph, path_finder=path_finder
        )

    @override
    def resolve_trie(
        self, source_nodes: OrderedSet[SemanticGraphNode], element_filter: Optional[LinkableElementFilter]
    ) -> TrieResolutionResult:
        execution_timer = ExecutionTimer()
        traversal_profile_differ = TraversalProfileDiffer(self._path_finder)
        with execution_timer, traversal_profile_differ:
            simple_result = self._simple_resolver.resolve_trie(
                source_nodes=source_nodes,
                element_filter=element_filter,
            )
            group_by_metric_result = self._group_by_metric_resolver.resolve_trie(
                source_nodes=source_nodes,
                element_filter=element_filter,
            )

        return TrieResolutionResult(
            execution_time=execution_timer.execution_time,
            traversal_profile=traversal_profile_differ.profile_delta,
            dunder_name_trie=MutableDunderNameTrie.union(
                (simple_result.dunder_name_trie, group_by_metric_result.dunder_name_trie)
            ),
        )
