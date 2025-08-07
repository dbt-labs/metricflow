from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import (
    MetricflowPathfinder,
)
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder_result import GraphTraversalResult
from metricflow_semantics.experimental.ordered_set import OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import (
    CumulativeMeasureLabel,
    DenyDatePartLabel,
    DenyVisibleAttributesLabel,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import (
    GroupByAttributeLabel,
    KeyAttributeLabel,
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
from metricflow_semantics.experimental.semantic_graph.trie_resolver.dunder_name_trie import (
    DunderNameTrie,
)
from metricflow_semantics.helpers.time_helpers import PrettyDuration
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter

logger = logging.getLogger(__name__)


class DunderNameTrieResolver(ABC):
    """ABC for classes that resolve the available group-by items for a query, returned as a trie."""

    def __init__(  # noqa: D107
        self,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowPathfinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
    ) -> None:
        self._semantic_graph = semantic_graph
        self._path_finder = path_finder

        # The below are used for convenient autocomplete in the IDE. There are some additional constants that can
        # be shared between the resolvers - consolidation pending.
        self._cumulative_measure_label = CumulativeMeasureLabel.get_instance()
        self._deny_date_part_label = DenyDatePartLabel.get_instance()
        self._measure_label = MeasureLabel.get_instance()
        self._local_model_label = LocalModelLabel.get_instance()
        self._metric_time_label = MetricTimeLabel.get_instance()
        self._metric_label = MetricLabel.get_instance()
        self._deny_visible_attributes_label = DenyVisibleAttributesLabel.get_instance()
        self._entity_key_attribute_label = KeyAttributeLabel.get_instance()
        self._virtual_semantic_model_ids = (
            SemanticModelId.get_instance(SemanticModelDerivation.VIRTUAL_SEMANTIC_MODEL_REFERENCE.semantic_model_name),
        )
        self._group_by_attribute_label = GroupByAttributeLabel.get_instance()

    @abstractmethod
    def resolve_trie(
        self, source_nodes: OrderedSet[SemanticGraphNode], element_filter: Optional[LinkableElementFilter]
    ) -> TrieResolutionResult:
        """Resolve the trie that represents the available group-by items when querying those nodes together.

        e.g. A query for the `bookings` metric and the `views` metric would have the `Metric(bookings)` node and
        the `Metric(views)` node as source nodes.

        When there are multiple source nodes, the available group-by items are the intersection of the set of items
        available to each source node.
        """
        raise NotImplementedError


@fast_frozen_dataclass()
class TrieCacheKey:
    """A key object to use for caching results."""

    key_nodes: AnyLengthTuple[SemanticGraphNode]
    element_filter: Optional[LinkableElementFilter]


@dataclass
class TrieResolutionResult(GraphTraversalResult):
    """A result object that contains some additional fields that describe the performance of resolution.

    Those fields were mostly helpful for debugging - this may be considered for removal.
    """

    duration: PrettyDuration
    dunder_name_trie: DunderNameTrie
