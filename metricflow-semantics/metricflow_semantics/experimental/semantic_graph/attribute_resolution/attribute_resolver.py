from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.collection_helpers.syntatic_sugar_wip import mf_get_or_else
from metricflow_semantics.experimental.cache.mf_cache import MetricflowCache, WeakValueDictCache
from metricflow_semantics.experimental.metricflow_exception import MetricflowAssertionError
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.ordered_set import OrderedSet, FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeDescriptor,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.dunder_name_weight import DunderNameWeightFunction
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import MetricAttributeNode, \
    EntityKeyAttributeNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    GroupByAttributeLabel,
    MetricAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_cache import PathFinderCache
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType

logger = logging.getLogger(__name__)


class AttributeResolver:
    def __init__(
            self,
            manifest_object_lookup: ManifestObjectLookup,
            semantic_graph: SemanticGraph,
            attribute_resolver_cache: AttributeResolverCache,
    ) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._semantic_graph = semantic_graph
        self._path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath](
            path_finder_cache=PathFinderCache()
        )
        self._attribute_resolver_cache = attribute_resolver_cache

    def _resolve_attribute_descriptors(
            self,
            metric_names: Sequence[str],
            group_by_attribute_node_label: MetricflowGraphLabel = GroupByAttributeLabel(),
    ) -> Sequence[AttributeDescriptor]:
        source_nodes =

    def _resolve_attribute_descriptors_for_one_metric(
            self,
            metric_name: str,
            group_by_attribute_node_label: MetricflowGraphLabel = GroupByAttributeLabel(),
    ) -> Sequence[AttributeDescriptor]:
        matching_nodes = self._semantic_graph.nodes_with_label(MetricAttributeLabel(metric_name=metric_name))

        metric_node = mf_first_item(
            matching_nodes,
            lambda: MetricflowAssertionError(
                LazyFormat(
                    "Did not find exactly 1 node in the semantic graph with the given metric name",
                    metric_name=metric_name,
                    matching_nodes=matching_nodes,
                )
            ),
        )


        group_by_attribute_nodes = self._semantic_graph.nodes_with_label(group_by_attribute_node_label)
        return self._resolve_descriptors_to_nodes(
            source_node=metric_node,
            target_nodes=group_by_attribute_nodes,
        )

    def _resolve_descriptors_to_nodes(
            self,
            source_node: SemanticGraphNode,
            target_nodes: OrderedSet[SemanticGraphNode],
    ) -> Sequence[AttributeDescriptor]:

        mutable_path = AttributeComputationPath.create()
        attribute_descriptors = []

        for path in self._path_finder.traverse_dfs(
            graph=self._semantic_graph,
            mutable_path=mutable_path,
            source_node=source_node,
            target_nodes=target_nodes,
            weight_function=DunderNameWeightFunction(),
            max_path_weight=3,
            allow_node_revisits=True,
        ):
            attribute_descriptor = mutable_path.attribute_computation.attribute_descriptor
            logger.debug(LazyFormat("Got path", path_nodes=path.nodes, descriptor=attribute_descriptor))
            if attribute_descriptor is not None:
                attribute_descriptors.append(attribute_descriptor)

        return attribute_descriptors

    def _generate_metric_subquery_entity_links(self, attribute_descriptors: Sequence[AttributeDescriptor]) -> None:
        for attribute_descriptor in attribute_descriptors:
            element_type = mf_first_item(attribute_descriptor.element_types)

            if element_type is LinkableElementType.DIMENSION:
                pass
            elif element_type is LinkableElementType.ENTITY:
                pass
            elif element_type is LinkableElementType.METRIC:
                pass
            elif element_type is LinkableElementType.TIME_DIMENSION:
                pass
            else:
                assert_values_exhausted(element_type)

            raise NotImplementedError

    def _resolve_metric_subquery_links_cached(
            self,
            metric_subquery_pattern: MetricSubqueryPattern
    ) -> AnyLengthTuple[AnyLengthTuple[str]]:
        return self._attribute_resolver_cache.metric_subquery_to_subquery_entity_links.get_or_create(
            metric_subquery_pattern,
            factory=lambda: self._resolve_metric_subquery_links_uncached(metric_subquery_pattern)
        )

    def _resolve_metric_subquery_links_uncached(
            self,
            metric_subquery_pattern: MetricSubqueryPattern
    ) -> AnyLengthTuple[AnyLengthTuple[str]]:
        descriptors = self._resolve_descriptors_to_nodes(
            source_node=MetricAttributeNode(
                attribute_name=metric_subquery_pattern.metric_name
            ),
            target_nodes=FrozenOrderedSet((EntityKeyAttributeNode(
                attribute_name=metric_subquery_pattern.last_entity_link
            ),)
        ))

        results = tuple(
            descriptor.dundered_name_elements for descriptor in descriptors
        )

        return results



@singleton_dataclass()
class MetricSubqueryPattern:
    metric_name: str
    last_entity_link: str


@dataclass
class AttributeResolverCache(WeakValueDictCache):
    metric_name_to_attribute_descriptors: WeakValueDictCache[str, AnyLengthTuple[AttributeDescriptor]]
    metric_subquery_to_subquery_entity_links: WeakValueDictCache[MetricSubqueryPattern, AnyLengthTuple[str]]