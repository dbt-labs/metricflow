from __future__ import annotations

import dataclasses
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.cache.mf_cache import MetricflowCache, WeakValueDictCache
from metricflow_semantics.experimental.metricflow_exception import MetricflowAssertionError
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeDescriptor,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.dunder_name_weight import DunderNameWeightFunction
from metricflow_semantics.experimental.semantic_graph.builder.group_by_attribute_subgraph import \
    GroupByAttributeSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    EntityKeyAttributeNode,
    MetricNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import GroupByAttributeRootNode
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
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph, MutableSemanticGraph
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
        self._group_by_attribute_subgraph_generator = GroupByAttributeSubgraphGenerator(
            semantic_graph=self._semantic_graph,
            path_finder=self._path_finder,
        )

    def _resolve_attribute_descriptors(
        self,
        metric_names: Sequence[str],
        group_by_attribute_node_label: MetricflowGraphLabel = GroupByAttributeLabel(),
    ) -> Sequence[AttributeDescriptor]:
        raise NotImplementedError()

    # def resolve_descriptors_for_metric(
    #     self,
    #     metric_name: str,
    #     group_by_attribute_node_label: MetricflowGraphLabel = GroupByAttributeLabel(),
    # ) -> Sequence[AttributeDescriptor]:
    #     matching_nodes = self._semantic_graph.nodes_with_label(MetricAttributeLabel(metric_name=metric_name))
    #
    #     metric_node = mf_first_item(
    #         matching_nodes,
    #         lambda: MetricflowAssertionError(
    #             LazyFormat(
    #                 "Did not find exactly 1 node in the semantic graph with the given metric name",
    #                 metric_name=metric_name,
    #                 matching_nodes=matching_nodes,
    #             )
    #         ),
    #     )
    #
    #     group_by_attribute_nodes = self._semantic_graph.nodes_with_label(group_by_attribute_node_label)
    #     return self._resolve_descriptors_to_nodes(
    #         source_node=metric_node,
    #         target_nodes=group_by_attribute_nodes,
    #     )


    def _resolve_descriptors_for_metric_nodes(
        self,
        metric_nodes: OrderedSet[SemanticGraphNode],
        target_attribute_nodes: OrderedSet[SemanticGraphNode],
    ) -> AnyLengthTuple[AttributeDescriptor]:
        group_by_attribute_subgraph = self._group_by_attribute_subgraph_generator.generate_subgraph(
            metric_nodes=metric_nodes,
        )

        mutable_path = AttributeComputationPath.create()
        attribute_descriptors = []

        for path in self._path_finder.traverse_dfs(
                graph=group_by_attribute_subgraph,
                mutable_path=mutable_path,
                source_node=GroupByAttributeRootNode(),
                target_nodes=target_attribute_nodes,
                weight_function=DunderNameWeightFunction(),
                max_path_weight=3,
                allow_node_revisits=True,
        ):
            attribute_descriptor = mutable_path.attribute_computation.attribute_descriptor
            logger.debug(LazyFormat("Got path", path_nodes=path.nodes, descriptor=attribute_descriptor))
            if attribute_descriptor is not None:
                attribute_descriptors.append(attribute_descriptor)

        return tuple(attribute_descriptors)

    def resolve_descriptors_for_measure_node(
        self,
        measure_node: SemanticGraphNode,
    ) -> Sequence[AttributeDescriptor]:
        group_by_attribute_nodes = self._semantic_graph.nodes_with_label(GroupByAttributeLabel())

        return self._resolve_descriptors_to_nodes(
            source_node=measure_node,
            target_nodes=group_by_attribute_nodes,
        )

    def resolve_descriptors_for_metric(self, metric_name: str) -> Sequence[AttributeDescriptor]:
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
        attribute_nodes = self._semantic_graph.nodes_with_label(GroupByAttributeLabel())
        return self._resolve_descriptors_for_metric_nodes(
            metric_nodes=FrozenOrderedSet((metric_node,)),
            target_attribute_nodes=attribute_nodes,
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
        self, metric_subquery_pattern: MetricSubqueryPattern
    ) -> AnyLengthTuple[AnyLengthTuple[str]]:
        return self._attribute_resolver_cache.metric_subquery_to_subquery_entity_links.get_or_create(
            metric_subquery_pattern,
            factory=lambda: self._resolve_metric_subquery_links_uncached(metric_subquery_pattern),
        )

    def _resolve_metric_subquery_links_uncached(
        self, metric_subquery_pattern: MetricSubqueryPattern
    ) -> AnyLengthTuple[AnyLengthTuple[str]]:
        descriptors = self._resolve_descriptors_to_nodes(
            source_node=MetricNode(attribute_name=metric_subquery_pattern.metric_name),
            target_nodes=FrozenOrderedSet(
                (EntityKeyAttributeNode(attribute_name=metric_subquery_pattern.last_entity_link),)
            ),
        )

        results = tuple(descriptor.dundered_name_elements for descriptor in descriptors)

        return results


@singleton_dataclass()
class MetricSubqueryPattern:
    metric_name: str
    last_entity_link: str


@dataclass
class AttributeResolverCache(MetricflowCache):
    metric_name_to_attribute_descriptors: WeakValueDictCache[
        str, AnyLengthTuple[AttributeDescriptor]
    ] = dataclasses.field(default_factory=WeakValueDictCache)
    metric_subquery_to_subquery_entity_links: WeakValueDictCache[
        MetricSubqueryPattern, AnyLengthTuple[AnyLengthTuple[str]]
    ] = dataclasses.field(default_factory=WeakValueDictCache)
    measure_node_to_attribute_descriptors: WeakValueDictCache[
        SemanticGraphNode, AnyLengthTuple[AttributeDescriptor]
    ] = dataclasses.field(default_factory=WeakValueDictCache)
