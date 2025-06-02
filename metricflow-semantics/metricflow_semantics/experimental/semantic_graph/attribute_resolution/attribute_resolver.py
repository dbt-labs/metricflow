from __future__ import annotations

import logging
from collections.abc import Sequence

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.metricflow_exception import MetricflowAssertionError
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeDescriptor,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.dunder_name_weight import DunderNameWeightFunction
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    GroupByAttributeLabel,
    MeasureAttributeLabel,
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
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class AttributeResolver:
    def __init__(self, manifest_object_lookup: ManifestObjectLookup, semantic_graph: SemanticGraph) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._semantic_graph = semantic_graph
        self._path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath](
            path_finder_cache=PathFinderCache()
        )

    def resolve_attribute_descriptors(self, measure_name: str) -> Sequence[AttributeDescriptor]:
        matching_nodes = self._semantic_graph.nodes_with_label(MeasureAttributeLabel(measure_name=measure_name))

        measure_node = mf_first_item(
            matching_nodes,
            lambda: MetricflowAssertionError(
                LazyFormat(
                    "Did not find exactly 1 node in the semantic graph with the given measure name",
                    measure_name=measure_name,
                    matching_nodes=matching_nodes,
                )
            ),
        )
        group_by_attribute_nodes = self._semantic_graph.nodes_with_label(GroupByAttributeLabel())

        mutable_path = AttributeComputationPath.create()
        attribute_descriptors = []
        for path in self._path_finder.traverse_dfs(
            graph=self._semantic_graph,
            mutable_path=mutable_path,
            source_node=measure_node,
            target_nodes=group_by_attribute_nodes,
            weight_function=DunderNameWeightFunction(),
            max_path_weight=3,
            allow_node_revisits=True,
        ):
            attribute_descriptor = mutable_path.attribute_computation.attribute_descriptor
            logger.debug(LazyFormat("Got path", path_nodes=path.nodes, descriptor=attribute_descriptor))
            if attribute_descriptor is not None:
                attribute_descriptors.append(attribute_descriptor)

        return attribute_descriptors
