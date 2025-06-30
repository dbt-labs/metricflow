from __future__ import annotations

import logging
from typing import Iterable, Optional

from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_flatten
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeComputationUpdate
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import (
    EntityRelationship,
    EntityRelationshipEdge,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import GroupByAttributeRootNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    GroupByAttributeLabel,
    JoinFromLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.weight_function import (
    EdgeCountWeightFunction,
    WeightFunction,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class GroupByAttributeSubgraphGenerator:
    # TODO: Replace this value with something more appropriate.
    _MAX_SEARCH_DEPTH = 10

    def __init__(
        self,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath],
    ) -> None:
        self._semantic_graph = semantic_graph
        self._path_finder = path_finder
        self._mutable_path = AttributeComputationPath.create()
        self._verbose_debug_logs = True

    def generate_subgraph(self, source_nodes: OrderedSet[SemanticGraphNode]) -> AttributeSubgraphResult:
        path_finder = self._path_finder
        semantic_graph = self._semantic_graph
        label = JoinFromLabel()
        # Since we're doing an intersection of the reachable `join_from_nodes` from all metrics, we can reduce the
        # search space by narrowing the set of target nodes to the current set of reachable nodes.
        current_common_join_from_nodes = MutableOrderedSet[SemanticGraphNode](semantic_graph.nodes_with_label(label))

        for i, metric_node in enumerate(source_nodes):
            result = path_finder.find_descendant_nodes(
                graph=self._semantic_graph,
                mutable_path=self._mutable_path,
                source_node=metric_node,
                candidate_target_nodes=current_common_join_from_nodes,
                max_path_weight=GroupByAttributeSubgraphGenerator._MAX_SEARCH_DEPTH,
                weight_function=BlockMetricDefinitionEdgeWeightFunction(),
            )
            current_common_join_from_nodes = current_common_join_from_nodes.intersection(result.descendant_nodes)

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Found common join-from nodes",
                    metric_nodes=source_nodes,
                    current_common_join_from_nodes=current_common_join_from_nodes,
                )
            )

        subgraph = MutableSemanticGraph.create()
        if len(current_common_join_from_nodes) == 0:
            subgraph.add_node(GroupByAttributeRootNode())
            return AttributeSubgraphResult(
                additional_derivative_model_ids=FrozenOrderedSet(),
                subgraph=subgraph,
                attribute_computation_updates=(),
            )

        for i, join_from_node in enumerate(current_common_join_from_nodes):
            subgraph_for_join_from_node = self._generate_subgraph_from_join_from_node(join_from_node)
            if i == 0:
                subgraph = subgraph_for_join_from_node
            else:
                subgraph = subgraph.intersection(subgraph_for_join_from_node)

        additional_derivative_model_ids = FrozenOrderedSet[SemanticModelId](
            mf_flatten(
                node.attribute_computation_update.derived_from_model_id_additions
                for node in current_common_join_from_nodes
            )
        )
        return AttributeSubgraphResult(
            additional_derivative_model_ids=additional_derivative_model_ids,
            attribute_computation_updates=tuple(
                node.attribute_computation_update for node in current_common_join_from_nodes
            ),
            subgraph=subgraph,
        )

    def _generate_subgraph_from_join_from_node(
        self,
        join_from_node: SemanticGraphNode,
    ) -> MutableSemanticGraph:
        path_finder = self._path_finder
        semantic_graph = self._semantic_graph

        result = path_finder.find_descendant_nodes(
            graph=self._semantic_graph,
            mutable_path=self._mutable_path,
            source_node=join_from_node,
            candidate_target_nodes=semantic_graph.nodes_with_label(GroupByAttributeLabel()),
            max_path_weight=GroupByAttributeSubgraphGenerator._MAX_SEARCH_DEPTH,
            weight_function=EdgeCountWeightFunction(),
        )

        nodes_in_path_to_group_by_attribute_nodes = result.descendant_nodes
        nodes_in_path_to_group_by_attribute_nodes.add(join_from_node)

        logger.debug(
            LazyFormat(
                "Found nodes in path to attribute nodes",
                nodes_in_path_to_group_by_attribute_nodes=nodes_in_path_to_group_by_attribute_nodes,
            )
        )

        subgraph_edges = semantic_graph.adjacent_edges(nodes_in_path_to_group_by_attribute_nodes)
        subgraph_edges = self._replace_join_to_node_with_group_by_attribute_root_node(
            join_from_node=join_from_node,
            edges=subgraph_edges,
        )
        subgraph = MutableSemanticGraph.create()
        subgraph.add_edges(subgraph_edges)
        return subgraph

    def _replace_join_to_node_with_group_by_attribute_root_node(
        self, join_from_node: SemanticGraphNode, edges: Iterable[SemanticGraphEdge]
    ) -> MutableOrderedSet[SemanticGraphEdge]:
        updated_edges = MutableOrderedSet[SemanticGraphEdge]()
        root_node = GroupByAttributeRootNode()
        for edge in edges:
            if edge.tail_node is join_from_node:
                updated_edges.add(
                    EntityRelationshipEdge.get_instance(
                        tail_node=root_node,
                        relationship=EntityRelationship.VALID,
                        head_node=edge.head_node,
                    )
                )
            else:
                updated_edges.add(edge)
        return updated_edges


@fast_frozen_dataclass()
class AttributeSubgraphResult:
    additional_derivative_model_ids: FrozenOrderedSet[SemanticModelId]
    attribute_computation_updates: AnyLengthTuple[AttributeComputationUpdate]
    subgraph: MutableSemanticGraph


class BlockMetricDefinitionEdgeWeightFunction(
    WeightFunction[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath]
):
    """Weight function that blocks traversal using metric definition edges.

    This reduces the search space for finding the group-by attribute nodes reachable from the metric node and prevents
    incorrect results.
    """

    @override
    def incremental_weight(
        self, path_to_node: AttributeComputationPath, edge_from_node: SemanticGraphEdge
    ) -> Optional[int]:
        # if SemanticGraphLabelFactory.get_metric_definition_label() in edge_from_node.labels:
        #     return None
        return 1
