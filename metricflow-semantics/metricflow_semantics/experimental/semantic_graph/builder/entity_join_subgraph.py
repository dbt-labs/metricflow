from __future__ import annotations

import logging

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import EntityType
from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import Pair
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import (
    EntityRelationship,
    EntityRelationshipEdge,
)
from metricflow_semantics.experimental.semantic_graph.edges.join_edge import JoinFromModelEdge, JoinToModelEdge
from metricflow_semantics.experimental.semantic_graph.edges.joined_dsi_entity import JoinedDsiEntityEdge
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.model_object_lookup import (
    SemanticModelObjectLookup,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    DsiEntityNode,
    JoinFromModelNode,
    JoinToModelNode,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


class EntityJoinSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)

    def _get_subgraph_for_model(
        self, lookup: SemanticModelObjectLookup
    ) -> Pair[MutableSemanticGraph, OrderedSet[JoinedDsiEntityEdge]]:
        current_subgraph = MutableSemanticGraph.create()

        model_id = SemanticModelId(model_name=lookup.semantic_model.name)

        join_to_semantic_model_node = JoinToModelNode(model_id=model_id)
        join_from_semantic_model_node = JoinFromModelNode(model_id=model_id)

        # List of `DsiEntityNode` that can be reached by joining to
        # this model (i.e. this model is on the right side of the join).
        valid_target_dsi_entity_nodes_for_joins_to_this_model: list[DsiEntityNode] = []
        # List of `DsiEntityNode` that can be reached by joining from
        # this model (this model is on the left side of the join).
        valid_target_dsi_entity_nodes_for_joins_from_this_model: list[DsiEntityNode] = []

        for entity in lookup.semantic_model.entities:
            dsi_entity_node = DsiEntityNode.get_instance(entity_name=entity.name)
            entity_type = entity.type
            if (
                entity_type is EntityType.PRIMARY
                or entity_type is EntityType.UNIQUE
                or entity_type is EntityType.NATURAL
            ):
                valid_target_dsi_entity_nodes_for_joins_to_this_model.append(dsi_entity_node)
                valid_target_dsi_entity_nodes_for_joins_from_this_model.append(dsi_entity_node)
            elif entity_type is EntityType.FOREIGN:
                valid_target_dsi_entity_nodes_for_joins_from_this_model.append(dsi_entity_node)
            else:
                assert_values_exhausted(entity_type)

        for entity_node in valid_target_dsi_entity_nodes_for_joins_to_this_model:
            current_subgraph.add_edge(
                JoinToModelEdge.get_instance(
                    tail_node=entity_node,
                    head_node=join_to_semantic_model_node,
                    left_model_id=model_id,
                )
            )

        for entity_node in valid_target_dsi_entity_nodes_for_joins_from_this_model:
            for right_model_id in self._manifest_object_lookup.entity_name_to_joinable_semantic_model_id[
                entity_node.entity_name
            ]:
                current_subgraph.add_edge(
                    JoinFromModelEdge.get_instance(
                        tail_node=join_from_semantic_model_node,
                        head_node=entity_node,
                        right_model_id=right_model_id,
                    )
                )
        entity_link_edges: MutableOrderedSet[JoinedDsiEntityEdge] = MutableOrderedSet()

        # for source_entity_node in valid_target_dsi_entity_nodes_for_joins_to_this_model:
        #     for target_entity_node in valid_target_dsi_entity_nodes_for_joins_from_this_model:
        #         if source_entity_node is not target_entity_node:
        #             for model_id in self._manifest_object_lookup.entity_name_to_joinable_semantic_model_id[
        #                 target_entity_node.entity_name
        #             ]:
        #                 entity_link_edge = JoinedDsiEntityEdge.get_instance(
        #                     tail_node=source_entity_node,
        #                     head_node=target_entity_node,
        #                     model_id=model_id,
        #                 )
        #                 entity_link_edges.add(entity_link_edge)

        return current_subgraph, entity_link_edges

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        current_entity_link_edges: MutableOrderedSet[JoinedDsiEntityEdge] = MutableOrderedSet()

        for lookup in self._manifest_object_lookup.model_object_lookups:
            # Add the primary entity to handle cases where that's the only thing in the model.
            if lookup.primary_entity_name is not None:
                join_from_semantic_model_node = JoinFromModelNode(model_id=lookup.model_id)
                join_to_semantic_model_node = JoinToModelNode(model_id=lookup.model_id)
                primary_entity_node = DsiEntityNode(entity_name=lookup.primary_entity_name)
                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=join_from_semantic_model_node,
                        head_node=primary_entity_node,
                        relationship=EntityRelationship.VALID,
                    )
                )
                current_subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=primary_entity_node,
                        head_node=join_to_semantic_model_node,
                        relationship=EntityRelationship.VALID,
                    )
                )

            # Get subgraph for joins to other entities.
            subgraph_from_model, entity_link_edges_from_model = self._get_subgraph_for_model(lookup)
            current_entity_link_edges.update(entity_link_edges_from_model)
            current_subgraph.update(subgraph_from_model)

        # Count the number of ways that you can go from one DSI entity to another.
        # entity_link_edge_node_pair_to_edge = defaultdict(list)
        # for entity_link_edge in current_entity_link_edges:
        #     entity_link_edge_node_pair_to_edge[entity_link_edge.node_pair].append(entity_link_edge)

        # If the number of ways is 1, then it means it's not ambiguous so we can add an edge.
        # for _, edges in entity_link_edge_node_pair_to_edge.items():
        #     if len(edges) == 1:
        #         current_subgraph.add_edges(edges)

        return current_subgraph
