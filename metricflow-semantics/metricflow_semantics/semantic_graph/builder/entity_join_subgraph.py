from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.edges.sg_edges import (
    EntityRelationshipEdge,
    JoinToModelEdge,
)
from metricflow_semantics.semantic_graph.lookups.join_lookup import SemanticModelJoinLookup
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.lookups.model_object_lookup import (
    ModelObjectLookup,
)
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.semantic_graph.nodes.entity_nodes import (
    ConfiguredEntityNode,
    JoinedModelNode,
    LocalModelNode,
)
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
)

logger = logging.getLogger(__name__)


class EntityJoinSubgraphGenerator(SemanticSubgraphGenerator):
    """Generator for the subgraph that represents the joins that are possible between semantic models.

    Following the current query interface, joins between semantic models are modeled as a path from a model node
    to a configured-entity node, and then to another model node.

    Also see `LocalModelNode` and `JoinedModelNode` for context on why two different nodes are required
    for each semantic model.
    """

    @override
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        super().__init__(manifest_object_lookup)
        self._verbose_debug_logs = True
        self._join_lookup = SemanticModelJoinLookup(manifest_object_lookup)

    @override
    def add_edges_for_manifest(self, edge_list: list[SemanticGraphEdge]) -> None:
        for lookup in self._manifest_object_lookup.model_object_lookups:
            self._add_edges_for_model(lookup, edge_list)

    def _add_edges_for_model(self, lookup: ModelObjectLookup, edge_list: list[SemanticGraphEdge]) -> None:
        left_model_id = SemanticModelId.get_instance(lookup.semantic_model.name)
        left_joined_model_node = JoinedModelNode.get_instance(left_model_id)
        left_local_model_node = LocalModelNode.get_instance(left_model_id)

        left_model = lookup.semantic_model

        # Add edges from the left model to the configured entities that can be reached by joining other semantic
        # models.
        #
        # e.g.
        #
        #   JoinedModel(bookings_source) -> ConfiguredEntity(listings_source.listings)
        #
        for right_model_id, join_descriptors in self._join_lookup.get_join_model_on_right_descriptors(
            left_model_id=left_model_id
        ).items():
            # Joining a semantic model to itself is currently not allowed.
            if right_model_id == left_model_id:
                continue

            # For all possible entities in the semantic model on the right.
            for join_descriptor in join_descriptors:
                right_entity_node = ConfiguredEntityNode.get_instance(
                    entity_name=join_descriptor.entity_name,
                    model_id=right_model_id,
                )
                # Add an edge from the joined-model node to the configured-entity node.
                edge_list.append(
                    JoinToModelEdge.create(
                        tail_node=left_joined_model_node,
                        head_node=right_entity_node,
                        right_model_id=right_model_id,
                    )
                )
                # Add an edge from the local-model node to the configured-entity node.
                edge_list.append(
                    JoinToModelEdge.create(
                        tail_node=left_local_model_node,
                        head_node=right_entity_node,
                        right_model_id=right_model_id,
                    )
                )

        # * The above loop created outgoing edges from the model node to the configured entity nodes.
        # * When this method is run with other models, it will do the same.
        # * From the configured-entity node, we need to add edges from the configured entity node to the model node so
        # * that there is a path from one model node to another.
        #
        # e.g.
        #   The previous loop added:
        #
        #       JoinedModel(bookings_source) -> ConfiguredEntity(listings_source.listings)
        #
        #   The following loop adds:
        #
        #       ConfiguredEntity(listings_source.listings) -> JoinedModel(listings_source)
        #
        # Combining those edges, we now have a path between the model nodes:
        #
        #   JoinedModel(bookings_source) -> ConfiguredEntity(listings_source.listings) -> JoinedModel(listings_source)
        #
        # In addition, we need to model the ability to query dimensions with an entity link, even if there's no actual
        # join. e.g. from `listings_source`, the `country_latest` dimension is accessed with `listing__country_latest`
        # but there is no join involved.
        #
        # This behavior can be modeled by adding the following edges.
        #
        #   LocalModel(listings_source) -> ConfiguredEntity(listing_source) -> JoinedModel(listings_source)
        valid_join_to_entity_types = self._join_lookup.valid_join_to_entity_types
        for entity in left_model.entities:
            if entity.type in valid_join_to_entity_types:
                entity_node = ConfiguredEntityNode.get_instance(
                    entity_name=entity.name,
                    model_id=left_model_id,
                )
                edge_list.append(
                    EntityRelationshipEdge.create(
                        tail_node=entity_node,
                        head_node=left_joined_model_node,
                    )
                )

                edge_list.append(
                    EntityRelationshipEdge.create(
                        tail_node=left_local_model_node,
                        head_node=entity_node,
                    )
                )

        # For semantic models with dimensions but no primary key, the `primary_entity` field is specified
        # as a virtual primary entity. In that case, the entity does not show up as an entity in
        # SemanticModel.entities, so add relevant edges.
        primary_entity_field_name = lookup.semantic_model.primary_entity
        if primary_entity_field_name is not None:
            primary_entity_node = ConfiguredEntityNode.get_instance(
                entity_name=primary_entity_field_name,
                model_id=left_model_id,
            )
            edge_list.append(
                EntityRelationshipEdge.create(
                    tail_node=left_local_model_node,
                    head_node=primary_entity_node,
                )
            )
            edge_list.append(
                EntityRelationshipEdge.create(
                    tail_node=primary_entity_node,
                    head_node=left_joined_model_node,
                )
            )
            # Since there's no join key, `primary_entity_node` is not accessible via a join from another model.
            # Consequently, no other edges are needed.
