from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Set

from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import SemanticModelReference

from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.experimental.semantic_graph.graph_edges import (
    Cardinality,
)
from metricflow_semantics.experimental.semantic_graph.graph_nodes import (
    AssociativeEntityNode,
    EntityNode,
    SemanticEntityType,
)
from metricflow_semantics.experimental.semantic_graph.graph_path.path_property import (
    AppendJoinPathAddition,
    JoinPathAddition,
)
from metricflow_semantics.experimental.semantic_graph.ids.entity_ids import ElementEntityId

logger = logging.getLogger(__name__)


# @dataclass(frozen=True)
# class EntityJoinPath:
#     nodes: Tuple[EntityNode, ...]
#     edges: Tuple[SemanticGraphEdge, ...]
#
#     @cached_property
#     def node_set(self) -> FrozenSet[EntityNode]:
#         return frozenset(self.nodes)
#
#     def contains_node(self, node: EntityNode) -> bool:
#         return node in self.node_set
#
#     def with_added_edge(self, edge: SemanticGraphEdge) -> EntityJoinPath:
#         return EntityJoinPath(
#             nodes=self.nodes + (edge.head_node,),
#             edges=self.edges + (edge,),
#         )


@dataclass(frozen=True)
class ExtensionResult:
    added_nodes: FrozenSet[EntityNode] = frozenset()


class ConnectJoinableEntitiesRecipe:
    def __init__(self, semantic_manifest: SemanticManifest) -> None:
        self._semantic_manifest = semantic_manifest
        self._entity_id_to_next_join_path_addition: Dict[ElementEntityId, List[JoinPathAddition]] = defaultdict(list)
        self._semantic_model_to_available_entities: Dict[SemanticModelReference, List[ElementEntityId]] = defaultdict(
            list
        )

        for semantic_model in self._semantic_manifest.semantic_models:
            for entity in semantic_model.entities:
                semantic_model_reference = semantic_model.reference
                if Cardinality.get_for_entity_type(entity.type) is Cardinality.ONE:
                    self._entity_id_to_next_join_path_addition[
                        ElementEntityId.get_instance(
                            element_name=entity.name,
                            entity_type=SemanticEntityType.ENTITY,
                        )
                    ].append(
                        JoinPathAddition(
                            right_semantic_model_reference=semantic_model_reference,
                            join_on_entity=entity.reference,
                        )
                    )
                self._semantic_model_to_available_entities[semantic_model_reference].append(
                    ElementEntityId.get_instance(
                        element_name=entity.name,
                        entity_type=SemanticEntityType.ENTITY,
                    )
                )

    def add_edges_to_joinable_entities(
        self,
        semantic_graph: InProgressSemanticGraph,
        tail_node: EntityNode,
        iteration_count: int,
    ) -> None:
        raise NotImplementedError

    def add_next_edge(self, semantic_graph: InProgressSemanticGraph, tail_node: EntityNode) -> ExtensionResult:
        semantic_model_entity_id = tail_node.entity_id.semantic_model_entity_id

        if semantic_model_entity_id is None:
            return ExtensionResult()
        added_nodes: Set[EntityNode] = set()

        for join_path_addition in self._entity_id_to_next_join_path_addition[semantic_model_entity_id]:
            for next_entity_id in self._semantic_model_to_available_entities[
                join_path_addition.right_semantic_model_reference
            ]:
                next_entity_node = AssociativeEntityNode.get_instance(
                    entity_id=next_entity_id, source_entity_id=tail_node.entity_id
                )
                if not semantic_graph.contains_node(next_entity_node):
                    semantic_graph.add_node(next_entity_node)
                    added_nodes.add(next_entity_node)

                semantic_graph.add_edge(
                    tail_node=tail_node,
                    head_node=next_entity_node,
                    join_operations=(AppendJoinPathAddition(join_path_addition),),
                )

        return ExtensionResult(frozenset(added_nodes))

    # def _add_edges_for_joined_semantic_model(
    #     self,
    #     join_on_entity_reference: EntityReference,
    #     join_on_entity_type_in_right_semantic_model: EntityType,
    #     right_semantic_model: SemanticModel,
    #     semantic_graph: InProgressSemanticGraph,
    # ) -> None:
    #     right_entity_node = EntityNode.get_instance(join_on_entity_reference.element_name, SemanticEntityType.ENTITY)
    #     for left_semantic_model in self._semantic_model_lookup.get_semantic_models_for_entity(join_on_entity_reference):
    #         # Don't do self-joins.
    #         if right_semantic_model.reference == left_semantic_model.reference:
    #             continue
    #
    #         # Handle primary entity
    #         # primary_entity_reference = left_semantic_model.primary_entity_reference
    #         # if primary_entity_reference is not None and primary_entity_reference not in {
    #         #     entity.reference for entity in left_semantic_model.entities
    #         # }:
    #         #     left_entity_node = EntityNode(primary_entity_reference)
    #         #     semantic_graph.add_edge(
    #         #         tail_node=left_entity_node,
    #         #         edge_type=SemanticGraphEdgeType.get_for_entity_types(
    #         #             tail_entity_type=EntityType.PRIMARY,
    #         #             head_entity_type=join_on_entity_type_in_right_semantic_model,
    #         #         ),
    #         #         head_node=right_entity_node,
    #         #         computation_method=JoinedComputationMethod(
    #         #             left_semantic_model_reference=left_semantic_model.reference,
    #         #             right_semantic_model_reference=right_semantic_model.reference,
    #         #             on_entity_reference=join_on_entity_reference,
    #         #         ),
    #         #         provided_tags=ProvidedEdgeTagSet.empty_set(),
    #         #         required_tags=RequiredTagSet.empty_set(),
    #         #     )
    #
    #         for entity_in_left_semantic_model in left_semantic_model.entities:
    #             if (
    #                 join_on_entity_reference == entity_in_left_semantic_model.reference
    #                 or Cardinality.get_for_entity_type(entity_in_left_semantic_model.type) is not Cardinality.ONE
    #             ):
    #                 continue
    #             left_entity_node = EntityNode.get_instance(
    #                 entity_in_left_semantic_model.reference.element_name,
    #                 SemanticEntityType.ENTITY,
    #             )
    #             semantic_graph.add_edge(
    #                 tail_node=left_entity_node,
    #                 head_node=right_entity_node,
    #                 join_operations=[
    #                     AppendJoinPathAddition(
    #                         JoinPathAddition(
    #                             right_semantic_model_reference=right_semantic_model.reference,
    #                             join_on_entity=join_on_entity_reference,
    #                         )
    #                     ),
    #                 ],
    #                 provided_tags=ProvidedEdgeTagSet.empty_set(),
    #                 required_tags=RequiredTagSet.empty_set(),
    #             )

    def execute_recipe(
        self, semantic_graph: InProgressSemanticGraph, tail_node: EntityNode, join_on_entity_id: ElementEntityId
    ) -> None:
        if (
            join_on_entity_id.entity_type is SemanticEntityType.COMPOSITE
            or join_on_entity_id.entity_type is SemanticEntityType.TIME_DIMENSION
        ):
            return
        elif join_on_entity_id.entity_type is SemanticEntityType.ENTITY:
            pass

        # for semantic_model in self._semantic_manifest.semantic_models:
        #     for entity in semantic_model.entities:
        #         if Cardinality.get_for_entity_type(entity.type) is Cardinality.ONE:
        #             self._add_edges_for_joined_semantic_model(
        #                 join_on_entity_reference=entity.reference,
        #                 join_on_entity_type_in_right_semantic_model=entity.type,
        #                 right_semantic_model=semantic_model,
        #                 semantic_graph=semantic_graph,
        #             )
