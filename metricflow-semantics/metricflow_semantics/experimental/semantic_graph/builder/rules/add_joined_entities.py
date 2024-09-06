from __future__ import annotations

import logging

from dbt_semantic_interfaces.protocols import SemanticModel
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import EntityType

from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_transform_rule import (
    SemanticGraphRecipe,
)
from metricflow_semantics.experimental.semantic_graph.computation_method import (
    JoinedComputationMethod,
)
from metricflow_semantics.experimental.semantic_graph.graph_edges import (
    Cardinality,
    ProvidedEdgeTagSet,
    RequiredTagSet,
    SemanticGraphEdgeType,
)
from metricflow_semantics.experimental.semantic_graph.graph_nodes import EntityNode

logger = logging.getLogger(__name__)


class AddJoinedEntityEdgesRule(SemanticGraphRecipe):
    # def _add_edges_from_tail_entity_to_head_entities(
    #     self,
    #     entity: Entity,
    #     entity_semantic_model: SemanticModel,
    #     other_semantic_model: SemanticModel,
    #     semantic_graph: InProgressSemanticGraph,
    # ) -> None:
    #     for other_entity in other_semantic_model.entities:
    #         # Only joinable if the same entity exists in both.
    #         if other_entity.reference != entity.reference:
    #             continue
    #         entity_node = EntityNode(entity.reference)
    #         other_entity_node = EntityNode(other_entity.reference)
    #
    #         semantic_graph.add_edge(
    #             tail_node=entity_node,
    #             edge_type=SemanticGraphEdgeType.get_for_entity_types(
    #                 tail_entity_type=entity.type, head_entity_type=other_entity.type
    #             ),
    #             head_node=other_entity_node,
    #             computation_method=JoinedComputationMethod(
    #                 left_semantic_model_reference=entity_semantic_model.reference,
    #                 right_semantic_model_reference=other_semantic_model.reference,
    #             ),
    #         )
    #
    #         semantic_graph.add_edge(
    #             tail_node=other_entity_node,
    #             edge_type=SemanticGraphEdgeType.get_for_entity_types(
    #                 tail_entity_type=other_entity.type, head_entity_type=entity.type
    #             ),
    #             head_node=entity_node,
    #             computation_method=JoinedComputationMethod(
    #                 left_semantic_model_reference=other_semantic_model.reference,
    #                 right_semantic_model_reference=entity_semantic_model.reference,
    #             ),
    #         )

    def _add_edges_for_joined_semantic_model(
        self,
        join_on_entity_reference: EntityReference,
        join_on_entity_type_in_right_semantic_model: EntityType,
        right_semantic_model: SemanticModel,
        semantic_graph: InProgressSemanticGraph,
    ) -> None:
        right_entity_node = EntityNode(join_on_entity_reference)
        for left_semantic_model in self._semantic_model_lookup.get_semantic_models_for_entity(join_on_entity_reference):
            # Don't do self-joins.
            if right_semantic_model.reference == left_semantic_model.reference:
                continue

            # Handle primary entity
            # primary_entity_reference = left_semantic_model.primary_entity_reference
            # if primary_entity_reference is not None and primary_entity_reference not in {
            #     entity.reference for entity in left_semantic_model.entities
            # }:
            #     left_entity_node = EntityNode(primary_entity_reference)
            #     semantic_graph.add_edge(
            #         tail_node=left_entity_node,
            #         edge_type=SemanticGraphEdgeType.get_for_entity_types(
            #             tail_entity_type=EntityType.PRIMARY,
            #             head_entity_type=join_on_entity_type_in_right_semantic_model,
            #         ),
            #         head_node=right_entity_node,
            #         computation_method=JoinedComputationMethod(
            #             left_semantic_model_reference=left_semantic_model.reference,
            #             right_semantic_model_reference=right_semantic_model.reference,
            #             on_entity_reference=join_on_entity_reference,
            #         ),
            #         provided_tags=ProvidedEdgeTagSet.empty_set(),
            #         required_tags=RequiredTagSet.empty_set(),
            #     )

            for entity_in_left_semantic_model in left_semantic_model.entities:
                if (
                    join_on_entity_reference == entity_in_left_semantic_model.reference
                    or Cardinality.get_for_entity_type(entity_in_left_semantic_model.type) is not Cardinality.ONE
                ):
                    continue
                left_entity_node = EntityNode(entity_in_left_semantic_model.reference)
                semantic_graph.add_edge(
                    tail_node=left_entity_node,
                    edge_type=SemanticGraphEdgeType.get_for_entity_types(
                        tail_entity_type=entity_in_left_semantic_model.type,
                        head_entity_type=join_on_entity_type_in_right_semantic_model,
                    ),
                    head_node=right_entity_node,
                    computation_method=JoinedComputationMethod(
                        left_semantic_model_reference=left_semantic_model.reference,
                        right_semantic_model_reference=right_semantic_model.reference,
                        on_entity_reference=join_on_entity_reference,
                    ),
                    provided_tags=ProvidedEdgeTagSet.empty_set(),
                    required_tags=RequiredTagSet.empty_set(),
                )
            # self._add_edges_from_tail_entity_to_head_entities(
            #     entity=join_on_entity,
            #     entity_semantic_model=right_semantic_model,
            #     other_semantic_model=left_semantic_model,
            #     semantic_graph=semantic_graph,
            # )

    def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
        for semantic_model in self._semantic_manifest.semantic_models:
            for entity in semantic_model.entities:
                if Cardinality.get_for_entity_type(entity.type) is Cardinality.ONE:
                    self._add_edges_for_joined_semantic_model(
                        join_on_entity_reference=entity.reference,
                        join_on_entity_type_in_right_semantic_model=entity.type,
                        right_semantic_model=semantic_model,
                        semantic_graph=semantic_graph,
                    )
