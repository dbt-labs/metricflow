from __future__ import annotations

import logging

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import Entity, SemanticModel
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import EntityType

from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_transform_rule import (
    SemanticGraphRecipe,
)
from metricflow_semantics.experimental.semantic_graph.computation_method import CoLocatedComputationMethod
from metricflow_semantics.experimental.semantic_graph.graph_edges import (
    Cardinality,
    ProvidedEdgeTagSet,
    RequiredTagSet,
    SemanticGraphEdgeType,
)
from metricflow_semantics.experimental.semantic_graph.graph_nodes import EntityNode

logger = logging.getLogger(__name__)


class AddCoLocatedEntityEdgesRule(SemanticGraphRecipe):
    def _add_edges_from_entity_in_semantic_model(
        self,
        entity: Entity,
        semantic_model: SemanticModel,
        semantic_graph: InProgressSemanticGraph,
    ) -> None:
        for other_entity in semantic_model.entities:
            if Cardinality.get_for_entity_type(other_entity.type) is not Cardinality.ONE or other_entity == entity:
                continue
            entity_node = EntityNode(entity.reference)
            other_entity_node = EntityNode(other_entity.reference)
            semantic_graph.add_edge(
                tail_node=entity_node,
                edge_type=SemanticGraphEdgeType.get_for_entity_types(
                    tail_entity_type=entity.type, head_entity_type=other_entity.type
                ),
                head_node=other_entity_node,
                computation_method=CoLocatedComputationMethod(semantic_model.reference),
                provided_tags=ProvidedEdgeTagSet.empty_set(),
                required_tags=RequiredTagSet.empty_set(),
            )

    def _add_edges_for_virtual_primary_entity(
        self,
        primary_entity_reference: EntityReference,
        semantic_model: SemanticModel,
        semantic_graph: InProgressSemanticGraph,
    ) -> None:
        primary_entity_node = EntityNode(primary_entity_reference)
        computation_method = CoLocatedComputationMethod(semantic_model.reference)

        for other_entity in semantic_model.entities:
            other_entity_node = EntityNode(other_entity.reference)
            # semantic_graph.add_edge(
            #     tail_node=primary_entity_node,
            #     edge_type=SemanticGraphEdgeType.get_for_entity_types(
            #         tail_entity_type=EntityType.PRIMARY, head_entity_type=other_entity.type
            #     ),
            #     head_node=other_entity_node,
            #     computation_method=computation_method,
            # )
            # if Cardinality.get_for_entity_type(other_entity.type) is Cardinality.ONE:
            semantic_graph.add_edge(
                tail_node=primary_entity_node,
                # edge_type=SemanticGraphEdgeType.get_for_entity_types(
                #     tail_entity_type=EntityType.PRIMARY, head_entity_type=other_entity.type
                # ),
                edge_type=self._get_edge_type_for_colocated_entity(
                    tail_entity_type=EntityType.PRIMARY,
                    head_entity_type=other_entity.type,
                ),
                head_node=other_entity_node,
                computation_method=computation_method,
                provided_tags=ProvidedEdgeTagSet.empty_set(),
                required_tags=RequiredTagSet.empty_set(),
            )

    def _get_edge_type_for_colocated_entity(
        self, tail_entity_type: EntityType, head_entity_type: EntityType
    ) -> SemanticGraphEdgeType:
        head_end_type = Cardinality.get_for_entity_type(head_entity_type)
        if head_end_type is Cardinality.ONE:
            return SemanticGraphEdgeType.ONE_TO_ONE
        elif head_end_type is Cardinality.MANY:
            return SemanticGraphEdgeType.MANY_TO_ONE
        else:
            assert_values_exhausted(head_end_type)

    def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
        for semantic_model in self._semantic_manifest.semantic_models:
            primary_entity_reference = semantic_model.primary_entity_reference
            if primary_entity_reference is not None and primary_entity_reference not in {
                entity.reference for entity in semantic_model.entities
            }:
                self._add_edges_for_virtual_primary_entity(
                    primary_entity_reference=primary_entity_reference,
                    semantic_model=semantic_model,
                    semantic_graph=semantic_graph,
                )

            # for entity_index, entity in enumerate(semantic_model.entities):
            #     for other_entity_index in range(entity_index + 1, len(semantic_model.entities)):
            #         entity_node = EntityNode(entity.reference)
            #         other_entity = semantic_model.entities[other_entity_index]
            #         other_entity_node = EntityNode(other_entity.reference)
            #         semantic_graph.add_edge(
            #             tail_node=entity_node,
            #             edge_type=SemanticGraphEdgeType.get_for_entity_types(
            #                 tail_entity_type=entity.type, head_entity_type=other_entity.type
            #             ),
            #             head_node=other_entity_node,
            #             computation_method=CoLocatedComputationMethod(semantic_model.reference),
            #         )

            for entity in semantic_model.entities:
                if Cardinality.get_for_entity_type(entity.type) is not Cardinality.ONE:
                    continue

                for other_entity in semantic_model.entities:
                    if entity.reference == other_entity.reference:
                        continue

                    entity_node = EntityNode(entity.reference)
                    other_entity_node = EntityNode(other_entity.reference)
                    semantic_graph.add_edge(
                        tail_node=entity_node,
                        # edge_type=SemanticGraphEdgeType.get_for_entity_types(
                        #     tail_entity_type=entity.type, head_entity_type=other_entity.type
                        # ),
                        edge_type=self._get_edge_type_for_colocated_entity(
                            tail_entity_type=entity.type,
                            head_entity_type=other_entity.type,
                        ),
                        head_node=other_entity_node,
                        computation_method=CoLocatedComputationMethod(semantic_model.reference),
                        provided_tags=ProvidedEdgeTagSet.empty_set(),
                        required_tags=RequiredTagSet.empty_set(),
                    )
