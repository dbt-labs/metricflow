# from __future__ import annotations
#
# import logging
#
# from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
# from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_transform_rule import (
#     SemanticGraphRecipe,
# )
# from metricflow_semantics.experimental.semantic_graph.graph_nodes import EntityNode, SemanticEntityType
#
# logger = logging.getLogger(__name__)
#
#
# class AddEntitiesRule(SemanticGraphRecipe):
#     def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
#         for semantic_model in self._semantic_manifest.semantic_models:
#             for entity in semantic_model.entities:
#                 entity_node = EntityNode.get_instance(entity.name, SemanticEntityType.ENTITY)
#                 semantic_graph._nodes.add(entity_node)
#
#             if semantic_model.primary_entity is not None:
#                 semantic_graph._nodes.add(
#                     EntityNode.get_instance(semantic_model.primary_entity, SemanticEntityType.VIRTUAL_PRIMARY_ENTITY)
#                 )
