# from __future__ import annotations
#
# import logging
# from dataclasses import dataclass
# from typing import Sequence
#
# from dbt_semantic_interfaces.protocols import SemanticManifest
# from dbt_semantic_interfaces.references import MetricReference
#
# from metricflow_semantics.experimental.semantic_graph_old.builder.in_progress_semantic_graph import InProgressSemanticGraph
# from metricflow_semantics.experimental.semantic_graph_old.builder.recipies.metric_adjacency import \
#     MetricAdjacencyLookup, MetricAdjacencyResult
# from metricflow_semantics.experimental.semantic_graph_old.graph_nodes import EntityNode, SpecialNodeEnum, QueryEntityNode, \
#     CompositeEntityNode
# from metricflow_semantics.experimental.semantic_graph_old.graph_path.path_property import AppendLeftSource
# from metricflow_semantics.experimental.semantic_graph_old.ids.attribute_ids import MetricAttributeId
# from metricflow_semantics.experimental.semantic_graph_old.ids.entity_ids import CompositeEntityId
# from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
#
# logger = logging.getLogger(__name__)
#
# """
# Add metric entities:
#     For a given set of metrics, add the common primary entity or a composite entity.
#
# Expand composite entity:
#     For composite entity, add attribute and adjacent primary entities based on left sources
#
# Create a compsoite entity for all possible semantic models that it can join to.
# """
#
# @dataclass(frozen=True)
# class AddQueryEntityResult:
#     adjacency_result: MetricAdjacencyResult
#
#
# class AddQueryCompositeEntity:
#
#     def __init__(  # noqa: D107
#         self,
#         semantic_manifest: SemanticManifest,
#         semantic_model_lookup: SemanticModelLookup,
#         metric_adjacency_lookup: MetricAdjacencyLookup,
#     ) -> None:
#         super().__init__(semantic_manifest, semantic_model_lookup)
#         self._metric_adjacency_lookup = metric_adjacency_lookup
#
#     def execute_recipe(
#         self,
#         semantic_graph_old: InProgressSemanticGraph,
#         metric_references: Sequence[MetricReference]
#     ) -> None:
#         metric_attribute_ids = []
#         for metric_reference in metric_references:
#             metric_attribute_ids.append(MetricAttributeId.get_instance(metric_reference.element_name))
#
#         adjacency_result = self._metric_adjacency_lookup.resolve_adjacency(metric_attribute_ids)
#
#         query_entity_node = QueryEntityNode.get_instance(metric_attribute_ids)
#
#         for adjacent_entity_id in adjacency_result.adjacent_non_virtual_entity_ids:
#             composite_entity_node = CompositeEntityNode.get_instance(
#                 entity_id=adjacent_entity_id,
#                 source_entity_id=query_entity_node.entity_id,
#             )
# """
#     def execute_recipe(self, semantic_graph_old: InProgressSemanticGraph) -> None:
#         metric_attribute_node = AttributeNode.get_instance(self._metric_attribute_id)
#
#         adjacency_result = self._metric_adjacent_entity_lookup.resolve_adjacency(self._metric_attribute_id)
#
#         adjacent_entity_ids = adjacency_result.adjacent_entity_ids
#         if len(adjacent_entity_ids) == 0:
#             return
#
#         for entity_id in adjacent_entity_ids:
#             entity_node = EntityNode.get_instance(entity_id)
#             semantic_graph_old.add_entity_node_for_semantic_manifest_entity(
#                 entity_id=entity_id,
#                 node=entity_node,
#             )
#             semantic_graph_old.add_edge(
#                 tail_node=metric_attribute_node,
#                 head_node=entity_node,
#                 join_operations=tuple(
#                     AppendLeftSource(left_source) for left_source in adjacency_result.left_sources
#                 ),
#             )
#
#         if len(adjacency_result.metric_time_grains) == 0:
#             return
#
#         metric_time_node = semantic_graph_old.get_by_node_id(SpecialNodeEnum.METRIC_TIME.value.):
#         min_metric_time_grain = min(adjacency_result.metric_time_grains, key=lambda metric_time_grain: metric_time_grain.to_int())
#
# """
#
# """
#     def execute_recipe(self, semantic_graph_old: InProgressSemanticGraph) -> None:
#         metric_attribute_node = AttributeNode.get_instance(self._metric_attribute_id)
#
#         adjacency_result = self._metric_adjacent_entity_lookup.resolve_adjacency(self._metric_attribute_id)
#
#         adjacent_entity_ids = adjacency_result.adjacent_entity_ids
#         if len(adjacent_entity_ids) == 0:
#             return
#
#         for entity_id in adjacent_entity_ids:
#             entity_node = EntityNode.get_instance(entity_id)
#             semantic_graph_old.add_entity_node_for_semantic_manifest_entity(
#                 entity_id=entity_id,
#                 node=entity_node,
#             )
#
#         if len(adjacent_entity_ids) == 1:
#             entity_id = tuple(adjacent_entity_ids)[0]
#             entity_node = EntityNode.get_instance(entity_id)
#             semantic_graph_old.add_entity_node_for_semantic_manifest_entity(entity_id, entity_node)
#             semantic_graph_old.add_edge(
#                 tail_node=metric_attribute_node,
#                 head_node=entity_node,
#                 join_operations=tuple(
#                     AppendLeftSource(left_source) for left_source in adjacency_result.left_sources
#                 ),
#             )
#             return
#
#         # If there are multiple entities available for the metric, create a composite entity to link them.
#         composite_entity_id = CompositeEntityId.get_instance(frozenset(adjacent_entity_ids))
#
#         composite_entity_node = semantic_graph_old.get_by_node_id(composite_entity_id)
#         if composite_entity_node is None:
#             composite_entity_node = EntityNode.get_instance(composite_entity_id)
#             semantic_graph_old.add_composite_entity_node(
#                 entity_id=composite_entity_id,
#                 node=composite_entity_node,
#             )
#             for adjacent_entity_id in adjacent_entity_ids:
#                 semantic_graph_old.add_edge(
#                     tail_node=composite_entity_node,
#                     head_node=EntityNode.get_instance(adjacent_entity_id),
#                     join_operations=(),
#                 )
#
#         semantic_graph_old.add_edge(
#             tail_node=metric_attribute_node,
#             head_node=composite_entity_node,
#             join_operations=tuple(
#                 AppendLeftSource(left_source) for left_source in adjacency_result.left_sources
#             ),
#         )
#
#         semantic_graph_old.add_edge(
#             tail_node=composite_entity_node,
#             head_node=SpecialNodeEnum.get_composite_time_node()
#         )
# """
