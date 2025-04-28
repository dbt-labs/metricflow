# from collections import defaultdict
# from typing import Dict, List, Sequence, Tuple
#
# from dbt_semantic_interfaces.protocols import SemanticManifest
# from dbt_semantic_interfaces.references import EntityReference
#
# from metricflow_semantics.experimental.semantic_graph.graph_edges import Cardinality, SemanticGraphEdge, \
#     SemanticGraphPathStatOperation
# from metricflow_semantics.experimental.semantic_graph.graph_nodes import SemanticGraphNode, SemanticEntityType, \
#     CompositeEntityNode, EntityNode
# from metricflow_semantics.experimental.semantic_graph.graph_path.path_property import SemanticModelJoinOperation, \
#     JoinPathAddition, AppendJoinPathAddition
# from metricflow_semantics.experimental.semantic_graph.ids.entity_ids import SemanticModelEntityId
#
#
# class SemanticGraphEdgeLookup:
#     def __init__(self, semantic_manifest: SemanticManifest) -> None:
#         self._semantic_manifest = semantic_manifest
#         self._entity_id_to_join_operation: Dict[SemanticModelEntityId, List[SemanticModelJoinOperation]] = defaultdict(list)
#
#         for semantic_model in self._semantic_manifest.semantic_models:
#             for entity in semantic_model.entities:
#                 if Cardinality.get_for_entity_type(entity.type) is Cardinality.ONE:
#                     self._entity_id_to_join_operation[
#                         SemanticModelEntityId.get_instance(
#                             element_name=entity.name,
#                             entity_type=SemanticEntityType.ENTITY,
#                         )
#                     ].append(
#                         AppendJoinPathAddition(
#                             JoinPathAddition(
#                                 right_semantic_model_reference=semantic_model.reference,
#                                 join_on_entity=entity.reference,
#                             )
#                         )
#                     )
#
#     def get_joinable_nodes(
#         self,
#         tail_node: EntityNode,
#         entity_id: SemanticModelEntityId,
#     ) -> Sequence[SemanticGraphEdge]:
#         edges: List[SemanticGraphEdge] = []
#
#         for join_operation in self._entity_id_to_join_operation[entity_id]:
#             head_node = CompositeEntityNode.get_instance(
#                 entity_id=join_operation,
#                 source_entity_id=tail_node.entity_id,
#             )
#             edges.append(
#                 SemanticGraphEdge(
#                     tail_node=tail_node,
#                     head_node=head_node,
#                     join_operations=(join_operation,),
#                 )
#             )
