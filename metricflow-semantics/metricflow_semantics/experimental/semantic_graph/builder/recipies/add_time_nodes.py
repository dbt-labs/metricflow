# from __future__ import annotations
#
# import logging
#
# from dbt_semantic_interfaces.type_enums.date_part import DatePart
#
# from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
# from metricflow_semantics.experimental.semantic_graph.builder.rules.time_helpers import TimeHelper
# from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_transform_rule import (
#     SemanticGraphRecipe,
# )
# from metricflow_semantics.experimental.semantic_graph.graph_edges import (
#     ProvidedEdgeTagSet,
#     RequiredTagSet,
# )
# from metricflow_semantics.experimental.semantic_graph.graph_nodes import SpecialNodeEnum, EntityNode
# from metricflow_semantics.experimental.semantic_graph.graph_path.path_property import SetRightElementTimeComponent
# from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
#
# logger = logging.getLogger(__name__)
#
#
# class AddTimeNodesRecipe(SemanticGraphRecipe):
#
#     def _add_edge_for_node(
#             self,
#             semantic_graph: InProgressSemanticGraph,
#             tail_node: EntityNode,
#             head_node: EntityNode,
#             date_part: DatePart,
#     ) -> None:
#         semantic_graph.add_edge(
#             tail_node=tail_node,
#             head_node=head_node,
#             join_operations=[SetRightElementTimeComponent(date_part=date_part)],
#             required_tags=RequiredTagSet.empty_set(),
#             provided_tags=ProvidedEdgeTagSet.empty_set(),
#         )
#
#     def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
#         semantic_graph.add_node(SpecialNodeEnum.METRIC_TIME.value)
#
#         for date_part in TimeHelper.ALLOWED_DATE_PARTS:
#             semantic_graph.add_node(SpecialNodeEnum.get_date_part_node(date_part))
#         for time_grain in TimeHelper.ALLOWED_TIME_GRAINS:
#             time_grain_entity_node = SpecialNodeEnum.get_time_grain_node(time_grain)
#             semantic_graph.add_node(time_grain_entity_node)
#
#             for date_part in DatePart:
#                 if date_part in TimeHelper.ALLOWED_DATE_PARTS and date_part.to_int() >= time_grain.to_int():
#                     self._add_edge_for_node(
#                         semantic_graph=semantic_graph,
#                         tail_node=time_grain_entity_node,
#                         head_node=SpecialNodeEnum.get_date_part_node(date_part),
#                         date_part=date_part,
#                     )
