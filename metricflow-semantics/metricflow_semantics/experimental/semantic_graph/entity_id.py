# from __future__ import annotations
#
# from abc import ABC
#
# from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
# from dbt_semantic_interfaces.type_enums import TimeGranularity
# from typing_extensions import override
#
# from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
# from metricflow_semantics.dag.mf_dag import DisplayedProperty
# from metricflow_semantics.experimental.mf_graph.comparable import Comparable, ComparisonKey
# from metricflow_semantics.experimental.mf_graph.displayable_graph_element import (
#     HasDisplayedProperty,
# )
# from metricflow_semantics.experimental.mf_graph.mf_graph_element_id import MetricflowGraphElementId
# from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
#
#
# @singleton_dataclass(order=False)
# class EntityId(MetricflowGraphElementId, HasDisplayedProperty, Comparable, ABC):
#     entity_name: str
#
#     @override
#     @property
#     def str_value(self) -> str:
#         return self.entity_name
#
#
# @singleton_dataclass(order=False)
# class TimeBaseEntityId(EntityId):
#     @staticmethod
#     def create(entity_name: str) -> TimeBaseEntityId:
#         return TimeBaseEntityId(entity_name=f"time({entity_name})" + entity_name)
#
#     @override
#     @property
#     def comparison_key(self) -> ComparisonKey:
#         return (self.entity_name,)
#
#
# @singleton_dataclass(order=False)
# class SemanticElementEntityId(EntityId):
#     @staticmethod
#     def get_instance(semantic_model_name: str, element_name: str) -> SemanticElementEntityId:
#         entity_name = f"{semantic_model_name}.{element_name}"
#         return SemanticElementEntityId(
#             entity_name=entity_name,
#         )
#
#     @override
#     @property
#     def comparison_key(self) -> ComparisonKey:
#         return (self.entity_name,)
#
#
# @singleton_dataclass(order=False)
# class JoinToSemanticModelId(EntityId):
#     semantic_model_name: str
#
#     @staticmethod
#     def get_instance(semantic_model_name: str) -> JoinToSemanticModelId:
#         return JoinToSemanticModelId(
#             entity_name=f"join_to({semantic_model_name})",
#             semantic_model_name=semantic_model_name,
#         )
#
#     @override
#     @property
#     def comparison_key(self) -> ComparisonKey:
#         return (self.entity_name, self.semantic_model_name)
#
#     @property
#     def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
#         return (
#             DisplayedProperty(
#                 "description",
#                 f"Entry point for joining to the {self.semantic_model_name!r} semantic model",
#             ),
#         )
#
#
# @singleton_dataclass(order=False)
# class JoinFromSemanticModelId(EntityId):
#     semantic_model_name: str
#
#     @staticmethod
#     def get_instance(semantic_model_name: str) -> JoinFromSemanticModelId:
#         return JoinFromSemanticModelId(
#             entity_name=f"join_from({semantic_model_name})",
#             semantic_model_name=semantic_model_name,
#         )
#
#     @override
#     @property
#     def comparison_key(self) -> ComparisonKey:
#         return (self.entity_name, self.semantic_model_name)
#
#     @property
#     def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
#         return (
#             DisplayedProperty(
#                 "comment",
#                 f"Exit point for joining from the {self.semantic_model_name!r} model",
#             ),
#         )
#
#
# @singleton_dataclass(order=False)
# class MetricTimeEntityId(EntityId):
#     time_grain_name: str
#
#     @staticmethod
#     def create(time_grain: TimeGranularity) -> MetricTimeEntityId:
#         return MetricTimeEntityId(
#             entity_name=f"{METRIC_TIME_ELEMENT_NAME}.{time_grain.value}",
#             time_grain_name=time_grain.value,
#         )
#
#     @override
#     @property
#     def comparison_key(self) -> ComparisonKey:
#         return (self.entity_name, self.time_grain_name)
#
#
# @singleton_dataclass(order=False)
# class AggregationEntityId(EntityId):
#     semantic_model_name: str
#     aggregation_time_dimension_name: str
#
#     @staticmethod
#     def get_instance(semantic_model_name: str, aggregation_time_dimension_name: str) -> AggregationEntityId:
#         return AggregationEntityId(
#             entity_name=f"aggregate({semantic_model_name},{aggregation_time_dimension_name})",
#             semantic_model_name=semantic_model_name,
#             aggregation_time_dimension_name=aggregation_time_dimension_name,
#         )
#
#     @override
#     @property
#     def comparison_key(self) -> ComparisonKey:
#         return (self.entity_name, self.semantic_model_name, self.aggregation_time_dimension_name)
