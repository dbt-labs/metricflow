# from __future__ import annotations
#
# from abc import ABC, abstractmethod
# from dataclasses import dataclass
# from typing import Optional, Sequence, Tuple
#
# from dbt_semantic_interfaces.references import ElementReference, EntityReference, SemanticModelReference
# from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
# from typing_extensions import override
#
# from metricflow_semantics.dag.mf_dag import DisplayedProperty
# from metricflow_semantics.experimental.comparison import Comparable, ComparisonAnyType
# from metricflow_semantics.experimental.semantic_graph_old.graph_nodes import SemanticGraphNode
# from metricflow_semantics.experimental.semantic_graph_old.graph_path.measure_attribute_computation import (
#     MeasureAttributeComputation,
# )
# from metricflow_semantics.model.semantics.linkable_element import SemanticModelJoinPathElement
#
#
# @dataclass(frozen=True)
# class SemanticGraphPathPropertySet:
#     query_entity_link_count: int = 0
#
#
# @dataclass(frozen=True)
# class ComputationMethod(Comparable, ABC):
#     """Describes how to compute a semantic graph node from another node"""
#
#     @property
#     @abstractmethod
#     def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
#         raise NotImplementedError
#
#     @property
#     @abstractmethod
#     def dot_label(self) -> str:
#         raise NotImplementedError
#
#     @property
#     @abstractmethod
#     def displayed_properties(self) -> Sequence[DisplayedProperty]:
#         raise NotImplementedError
#
#     @abstractmethod
#     def update_computation(
#         self, tail_node: SemanticGraphNode, head_node: SemanticGraphNode, computation: MeasureAttributeComputation
#     ) -> MeasureAttributeComputation:
#         raise NotImplementedError
#
#
# @dataclass(frozen=True)
# class CoLocatedComputationMethod(ComputationMethod):
#     """Describes computing an entity / entity attribute by using the same rows from a common semantic model."""
#
#     semantic_model_reference: Optional[SemanticModelReference]
#     element_reference: Optional[ElementReference]
#     date_trunc_to_grain: Optional[TimeGranularity] = None
#
#     @property
#     def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
#         return (self.semantic_model_reference,)
#
#     @property
#     @override
#     def dot_label(self) -> str:
#         return f"Co-located in {repr(self.semantic_model_reference.semantic_model_name)}"
#
#     @property
#     @override
#     def displayed_properties(self) -> Sequence[DisplayedProperty]:
#         return (
#             DisplayedProperty("via", "COLOCATED"),
#             DisplayedProperty("model", self.semantic_model_reference.semantic_model_name),
#         ) + (
#             (DisplayedProperty("element", self.element_reference.element_name),)
#             if self.element_reference is not None
#             else ()
#         )
#
#     @override
#     def update_computation(
#         self, tail_node: SemanticGraphNode, head_node: SemanticGraphNode, computation: MeasureAttributeComputation
#     ) -> MeasureAttributeComputation:
#         return MeasureAttributeComputation(
#             measure_reference=computation.measure_reference,
#             semantic_model_join_path=computation.semantic_model_join_path,
#             source_element_reference_for_attribute=self.element_reference
#             or computation.source_element_reference_for_attribute,
#             time_grain=computation.time_grain,
#             date_part=computation.date_part,
#         )
#
#
# @dataclass(frozen=True)
# class JoinedComputationMethod(ComputationMethod):
#     """Describes computing an entity / entity attribute by joining two semantic models."""
#
#     left_semantic_model_reference: SemanticModelReference
#     right_semantic_model_reference: SemanticModelReference
#     join_on_entity: EntityReference
#
#     @property
#     @override
#     def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
#         return (self.left_semantic_model_reference, self.right_semantic_model_reference, self.join_on_entity)
#
#     @property
#     @override
#     def dot_label(self) -> str:
#         return (
#             f"({repr(self.left_semantic_model_reference.semantic_model_name)} JOIN "
#             f"{repr(self.right_semantic_model_reference.semantic_model_name)} ON "
#             f"{repr(self.join_on_entity.element_name)})"
#         )
#
#     @property
#     @override
#     def displayed_properties(self) -> Sequence[DisplayedProperty]:
#         return (
#             DisplayedProperty("via", "JOINED"),
#             DisplayedProperty("left_model", self.left_semantic_model_reference.semantic_model_name),
#             DisplayedProperty("right_model", self.right_semantic_model_reference.semantic_model_name),
#         )
#
#     # @override
#     # def update_path_state(self, tail_node: SemanticGraphNode, head_node: SemanticGraphNode, path_state: SemanticGraphPathState) -> SemanticGraphPathState:
#     #     return SemanticGraphPathState(
#     #         measure_reference=path_state.measure_reference,
#     #         measure_semantic_model_reference=path_state.measure_semantic_model_reference,
#     #         join_path=SemanticModelJoinPath(
#     #             left_semantic_model_reference=path_state.join_path.left_semantic_model_reference,
#     #             path_elements=path_state.join_path.path_elements + (
#     #                 SemanticModelJoinPathElement(
#     #                     semantic_model_reference=self.right_semantic_model_reference,
#     #                     join_on_entity=self.on_entity_reference,
#     #                 ),
#     #             )
#     #         ),
#     #         right_attribute_description=SemanticModelElementDescription()
#     #     )
#
#     @override
#     def update_computation(
#         self, tail_node: SemanticGraphNode, head_node: SemanticGraphNode, computation: MeasureAttributeComputation
#     ) -> MeasureAttributeComputation:
#         return computation.with_additional_join_path_element(
#             SemanticModelJoinPathElement(
#                 semantic_model_reference=self.right_semantic_model_reference,
#                 join_on_entity=self.join_on_entity,
#             )
#         )
#
#
# @dataclass(frozen=True)
# class MetricTimeComputationMethod(ComputationMethod):
#     @property
#     @override
#     def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
#         return ()
#
#     @property
#     @override
#     def dot_label(self) -> str:
#         return "METRIC_TIME"
#
#     @property
#     @override
#     def displayed_properties(self) -> Sequence[DisplayedProperty]:
#         return (DisplayedProperty("via", "METRIC_TIME"),)
#
#     @override
#     def update_computation(
#         self, tail_node: SemanticGraphNode, head_node: SemanticGraphNode, computation: MeasureAttributeComputation
#     ) -> MeasureAttributeComputation:
#         return computation
#
#
# @dataclass(frozen=True)
# class DateTruncComputationMethod(ComputationMethod):
#     time_grain: TimeGranularity
#
#     @property
#     @override
#     def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
#         return (self.time_grain,)
#
#     @property
#     @override
#     def dot_label(self) -> str:
#         return f"DATE_TRUNC({self.time_grain.value!r})"
#
#     @property
#     @override
#     def displayed_properties(self) -> Sequence[DisplayedProperty]:
#         return (DisplayedProperty("via", "DATE_TRUNC"), DisplayedProperty("grain", self.time_grain.value))
#
#     @override
#     def update_computation(
#         self, tail_node: SemanticGraphNode, head_node: SemanticGraphNode, computation: MeasureAttributeComputation
#     ) -> MeasureAttributeComputation:
#         return computation.with_time_grain(self.time_grain)
#
#     # def __lt__(self, other: ComparisonAnyType) -> bool:  # noqa: D105
#     #     if not isinstance(other, ComputationMethod):
#     #         return NotImplemented
#     #     if not isinstance(other, self.__class__):
#     #         return self.__class__.__name__ < other.__class__.__name__
#     #     self_comparison_key = (self.left_semantic_model_reference, self.join_type, self.right_semantic_model_reference)
#     #     other_comparison_key = (
#     #         other.left_semantic_model_reference,
#     #         other.join_type,
#     #         other.right_semantic_model_reference,
#     #     )
#     #     return self_comparison_key < other_comparison_key
#
#
# # @dataclass(frozen=True)
# # class UnknownComputationMethod(ComputationMethod):
# #     """Placeholder for a computation method that needs to be resolved."""
# #
# #     @property
# #     def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
# #         return ()
#
#
# @dataclass(frozen=True)
# class ExtractDatePartComputationMethod(ComputationMethod):
#     date_part: DatePart
#
#     @property
#     @override
#     def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
#         return (self.date_part,)
#
#     @property
#     @override
#     def dot_label(self) -> str:
#         return f"EXTRACT({self.date_part.value!r})"
#
#     @property
#     @override
#     def displayed_properties(self) -> Sequence[DisplayedProperty]:
#         return (DisplayedProperty("via", "EXTRACT"), DisplayedProperty("date_part", self.date_part.value))
#
#     @override
#     def update_computation(
#         self, tail_node: SemanticGraphNode, head_node: SemanticGraphNode, computation: MeasureAttributeComputation
#     ) -> MeasureAttributeComputation:
#         return computation.with_date_part(self.date_part)
#
#
# @dataclass(frozen=True)
# class MetricComputationMethod(ComputationMethod):
#     @property
#     @override
#     def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
#         return ()
#
#     @property
#     @override
#     def dot_label(self) -> str:
#         return "METRIC"
#
#     @property
#     @override
#     def displayed_properties(self) -> Sequence[DisplayedProperty]:
#         return ()
#
#     @override
#     def update_computation(
#         self, tail_node: SemanticGraphNode, head_node: SemanticGraphNode, computation: MeasureAttributeComputation
#     ) -> MeasureAttributeComputation:
#         return computation
