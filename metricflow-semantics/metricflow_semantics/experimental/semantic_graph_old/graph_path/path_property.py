"""@dataclass(frozen=True)
class MeasureAttributeComputation:
    measure_reference: Optional[MeasureReference] = None
    semantic_model_join_path: Optional[SemanticModelJoinPath] = None
    source_element_reference_for_attribute: Optional[ElementReference] = None
    time_grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None
"""
from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple, override

from dbt_semantic_interfaces.references import (
    ElementReference,
    EntityReference,
    MeasureReference,
    SemanticModelReference,
)
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.model.semantics.linkable_element import SemanticModelJoinPathElement


@dataclass(frozen=True)
class UpdateSelectedMeasure:
    measure_reference: Tuple[MeasureReference, ...]
    measure_semantic_model: SemanticModelReference


@dataclass(frozen=True)
class UpdateSelectedElement:
    selected_element: ElementReference


@dataclass(frozen=True)
class UpdateSelectedElementTimeGrain:
    time_grain: TimeGranularity


@dataclass(frozen=True)
class UpdateSelectedElementDatePart:
    date_part: DatePart


@dataclass(frozen=True)
class SemanticModelJoinPathAddition:
    added_element: SemanticModelJoinPathElement


@dataclass(frozen=True)
class IncrementEntityCount:
    entity_reference: EntityReference


@dataclass(frozen=True)
class JoinPathAddition:
    right_semantic_model_reference: SemanticModelReference
    join_on_entity: Optional[EntityReference]


@dataclass(frozen=True, order=True)
class LeftSource:
    measure_reference: MeasureReference
    semantic_model_reference: SemanticModelReference


@dataclass
class SemanticModelJoin:
    left_sources: List[LeftSource] = dataclasses.field(default_factory=list)
    join_path_additions: List[JoinPathAddition] = dataclasses.field(default_factory=list)
    right_element_reference: Optional[ElementReference] = None

    metric_time: bool = False
    time_grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def append_left_source(self, left_source: LeftSource) -> SemanticModelJoin:
        self.left_sources.append(left_source)
        return self

    def append_join_path_addition(self, join_path_addition: JoinPathAddition) -> SemanticModelJoin:
        self.join_path_additions.append(join_path_addition)
        return self

    def set_right_element_reference(self, right_element_reference: ElementReference) -> SemanticModelJoin:
        self.right_element_reference = right_element_reference
        return self

    def set_right_element_time_grain(self, time_grain: TimeGranularity) -> SemanticModelJoin:
        self.time_grain = time_grain
        return self

    def set_right_element_date_part(self, date_part: DatePart) -> SemanticModelJoin:
        self.date_part = date_part
        return self

    def set_metric_time(self) -> SemanticModelJoin:
        self.metric_time = True
        return self


@dataclass(frozen=True)
class SemanticModelJoinOperation(ABC):
    @abstractmethod
    def update_join(self, semantic_model_join: SemanticModelJoin) -> SemanticModelJoin:
        raise NotImplementedError

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return ()


@dataclass(frozen=True)
class AppendLeftSource(SemanticModelJoinOperation):
    left_source: LeftSource

    @override
    def update_join(self, semantic_model_join: SemanticModelJoin) -> SemanticModelJoin:
        return semantic_model_join.append_left_source(self.left_source)

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return (DisplayedProperty("left_source", self.left_source),)


@dataclass(frozen=True)
class SetMetricTime(SemanticModelJoinOperation):
    @override
    def update_join(self, semantic_model_join: SemanticModelJoin) -> SemanticModelJoin:
        return semantic_model_join.set_metric_time()

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return (DisplayedProperty("metric_time", "true"),)


@dataclass(frozen=True)
class SetRightElement(SemanticModelJoinOperation):
    element_reference: ElementReference

    @override
    def update_join(self, semantic_model_join: SemanticModelJoin) -> SemanticModelJoin:
        return semantic_model_join.set_right_element_reference(self.element_reference)

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return (DisplayedProperty("right_element", self.element_reference.element_name),)


@dataclass(frozen=True)
class SetRightElementTimeComponent(SemanticModelJoinOperation):
    time_grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    @override
    def update_join(self, semantic_model_join: SemanticModelJoin) -> SemanticModelJoin:
        if self.time_grain is not None:
            semantic_model_join.set_right_element_time_grain(self.time_grain)

        if self.date_part is not None:
            semantic_model_join.set_right_element_date_part(self.date_part)

        return semantic_model_join

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        displayed_properties = []
        if self.time_grain is not None:
            displayed_properties.append(DisplayedProperty("time_grain", self.time_grain.value))
        if self.date_part is not None:
            displayed_properties.append(DisplayedProperty("date_part", self.date_part.value))

        return displayed_properties


@dataclass(frozen=True)
class AppendJoinPathAddition(SemanticModelJoinOperation):
    join_path_addition: JoinPathAddition

    @override
    def update_join(self, semantic_model_join: SemanticModelJoin) -> SemanticModelJoin:
        return semantic_model_join.append_join_path_addition(self.join_path_addition)

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        displayed_properties = [
            DisplayedProperty("join", self.join_path_addition.right_semantic_model_reference.semantic_model_name),
        ]

        if self.join_path_addition.join_on_entity is not None:
            displayed_properties.append(
                DisplayedProperty("on", self.join_path_addition.join_on_entity.element_name),
            )

        return displayed_properties


@dataclass(frozen=True)
class EntityJoinStepSequence:
    selected_measure_updates: Tuple[UpdateSelectedMeasure, ...]
    selected_element_updates: Tuple[UpdateSelectedElement, ...]
    join_path_additions: Tuple[SemanticModelJoinPathAddition, ...]
    time_grain_updates: Tuple[UpdateSelectedElementTimeGrain, ...]
    date_part_updates: Tuple[UpdateSelectedElementDatePart, ...]
    entity_count_increments: Tuple[IncrementEntityCount, ...]


# class ConnectedAttributePathPropertyValidator:
#     def partial_path_is_valid(self, property_change_set: ConnectedAttributePathPropertyChangeSet) -> bool:
#         if len(property_change_set.selected_element_updates) > 1:
#             return False
#         if len(property_change_set.selected_element_updates) > 1:
#             return False
#         if len(property_change_set.join_path_additions) > 2:
#             return False
#         if len(property_change_set.time_grain_updates) > 1:
#             return False
#         if len(property_change_set.date_part_updates) > 1:
#             return False
#         if len(property_change_set.entity_count_increments) > 2:
#             return False
#         return True
