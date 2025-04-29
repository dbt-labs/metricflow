from __future__ import annotations

import threading
from abc import ABC
from dataclasses import dataclass
from typing import ClassVar, Dict, Optional, Tuple

from dbt_semantic_interfaces.references import SemanticModelReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.experimental.semantic_graph_old.graph_nodes import SemanticEntityType
from metricflow_semantics.experimental.semantic_graph_old.ids.node_id import SemanticGraphNodeId


@dataclass(frozen=True, order=True)
class EntityId(SemanticGraphNodeId, ABC):
    @property
    def semantic_model_entity_id(self) -> Optional[ElementEntityId]:
        return None


@dataclass(frozen=True, order=True)
class TimeGrainEntityId(EntityId):
    time_grain: TimeGranularity
    _private_init: None

    _instances: ClassVar[Dict[TimeGranularity, TimeGrainEntityId]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, time_grain: TimeGranularity) -> TimeGrainEntityId:
        key = time_grain
        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance

            instance = TimeGrainEntityId(time_grain, None)
            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    def dot_label(self) -> str:
        return f"{self.time_grain.name}"


# @dataclass(frozen=True, order=True)
# class DatePartEntityId(EntityId):
#
#     date_part: DatePart
#     _private_init: None
#
#     _instances: ClassVar[Dict[DatePart, DatePartEntityId]] = {}
#     _instances_lock: ClassVar[threading.Lock] = threading.Lock()
#
#     @classmethod
#     def get_instance(cls, date_part: DatePart) -> DatePartEntityId:
#         key = date_part
#         instance = cls._instances.get(key)
#         if instance is not None:
#             return instance
#
#         with cls._instances_lock:
#             instance = cls._instances.get(key)
#             if instance is not None:
#                 return instance
#
#             instance = DatePartEntityId(date_part, None)
#             cls._instances[key] = instance
#             return instance
#
#     __hash__: ClassVar = object.__hash__
#     __eq__: ClassVar = object.__eq__
#
#     @property
#     def dot_label(self) -> str:
#         return f"{self.date_part.name}"


@dataclass(frozen=True, order=True)
class ElementEntityId(EntityId):
    element_name: str
    entity_type: SemanticEntityType
    _private_init: None

    _instances: ClassVar[Dict[Tuple[str, SemanticEntityType], ElementEntityId]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, element_name: str, entity_type: SemanticEntityType) -> ElementEntityId:
        key = (element_name, entity_type)
        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance
            instance = ElementEntityId(element_name, entity_type, None)
            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    def dot_label(self) -> str:
        return f"[{self.entity_type.name}, {self.element_name}]"


@dataclass(frozen=True, order=True)
class ConfiguredEntityId(EntityId):
    semantic_model_name: str
    element_name: str
    _private_init: None

    _instances: ClassVar[Dict[Tuple[str, str], ConfiguredEntityId]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, element_name: str, semantic_model_name: str) -> ConfiguredEntityId:
        key = (element_name, semantic_model_name)

        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance
            instance = ConfiguredEntityId(element_name, semantic_model_name, None)
            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    def dot_label(self) -> str:
        return f"[{SemanticEntityType.ENTITY.name} {self.semantic_model_name} {self.element_name}]"


@dataclass(frozen=True, order=True)
class DimensionEntityId(EntityId):
    element_name: str
    _private_init: None

    _instances: ClassVar[Dict[str, DimensionEntityId]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, element_name: str) -> DimensionEntityId:
        key = element_name
        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance
            instance = DimensionEntityId(element_name, None)
            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    def dot_label(self) -> str:
        return f"[{SemanticEntityType.ENTITY.name} {self.element_name}]"


@dataclass(frozen=True)
class AssociativeEntityId(EntityId):
    element_entity_id: ElementEntityId
    via_semantic_model: SemanticModelReference

    _private_init: None
    _instances: ClassVar[Dict[Tuple[ElementEntityId, Optional[SemanticModelReference]], AssociativeEntityId]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(
        cls, associated_entity_id: ElementEntityId, via_semantic_model: Optional[SemanticModelReference]
    ) -> AssociativeEntityId:
        key = (associated_entity_id, via_semantic_model)
        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance

            instance = AssociativeEntityId(associated_entity_id, via_semantic_model, _private_init=None)
            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    def dot_label(self) -> str:
        return f"{self.element_entity_id.dot_label, self.via_semantic_model.semantic_model_name}"


# @dataclass(frozen=True)
# class QueryEntityId(EntityId):
#     metric_attribute_ids: FrozenSet[MetricAttributeId]
#
#     _private_init: None
#     _instances: ClassVar[Dict[FrozenSet[MetricAttributeId], QueryEntityId]] = {}
#     _instances_lock: ClassVar[threading.Lock] = threading.Lock()
#
#     @classmethod
#     def get_instance(cls, metric_attribute_ids: FrozenSet[MetricAttributeId]) -> QueryEntityId:
#         key = metric_attribute_ids
#         instance = cls._instances.get(key)
#         if instance is not None:
#             return instance
#
#         with cls._instances_lock:
#             instance = cls._instances.get(key)
#             if instance is not None:
#                 return instance
#
#             instance = QueryEntityId(metric_attribute_ids, None)
#             cls._instances[key] = instance
#             return instance
#
#     __hash__: ClassVar = object.__hash__
#     __eq__: ClassVar = object.__eq__
#
#     @property
#     def dot_label(self) -> str:
#         return f"{[entity_id.dot_label for entity_id in sorted(self.metric_attribute_ids)]}"
