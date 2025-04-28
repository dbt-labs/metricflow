from __future__ import annotations

import threading
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar, Dict, Optional

from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow_semantics.experimental.semantic_graph.ids.node_id import SemanticGraphNodeId


class AttributeType(Enum):
    METRIC = "metric"
    ENTITY_KEY = "key"
    DIMENSION = "dimension"


@dataclass(frozen=True)
class AttributeId(SemanticGraphNodeId):
    @property
    @abstractmethod
    def attribute_type(self) -> AttributeType:
        raise NotImplementedError

    @abstractmethod
    def checked_metric_attribute_id(self) -> MetricAttributeId:
        raise NotImplementedError


# @dataclass(frozen=True)
# class DimensionAttributeId(AttributeId):
#     element_name: str
#
#     @property
#     @override
#     def dot_label(self) -> str:
#         return self.element_name
#
#
# @dataclass(frozen=True)
# class DatePartAttributeId(AttributeId):
#     date_part: DatePart
#
#     @property
#     @override
#     def dot_label(self) -> str:
#         return self.date_part.name


@dataclass(frozen=True, order=True)
class MetricAttributeId(AttributeId):
    element_name: str
    _private_init: None

    _instances: ClassVar[Dict[str, MetricAttributeId]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, element_name: str) -> MetricAttributeId:
        key = element_name
        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance
            instance = MetricAttributeId(element_name, None)

            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    @override
    def dot_label(self) -> str:
        return f"METRIC {self.element_name}"

    @property
    @override
    def attribute_type(self) -> AttributeType:
        return AttributeType.METRIC

    @override
    def checked_metric_attribute_id(self) -> MetricAttributeId:
        return self


@dataclass(frozen=True)
class DimensionAttributeId(AttributeId):
    element_name: str
    _private_init: None

    _instances: ClassVar[Dict[str, DimensionAttributeId]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, element_name: str) -> DimensionAttributeId:
        key = element_name
        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance
            instance = DimensionAttributeId(element_name, None)

            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    @override
    def dot_label(self) -> str:
        return f"DIMENSION {self.element_name}"

    @property
    @override
    def attribute_type(self) -> AttributeType:
        return AttributeType.DIMENSION

    @override
    def checked_metric_attribute_id(self) -> MetricAttributeId:
        raise RuntimeError("This attribute does not have a metric attribute ID since it is a dimension attribute.")


@dataclass(frozen=True)
class EntityKeyAttributeId(AttributeId):
    element_name: str

    _private_init: None
    _instance: ClassVar[Optional[EntityKeyAttributeId]]
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, element_name: str) -> EntityKeyAttributeId:
        instance = cls._instance
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instance
            if instance is not None:
                return instance
            instance = EntityKeyAttributeId(element_name, None)

            cls._instance = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    @override
    def dot_label(self) -> str:
        return "KEY"

    @property
    @override
    def attribute_type(self) -> AttributeType:
        return AttributeType.METRIC

    @override
    def checked_metric_attribute_id(self) -> MetricAttributeId:
        raise RuntimeError("This attribute does not have a metric attribute ID since it is a key attribute.")


@dataclass(frozen=True)
class DatePartAttributeId(AttributeId):
    date_part: DatePart

    _private_init: None
    _instance: ClassVar[Optional[EntityKeyAttributeId]]
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, element_name: str) -> EntityKeyAttributeId:
        instance = cls._instance
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instance
            if instance is not None:
                return instance
            instance = EntityKeyAttributeId(element_name, None)

            cls._instance = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    @override
    def dot_label(self) -> str:
        return "KEY"

    @property
    @override
    def attribute_type(self) -> AttributeType:
        return AttributeType.METRIC

    @override
    def checked_metric_attribute_id(self) -> MetricAttributeId:
        raise RuntimeError("This attribute does not have a metric attribute ID since it is a key attribute.")
