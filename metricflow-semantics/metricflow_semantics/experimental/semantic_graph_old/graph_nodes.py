from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar, Dict, FrozenSet

from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.experimental.mf_graph.comparable import Comparable, ComparisonAnyType, ComparisonKey
from metricflow_semantics.experimental.mf_graph.graph_node import DisplayableGraphNode
from metricflow_semantics.experimental.semantic_graph_old.ids.attribute_ids import AttributeId
from metricflow_semantics.experimental.semantic_graph_old.ids.entity_ids import (
    AssociativeEntityId,
    ElementEntityId,
    EntityId,
)
from metricflow_semantics.experimental.semantic_graph_old.ids.node_id import SemanticGraphNodeId

#
# @dataclass(frozen=True, order=True)
# class SemanticGraphNodeComparisonKey:
#     node_type: str
#     comparison_str: Optional[str]


class SemanticGraphNodeType(Enum):
    ENTITY = "entity"
    ATTRIBUTE = "attribute"


class SemanticGraphNode(DisplayableGraphNode["SemanticGraphNode"], Comparable, ABC):
    """A node in the semantic graph."""

    @property
    @abstractmethod
    def node_type(self) -> SemanticGraphNodeType:
        raise NotImplementedError

    @property
    @abstractmethod
    def node_id(self) -> SemanticGraphNodeId:
        raise NotImplementedError

    @abstractmethod
    def checked_attribute_id(self) -> AttributeId:
        raise NotImplementedError

    @abstractmethod
    def checked_entity_id(self) -> EntityId:
        raise NotImplementedError


@dataclass(frozen=True)
class SemanticGraphNodeSet:
    primary_entity_nodes: FrozenSet[ElementEntityNode]
    composite_entity_nodes: FrozenSet[AssociativeEntityNode]
    attribute_nodes: FrozenSet[AttributeNode]


class SemanticEntityType(Enum):
    MEASURE = "measure"
    ENTITY = "entity"
    TIME_DIMENSION = "time_dimension"
    COMPOSITE = "composite"

    def __lt__(self, other: ComparisonAnyType) -> bool:  # noqa: D105
        if not isinstance(other, SemanticEntityType):
            return NotImplemented
        return self.value < other.value


@dataclass(frozen=True)
class EntityNode(SemanticGraphNode, ABC):
    """An entity in the entity-relationship diagram / semantic graph."""

    @property
    @abstractmethod
    def entity_id(self) -> EntityId:
        raise NotImplementedError

    @property
    @override
    def node_type(self) -> SemanticGraphNodeType:
        return SemanticGraphNodeType.ENTITY


@dataclass(frozen=True)
class ElementEntityNode(EntityNode):
    """An entity in the entity-relationship diagram / semantic graph."""

    _entity_id: ElementEntityId
    _private_init: None

    _instances: ClassVar[Dict[ElementEntityId, ElementEntityNode]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, entity_id: ElementEntityId) -> ElementEntityNode:
        key = entity_id
        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance
            instance = ElementEntityNode(entity_id, None)

            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    def entity_id(self) -> ElementEntityId:
        return self._entity_id

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.ELEMENT_ENTITY_NODE

    @property
    @override
    def dot_label(self) -> str:
        return f"PrimaryEntity({self.entity_id.dot_label})"

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        # TODO: Fix.
        return (str(self.entity_id),)

    @property
    @override
    def node_type(self) -> SemanticGraphNodeType:
        return SemanticGraphNodeType.ATTRIBUTE

    @property
    @override
    def node_id(self) -> SemanticGraphNodeId:
        return self.checked_entity_id()

    @override
    def checked_attribute_id(self) -> AttributeId:
        raise RuntimeError("This node has no attribute ID because it is an entity.")

    @override
    def checked_entity_id(self) -> EntityId:
        return self.entity_id


@dataclass(frozen=True)
class AssociativeEntityNode(EntityNode):
    """An entity in the entity-relationship diagram / semantic graph."""

    _entity_id: AssociativeEntityId
    _private_init: None

    _instances: ClassVar[Dict[AssociativeEntityId, AssociativeEntityNode]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(
        cls, element_entity_id: ElementEntityId, via_semantic_model: SemanticModelReference
    ) -> AssociativeEntityNode:
        key = AssociativeEntityId.get_instance(element_entity_id, via_semantic_model)
        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance
            instance = AssociativeEntityNode(key, None)

            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @property
    def entity_id(self) -> AssociativeEntityId:
        return self._entity_id

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.COMPOSITE_ENTITY_NODE

    @property
    @override
    def dot_label(self) -> str:
        return f"CompositeEntity({self.entity_id.dot_label})"

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        # TODO: Fix.
        return (str(self.entity_id),)

    @property
    @override
    def node_type(self) -> SemanticGraphNodeType:
        return SemanticGraphNodeType.ENTITY

    @property
    @override
    def node_id(self) -> SemanticGraphNodeId:
        return self.checked_entity_id()

    @override
    def checked_attribute_id(self) -> AttributeId:
        raise RuntimeError("This node has no attribute ID because it is an entity.")

    @override
    def checked_entity_id(self) -> EntityId:
        return self.entity_id


# @dataclass(frozen=True)
# class QueryEntityNode(EntityNode[QueryEntityId]):
#     """An entity in the entity-relationship diagram / semantic graph."""
#
#     _entity_id: QueryEntityId
#     _private_init: None
#
#     _instances: ClassVar[Dict[QueryEntityId, QueryEntityNode]] = {}
#     _instances_lock: ClassVar[threading.Lock] = threading.Lock()
#
#     @classmethod
#     def get_instance(cls, metric_attribute_ids: typing.Iterable[MetricAttributeId]) -> QueryEntityNode:
#         key = QueryEntityId.get_instance(frozenset(metric_attribute_ids))
#         instance = cls._instances.get(key)
#         if instance is not None:
#             return instance
#
#         with cls._instances_lock:
#             instance = cls._instances.get(key)
#             if instance is not None:
#                 return instance
#             instance = QueryEntityNode(key, None)
#
#             cls._instances[key] = instance
#             return instance
#
#     __hash__: ClassVar = object.__hash__
#     __eq__: ClassVar = object.__eq__
#
#     @property
#     @override
#     def entity_id(self) -> QueryEntityId:
#         return self._entity_id
#
#     @classmethod
#     @override
#     def id_prefix(cls) -> IdPrefix:
#         return StaticIdPrefix.QUERY_ENTITY_NODE
#
#     @property
#     @override
#     def dot_label(self) -> str:
#         return f"QueryEntity({self.entity_id.dot_label})"
#
#     @property
#     @override
#     def comparison_tuple(self) -> ComparisonTuple:
#         # TODO: Fix.
#         return (str(self.entity_id),)
#
#     @override
#     def node_type(self) -> SemanticGraphNodeType:
#         return SemanticGraphNodeType.ENTITY
#
#     @property
#     @override
#     def node_id(self) -> SemanticGraphNodeId:
#         return self.checked_entity_id()
#
#     @override
#     def checked_attribute_id(self) -> AttributeId:
#         raise RuntimeError("This node has no attribute ID because it is an entity.")
#
#     @override
#     def checked_entity_id(self) -> EntityId:
#         return self.entity_id


@dataclass(frozen=True)
class AttributeNode(SemanticGraphNode):
    attribute_id: AttributeId
    _private_init: None

    _instances: ClassVar[Dict[AttributeId, AttributeNode]] = {}
    _instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def get_instance(cls, attribute_id: AttributeId) -> AttributeNode:
        key = attribute_id
        instance = cls._instances.get(key)
        if instance is not None:
            return instance

        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is not None:
                return instance
            instance = AttributeNode(attribute_id, None)

            cls._instances[key] = instance
            return instance

    __hash__: ClassVar = object.__hash__
    __eq__: ClassVar = object.__eq__

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.ATTRIBUTE_NODE

    @property
    @override
    def dot_label(self) -> str:
        return f"Attribute({self.attribute_id.dot_label})"

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        # TODO: Fix.
        return (str(self.attribute_id),)

    @property
    @override
    def node_id(self) -> SemanticGraphNodeId:
        return self.checked_attribute_id()

    @override
    def checked_attribute_id(self) -> AttributeId:
        return self.attribute_id

    @override
    def checked_entity_id(self) -> EntityId:
        raise RuntimeError("This node has no entity ID because it is an attribute.")

    @property
    @override
    def node_type(self) -> SemanticGraphNodeType:
        return SemanticGraphNodeType.ATTRIBUTE


# @dataclass(frozen=True)
# class AssociativeEntityNode(SemanticGraphNode):
#     """An entity in the entity-relationship diagram / semantic graph."""
#
#     time_grain: TimeGranularity
#     _private_init: None
#
#     _instances: ClassVar[Dict[TimeGranularity, TimeGrainAssociationNode]] = {}
#     _instances_lock: ClassVar[threading.Lock] = threading.Lock()
#
#     @classmethod
#     def get_instance(cls, time_grain: TimeGranularity) -> TimeGrainAssociationNode:
#         key = time_grain
#         instance = cls._instances.get(key)
#         if instance is not None:
#             return instance
#
#         with cls._instances_lock:
#             instance = cls._instances.get(key)
#             if instance is not None:
#                 return instance
#             instance = TimeGrainAssociationNode(key, None)
#
#             cls._instances[key] = instance
#             return instance
#
#     __hash__: ClassVar = object.__hash__
#     __eq__: ClassVar = object.__eq__
#
#     @classmethod
#     @override
#     def id_prefix(cls) -> IdPrefix:
#         return StaticIdPrefix.ASSOCIATIVE_ENTITY_NODE
#
#     @cached_property
#     @override
#     def dot_label(self) -> str:
#         return f"TimeGrainAssociationNode({self.time_grain.value})"
#
#     @property
#     @override
#     def comparison_tuple(self) -> ComparisonTuple:
#         return (self.time_grain,)


class SpecialSemanticManifestElementName(Enum):
    ENTITY_KEY = "entity_key"
    TIME = "time"


# class SpecialNodeId(Enum):
#     METRIC_TIME = SemanticModelEntityId.get_instance(
#         SpecialSemanticManifestElementName.TIME.value, SemanticEntityType.TIME
#     )


# class SpecialNodeEnum(Enum):
#     METRIC_TIME = EntityNode.get_instance(SpecialNodeId.METRIC_TIME.value)
#     ENTITY_KEY_ATTRIBUTE = EntityNode.get_instance(
#         SemanticModelEntityId.get_instance(SpecialSemanticManifestElementName.ENTITY_KEY.value, SemanticEntityType.TIME)
#     )
#
#     @staticmethod
#     def get_time_grain_node(time_grain: TimeGranularity) -> EntityNode:
#         return EntityNode.get_instance(TimeGrainEntityId.get_instance(time_grain))
#
#     @staticmethod
#     def get_composite_time_node(
#         entity: EntityNode, other_entity: EntityNode, time_grain: TimeGranularity
#     ) -> EntityNode:
#         time_grain_node = SpecialNodeEnum.get_time_grain_node(time_grain)
#         composite_entity_id = CompositeEntityId.get_instance(
#             {entity.entity_id, other_entity.entity_id, time_grain_node.entity_id}
#         )
#         return EntityNode.get_instance(composite_entity_id)
#
#     @staticmethod
#     def get_date_part_node(date_part: DatePart) -> EntityNode:
#         return EntityNode.get_instance(DatePartEntityId.get_instance(date_part))
