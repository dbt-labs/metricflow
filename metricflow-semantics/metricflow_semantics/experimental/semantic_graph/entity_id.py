from __future__ import annotations

from abc import ABC
from functools import cached_property

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.singleton import Singleton


@fast_frozen_dataclass(order=False)
class SemanticModelId(Singleton):
    semantic_model_name: str

    @classmethod
    def get_instance(cls, semantic_model_name: str) -> SemanticModelId:
        return cls._get_singleton_by_kwargs(semantic_model_name=semantic_model_name)


@fast_frozen_dataclass(order=False)
class EntityId(Singleton, ABC):
    entity_name: str

    @property
    def dot_label(self) -> str:
        return self.entity_name

    @property
    def graphviz_label(self) -> str:
        return self.dot_label


@fast_frozen_dataclass(order=False)
class VirtualEntityId(EntityId, Singleton):
    @classmethod
    def get_instance(cls, entity_name: str) -> VirtualEntityId:
        return cls._get_singleton_by_kwargs(entity_name=entity_name)


@fast_frozen_dataclass(order=False)
class SemanticModelEntityId(EntityId, Singleton):
    model_id: SemanticModelId

    @cached_property
    def dot_label(self) -> str:
        return self.model_id.semantic_model_name + self.entity_name

    @classmethod
    def get_instance(cls, semantic_model_name: str, entity_name: str) -> SemanticModelEntityId:
        return cls._get_singleton_by_kwargs(semantic_model_name=semantic_model_name, entity_name=entity_name)
