from __future__ import annotations

from abc import ABC

from aenum import OrderedEnum

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.singleton import Singleton


@fast_frozen_dataclass()
class SemanticGraphProperty(ABC):
    pass


@fast_frozen_dataclass()
class NameProperty(SemanticGraphProperty, Singleton):
    element_name: str

    @classmethod
    def get_instance(cls, element_name) -> NameProperty:
        return cls._get_singleton_by_kwargs(element_name=element_name)


class ManifestElementType(OrderedEnum):
    MEASURE = "measure"
    DIMENSION = "dimension"
    ENTITY = "entity"


@fast_frozen_dataclass()
class ManifestElementTypeProperty(SemanticGraphProperty, Singleton):
    element_type: ManifestElementType

    @classmethod
    def get_instance(cls, element_type: ManifestElementType) -> ManifestElementTypeProperty:
        return cls._get_singleton_by_kwargs(element_type=element_type)


@fast_frozen_dataclass()
class DunderElementNameProperty(SemanticGraphProperty, Singleton):
    element_name: str

    @classmethod
    def get_instance(cls, element_name: str) -> DunderElementNameProperty:
        return cls._get_singleton_by_kwargs(element_name=element_name)
