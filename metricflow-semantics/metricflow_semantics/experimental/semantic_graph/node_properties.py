from __future__ import annotations

from abc import ABC

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.singleton import Singleton


@fast_frozen_dataclass()
class SemanticGraphProperty(ABC):
    """A property that a node or edge in a semantic graph can have."""

    pass


@fast_frozen_dataclass()
class ManifestElementNameProperty(SemanticGraphProperty, Singleton):
    """A property corresponding to the name used in the semantic manifest."""

    element_name: str

    @classmethod
    def get_instance(cls, element_name: str) -> ManifestElementNameProperty:
        return cls._get_singleton_by_kwargs(element_name=element_name)


class SemanticModelElementType(OrderedEnum):
    """Enumeration of the types of elements in a semantic model."""

    MEASURE = "measure"
    DIMENSION = "dimension"
    ENTITY = "entity"


@fast_frozen_dataclass()
class SemanticModelElementTypeProperty(SemanticGraphProperty, Singleton):
    """Property describing the type of semantic element."""

    element_type: SemanticModelElementType

    @classmethod
    def get_instance(cls, element_type: SemanticModelElementType) -> SemanticModelElementTypeProperty:
        return cls._get_singleton_by_kwargs(element_type=element_type)


@fast_frozen_dataclass()
class DunderElementNameProperty(SemanticGraphProperty, Singleton):
    """A property that describes how an edge contributes to the dundered name."""

    element_name: str

    @classmethod
    def get_instance(cls, element_name: str) -> DunderElementNameProperty:
        return cls._get_singleton_by_kwargs(element_name=element_name)
