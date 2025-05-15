from __future__ import annotations

from abc import ABC
from typing import Optional

from typing_extensions import override

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType


class SemanticGraphProperty(Comparable, ABC):
    """A property that a node or edge in a semantic graph can have."""

    pass


@fast_frozen_dataclass(order=False)
class ManifestElementNameProperty(SemanticGraphProperty, Singleton):
    """A property corresponding to the name used in the semantic manifest."""

    element_name: str

    @classmethod
    def get_instance(cls, element_name: str) -> ManifestElementNameProperty:
        return cls._get_singleton_by_kwargs(element_name=element_name)

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.element_name,)


# class SemanticModelElementType(OrderedEnum):
#     """Enumeration of the types of elements in a semantic model."""
#
#     MEASURE = "measure"
#     DIMENSION = "dimension"
#     ENTITY = "entity"
#
#
# @fast_frozen_dataclass()
# class SemanticModelElementTypeProperty(SemanticGraphProperty, Singleton):
#     """Property describing the type of semantic element."""
#
#     element_type: SemanticModelElementType
#
#     @classmethod
#     def get_instance(cls, element_type: SemanticModelElementType) -> SemanticModelElementTypeProperty:
#         return cls._get_singleton_by_kwargs(element_type=element_type)
#
#     @property
#     @override
#     def comparison_key(self) -> ComparisonKey:
#         return (self.element_type,)
#
#
# @fast_frozen_dataclass()
# class DunderElementNameProperty(SemanticGraphProperty, Singleton):
#     """A property that describes how an edge contributes to the dundered name."""
#
#     element_name: str
#
#     @classmethod
#     def get_instance(cls, element_name: str) -> DunderElementNameProperty:
#         return cls._get_singleton_by_kwargs(element_name=element_name)
#
#
# @fast_frozen_dataclass()
# class GraphLinkableElementProperty(SemanticGraphProperty, Singleton):
#     properties: FrozenOrderedSet[LinkableElementProperty]
#
#     @classmethod
#     def get_instance(cls, properties: OrderedSet[LinkableElementProperty]) -> GraphLinkableElementProperty:
#         return cls._get_singleton_by_kwargs(
#             properties=properties.as_frozen()
#         )


@fast_frozen_dataclass(order=False)
class LinkableElementUpdateProperty(SemanticGraphProperty, Singleton):
    element_type: Optional[LinkableElementType]
    entity_link_name: Optional[str]
    linkable_element_properties: FrozenOrderedSet[LinkableElementProperty]
    semantic_model_derivation: Optional[str]

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (
            # Need to wrap in a tuple with optional fields as None can't be compared.
            (self.element_type,) if self.element_type is not None else (),
            (self.entity_link_name,) if self.entity_link_name is not None else (),
            self.linkable_element_properties,
            (self.semantic_model_derivation,) if self.semantic_model_derivation is not None else (),
        )
