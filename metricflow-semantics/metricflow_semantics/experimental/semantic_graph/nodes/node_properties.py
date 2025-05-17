from __future__ import annotations

from abc import ABC
from typing import Optional

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType


class SemanticGraphProperty(Comparable, ABC):
    """A property that a node or edge in a semantic graph can have."""

    pass


@singleton_dataclass(order=False)
class ManifestElementNameProperty(SemanticGraphProperty):
    """A property corresponding to the name used in the semantic manifest."""

    element_name: str

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.element_name,)


@singleton_dataclass(order=False)
class LinkableElementUpdateProperty(SemanticGraphProperty):
    element_type: Optional[LinkableElementType]
    entity_link_name: Optional[str]
    linkable_element_properties: FrozenOrderedSet[LinkableElementProperty]
    semantic_model_derivation: Optional[str]

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (
            self.element_type,
            self.entity_link_name,
            self.linkable_element_properties,
            self.semantic_model_derivation,
        )
