from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from typing import Optional, override

from dbt_semantic_interfaces.references import SemanticModelReference

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.graph_element import HasDisplayedProperty
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType

logger = logging.getLogger(__name__)


class AttributeComputation(SemanticModelDerivation, ABC):
    @property
    @abstractmethod
    def linkable_element_properties(self) -> OrderedSet[LinkableElementProperty]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def attribute_descriptor(self) -> Optional[AttributeDescriptor]:
        raise NotImplementedError()


@fast_frozen_dataclass()
class AttributeComputationUpdate(HasDisplayedProperty):
    dundered_name_element_additions: AnyLengthTuple[str] = ()
    linkable_element_property_additions: AnyLengthTuple[LinkableElementProperty] = ()
    derived_from_model_id_additions: AnyLengthTuple[SemanticModelId] = ()
    element_type_additions: AnyLengthTuple[LinkableElementType] = ()
    dsi_entity_additions: AnyLengthTuple[str] = ()

    @override
    @cached_property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties = list(super().displayed_properties)

        for dundered_name_element_addition in self.dundered_name_element_additions:
            properties.append(
                DisplayedProperty("add_name", dundered_name_element_addition),
            )
        for linkable_element_property_addition in self.linkable_element_property_additions:
            properties.append(DisplayedProperty("add_prop", linkable_element_property_addition.name))

        for derived_from_model_id_addition in self.derived_from_model_id_additions:
            properties.append(DisplayedProperty("add_model", derived_from_model_id_addition.model_name))
        for element_type_addition in self.element_type_additions:
            properties.append(DisplayedProperty("add_type", element_type_addition.name))
        for dsi_entity_addition in self.dsi_entity_additions:
            properties.append(DisplayedProperty("add_dsi_entity", dsi_entity_addition))
        return tuple(properties)


@fast_frozen_dataclass()
class AttributeDescriptor:
    dundered_name_elements: AnyLengthTuple[str] = ()
    model_ids: FrozenOrderedSet[SemanticModelId] = FrozenOrderedSet()
    properties: FrozenOrderedSet[LinkableElementProperty] = FrozenOrderedSet()
    element_types: FrozenOrderedSet[LinkableElementType] = FrozenOrderedSet()
    dsi_entity_names: FrozenOrderedSet[str] = FrozenOrderedSet()


@dataclass
class MutableAttributeComputation(AttributeComputation):
    ordered_attribute_descriptors: list[AttributeDescriptor] = field(default_factory=list)

    @property
    def linkable_element_properties(self) -> FrozenOrderedSet[LinkableElementProperty]:
        if len(self.ordered_attribute_descriptors) == 0:
            return FrozenOrderedSet()

        return FrozenOrderedSet(self.ordered_attribute_descriptors[-1].properties)

    @property
    def element_types(self) -> FrozenOrderedSet[LinkableElementType]:
        if len(self.ordered_attribute_descriptors) == 0:
            return FrozenOrderedSet()

        return FrozenOrderedSet(self.ordered_attribute_descriptors[-1].element_types)

    @property
    def derived_from_semantic_models(self) -> AnyLengthTuple[SemanticModelReference]:
        if len(self.ordered_attribute_descriptors) == 0:
            return ()
        return tuple(
            SemanticModelReference(semantic_model_name=model_id.model_name)
            for model_id in self.ordered_attribute_descriptors[-1].model_ids
        )

    def append_update(self, update: AttributeComputationUpdate) -> None:
        if len(self.ordered_attribute_descriptors) == 0:
            self.ordered_attribute_descriptors.append(
                AttributeDescriptor(
                    dundered_name_elements=update.dundered_name_element_additions,
                    model_ids=FrozenOrderedSet(update.derived_from_model_id_additions),
                    properties=FrozenOrderedSet(update.linkable_element_property_additions),
                    element_types=FrozenOrderedSet(update.element_type_additions),
                    dsi_entity_names=FrozenOrderedSet(update.dsi_entity_additions),
                )
            )
            return
        previous_attribute_descriptor = self.ordered_attribute_descriptors[-1]

        self.ordered_attribute_descriptors.append(
            AttributeDescriptor(
                dundered_name_elements=previous_attribute_descriptor.dundered_name_elements
                + update.dundered_name_element_additions,
                model_ids=previous_attribute_descriptor.model_ids.union(update.derived_from_model_id_additions),
                properties=previous_attribute_descriptor.properties.union(update.linkable_element_property_additions),
                element_types=previous_attribute_descriptor.element_types.union(update.element_type_additions),
                dsi_entity_names=previous_attribute_descriptor.dsi_entity_names.union(update.dsi_entity_additions),
            )
        )
        return

    def pop_update(self) -> None:
        self.ordered_attribute_descriptors.pop()

    @property
    def attribute_descriptor(self) -> AttributeDescriptor:
        if len(self.ordered_attribute_descriptors) == 0:
            return AttributeDescriptor()

        return self.ordered_attribute_descriptors[-1]

    @property
    def previous_attribute_descriptor(self) -> AttributeDescriptor:
        if len(self.ordered_attribute_descriptors) == 1:
            return AttributeDescriptor()

        return self.ordered_attribute_descriptors[-2]


class AttributeComputationUpdater(HasDisplayedProperty, ABC):
    @cached_property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate()

