from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from typing import Optional, override

from dbt_semantic_interfaces.references import SemanticModelReference

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_flatten
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.graph_element import HasDisplayedProperty
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.naming.linkable_spec_name import DUNDER

logger = logging.getLogger(__name__)


class AttributeComputation(SemanticModelDerivation, ABC):
    @property
    @abstractmethod
    def linkable_element_properties(self) -> OrderedSet[LinkableElementProperty]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def spec(self) -> Optional[SpecResult]:
        raise NotImplementedError()


@fast_frozen_dataclass()
class AttributeComputationUpdate(HasDisplayedProperty):
    dundered_name_element_addition: Optional[str] = None
    linkable_element_property_additions: FrozenOrderedSet[LinkableElementProperty] = FrozenOrderedSet()
    derived_from_model_id_addition: Optional[SemanticModelId] = None

    @property
    def is_no_op(self) -> bool:
        return (
            self.dundered_name_element_addition is None
            and len(self.linkable_element_property_additions) == 0
            and self.derived_from_model_id_addition is None
        )

    @override
    @cached_property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties = list(super().displayed_properties)

        if self.dundered_name_element_addition is not None:
            properties.append(
                DisplayedProperty("add_name", self.dundered_name_element_addition),
            )
        if self.linkable_element_property_additions is not None:
            for linkable_element_property_addition in self.linkable_element_property_additions:
                properties.append(DisplayedProperty("add_prop", linkable_element_property_addition.name))
        if self.derived_from_model_id_addition is not None:
            properties.append(DisplayedProperty("add_model", self.derived_from_model_id_addition.model_name))
        return tuple(properties)


@dataclass
class SpecResult:
    name: str
    model_ids: MutableOrderedSet[SemanticModelId]
    properties: MutableOrderedSet[LinkableElementProperty]


@dataclass
class MutableAttributeComputation(AttributeComputation):
    _updates: list[AttributeComputationUpdate] = field(default_factory=list)

    @property
    def linkable_element_properties(self) -> OrderedSet[LinkableElementProperty]:
        return FrozenOrderedSet(
            mf_flatten(
                update.linkable_element_property_additions
                for update in self._updates
                if update.linkable_element_property_additions is not None
            )
        )

    @property
    def spec(self) -> Optional[SpecResult]:
        dundered_name_elements: list[str] = []
        model_ids = MutableOrderedSet[SemanticModelId]()
        properties = MutableOrderedSet[LinkableElementProperty]()
        for update in self._updates:
            if update is None:
                continue
            if update.dundered_name_element_addition is not None:
                # We currently do not allow query items of the form `entity__entity`.
                if (
                    len(dundered_name_elements) > 0
                    and dundered_name_elements[-1] == update.dundered_name_element_addition
                ):
                    return None
                dundered_name_elements.append(update.dundered_name_element_addition)
            if update.linkable_element_property_additions is not None:
                properties.update(update.linkable_element_property_additions)
            if update.derived_from_model_id_addition is not None:
                model_ids.update((update.derived_from_model_id_addition,))

        name = DUNDER.join(dundered_name_elements)

        return SpecResult(
            name=name,
            model_ids=model_ids,
            properties=properties,
        )

    @property
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        return tuple(
            update.derived_from_model_id_addition.semantic_model_reference
            for update in self._updates
            if update.derived_from_model_id_addition is not None
        )

    def append_update(self, update: AttributeComputationUpdate) -> None:
        self._updates.append(update)

    def pop_update(self) -> None:
        self._updates.pop()


class AttributeComputationUpdater(HasDisplayedProperty, ABC):
    def update_attribute_computation(self, attribute_computation: MutableAttributeComputation) -> None:
        update = self.attribute_computation_update
        if update is not None:
            attribute_computation.append_update(update)

    @cached_property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate()


class RightAttributeType(Enum):
    DIMENSION = "dimension"
    TIME_DIMENSION = "time dimension"
