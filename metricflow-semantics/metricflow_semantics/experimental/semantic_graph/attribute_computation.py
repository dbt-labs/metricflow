from __future__ import annotations

import logging
from abc import ABC
from functools import cached_property
from typing import Optional, override

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.experimental.mf_graph.graph_element import HasDisplayedProperty
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_set import DsiEntityKeyQuerySet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


# class AttributeRecipeWriter(SemanticModelDerivation, ABC):
#     @property
#     @abstractmethod
#     def latest_recipe(self) -> Optional[AttributeRecipe]:
#         raise NotImplementedError()


@fast_frozen_dataclass(order=False)
class AttributeRecipeUpdate(HasDisplayedProperty, Comparable):
    add_dunder_name_element: Optional[str] = None
    add_properties: AnyLengthTuple[LinkableElementProperty] = ()
    join_model: Optional[SemanticModelId] = None
    add_min_time_grain: Optional[TimeGranularity] = None
    provide_key_query_set: Optional[DsiEntityKeyQuerySet] = None

    # The fields below are specifically to support current definition of `*Spec` objects.
    set_element_type: Optional[LinkableElementType] = None
    add_entity_link: Optional[str] = None
    set_time_grain: Optional[ExpandedTimeGranularity] = None
    set_date_part: Optional[DatePart] = None

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (
            self.add_dunder_name_element,
            self.add_properties,
            self.join_model,
            self.add_min_time_grain.value if self.add_min_time_grain is not None else None,
            self.set_element_type,
            self.add_entity_link,
            self.set_time_grain,
            self.set_date_part.to_int() if self.set_date_part is not None else None,
            self.provide_key_query_set,
        )

    @override
    @cached_property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties = list(super().displayed_properties)

        if self.add_dunder_name_element is not None:
            properties.append(
                DisplayedProperty("add_name", self.add_dunder_name_element),
            )
        for linkable_element_property_addition in self.add_properties:
            properties.append(DisplayedProperty("add_prop", linkable_element_property_addition.name))

        if self.join_model is not None:
            properties.append(DisplayedProperty("join_model", self.join_model.model_name))
        if self.add_min_time_grain is not None:
            properties.append(DisplayedProperty("set_min_grain", self.add_min_time_grain.name))
        if self.provide_key_query_set is not None:
            for i, key_query in enumerate(self.provide_key_query_set.entity_key_queries):
                properties.append(DisplayedProperty(f"key_query_{i}", key_query.query_dunder_name_elements))
                properties.append(
                    DisplayedProperty(
                        f"key_query_{i}_models", [model_id.model_name for model_id in key_query.accessed_model_ids]
                    )
                )
        if self.set_element_type is not None:
            properties.append(DisplayedProperty("add_type", self.set_element_type.name))
        if self.add_entity_link is not None:
            properties.append(DisplayedProperty("add_entity_link", self.add_entity_link))
        if self.set_time_grain is not None:
            properties.append(DisplayedProperty("set_time_grain", self.set_time_grain.name))
        if self.set_date_part is not None:
            properties.append(DisplayedProperty("set_date_part", self.set_date_part.name))

        return tuple(properties)


@fast_frozen_dataclass()
class AttributeRecipe:
    dunder_name_elements: AnyLengthTuple[str] = ()
    models_in_join: AnyLengthTuple[SemanticModelId] = ()
    properties: FrozenOrderedSet[LinkableElementProperty] = FrozenOrderedSet()
    entity_link_names: AnyLengthTuple[str] = ()
    key_query_set: Optional[DsiEntityKeyQuerySet] = None

    element_type: Optional[LinkableElementType] = None
    min_time_grain: Optional[TimeGranularity] = None
    time_grain: Optional[ExpandedTimeGranularity] = None
    date_part: Optional[DatePart] = None

    @cached_property
    def last_model_id(self) -> Optional[SemanticModelId]:
        if len(self.models_in_join) == 0:
            return None

        return tuple(self.models_in_join)[-1]

    def with_update(self, update: AttributeRecipeUpdate) -> AttributeRecipe:
        dundered_name_elements = self.dunder_name_elements
        if update.add_dunder_name_element is not None:
            dundered_name_elements = dundered_name_elements + (update.add_dunder_name_element,)
        entity_link_names = self.entity_link_names
        if update.add_entity_link is not None:
            entity_link_names = entity_link_names + (update.add_entity_link,)
        models_in_join = self.models_in_join
        if update.join_model is not None:
            if len(models_in_join) == 0:
                models_in_join = (update.join_model,)
            elif models_in_join[-1] != update.join_model:
                models_in_join = models_in_join + (update.join_model,)

        return AttributeRecipe(
            dunder_name_elements=dundered_name_elements,
            models_in_join=models_in_join,
            properties=self.properties.union(update.add_properties),
            element_type=update.set_element_type or self.element_type,
            entity_link_names=entity_link_names,
            min_time_grain=update.add_min_time_grain or self.min_time_grain,
            time_grain=update.set_time_grain or self.time_grain,
            date_part=update.set_date_part or self.date_part,
            key_query_set=update.provide_key_query_set or self.key_query_set,
        )


class AttributeRecipeWriter:
    def __init__(self) -> None:
        self._recipe_versions: list[AttributeRecipe] = list()

    def append_update(self, update: AttributeRecipeUpdate) -> None:
        if len(self._recipe_versions) == 0:
            dunder_name_elements: AnyLengthTuple[str] = ()
            if update.add_dunder_name_element is not None:
                dunder_name_elements = (update.add_dunder_name_element,)
            entity_link_names: AnyLengthTuple[str] = ()
            if update.add_entity_link is not None:
                entity_link_names = (update.add_entity_link,)

            self._recipe_versions.append(
                AttributeRecipe(
                    dunder_name_elements=dunder_name_elements,
                    models_in_join=(update.join_model,) if update.join_model is not None else (),
                    properties=FrozenOrderedSet(update.add_properties),
                    element_type=update.set_element_type,
                    entity_link_names=entity_link_names,
                    min_time_grain=update.add_min_time_grain,
                    time_grain=update.set_time_grain,
                    date_part=update.set_date_part,
                )
            )
            return

        previous_recipe = self._recipe_versions[-1]

        self._recipe_versions.append(previous_recipe.with_update(update))
        return

    def pop_update(self) -> None:
        self._recipe_versions.pop()

    @property
    def latest_recipe(self) -> AttributeRecipe:
        if len(self._recipe_versions) == 0:
            return AttributeRecipe()

        return self._recipe_versions[-1]

    @property
    def previous_recipe(self) -> AttributeRecipe:
        if len(self._recipe_versions) <= 1:
            return AttributeRecipe()

        return self._recipe_versions[-2]


class AttributeRecipeUpdateSource(HasDisplayedProperty, ABC):
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate()
