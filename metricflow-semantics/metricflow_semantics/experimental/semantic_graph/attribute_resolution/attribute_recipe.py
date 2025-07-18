from __future__ import annotations

import logging
from abc import ABC
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.graph_element import HasDisplayedProperty
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe_update import (
    QueryRecipeStep,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_set import (
    KeyQueryGroup,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


_EMPTY_RECIPE_STEP = QueryRecipeStep()


@fast_frozen_dataclass()
class AttributeQueryRecipe:
    dunder_name_elements: AnyLengthTuple[str] = ()
    models_in_join: AnyLengthTuple[SemanticModelId] = ()
    properties: FrozenOrderedSet[LinkableElementProperty] = FrozenOrderedSet()
    entity_link_names: AnyLengthTuple[str] = ()
    key_query_groups: Optional[KeyQueryGroup] = None

    element_type: Optional[LinkableElementType] = None
    min_time_grain: Optional[TimeGranularity] = None
    time_grain: Optional[ExpandedTimeGranularity] = None
    date_part: Optional[DatePart] = None
    deny_date_part: Optional[bool] = None

    @cached_property
    def last_model_id(self) -> Optional[SemanticModelId]:
        if len(self.models_in_join) == 0:
            return None

        return tuple(self.models_in_join)[-1]

    def append_step(self, update: QueryRecipeStep) -> AttributeQueryRecipe:
        if update == _EMPTY_RECIPE_STEP:
            return self

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

        return AttributeQueryRecipe(
            dunder_name_elements=dundered_name_elements,
            models_in_join=models_in_join,
            properties=self.properties.union(update.add_properties)
            if update.add_properties is not None
            else self.properties,
            element_type=update.set_element_type or self.element_type,
            entity_link_names=entity_link_names,
            min_time_grain=update.add_min_time_grain or self.min_time_grain,
            time_grain=update.set_time_grain_access or self.time_grain,
            date_part=update.set_date_part or self.date_part,
            key_query_groups=update.provide_key_query_group or self.key_query_groups,
            deny_date_part=update.set_deny_date_part if update.set_deny_date_part is not None else self.deny_date_part,
        )

    def push_steps(self, *updates: QueryRecipeStep) -> AttributeQueryRecipe:
        result = self
        for update in updates:
            result = self.push_step(update)
        return result

    def push_step(self, update: QueryRecipeStep) -> AttributeQueryRecipe:
        if update == _EMPTY_RECIPE_STEP:
            return self

        dundered_name_elements = self.dunder_name_elements
        if update.add_dunder_name_element is not None:
            dundered_name_elements = (update.add_dunder_name_element,) + dundered_name_elements
        entity_link_names = self.entity_link_names
        if update.add_entity_link is not None:
            entity_link_names = (update.add_entity_link,) + entity_link_names
        models_in_join = self.models_in_join
        if update.join_model is not None:
            if len(models_in_join) == 0:
                models_in_join = (update.join_model,)
            elif models_in_join[0] != update.join_model:
                models_in_join = (update.join_model,) + models_in_join

        return AttributeQueryRecipe(
            dunder_name_elements=dundered_name_elements,
            models_in_join=models_in_join,
            properties=FrozenOrderedSet(update.add_properties).union(self.properties)
            if update.add_properties is not None
            else self.properties,
            element_type=self.element_type or update.set_element_type,
            entity_link_names=entity_link_names,
            min_time_grain=self.min_time_grain or update.add_min_time_grain,
            time_grain=self.time_grain or update.set_time_grain_access,
            date_part=self.date_part or update.set_date_part,
            key_query_groups=self.key_query_groups or update.provide_key_query_group,
            deny_date_part=self.deny_date_part if self.deny_date_part is not None else update.set_deny_date_part,
        )


class AttributeQueryRecipeWriter:
    def __init__(self) -> None:
        self._recipe_versions: list[AttributeQueryRecipe] = list()

    def append_update(self, update: QueryRecipeStep) -> None:
        if len(self._recipe_versions) == 0:
            dunder_name_elements: AnyLengthTuple[str] = ()
            if update.add_dunder_name_element is not None:
                dunder_name_elements = (update.add_dunder_name_element,)
            entity_link_names: AnyLengthTuple[str] = ()
            if update.add_entity_link is not None:
                entity_link_names = (update.add_entity_link,)

            self._recipe_versions.append(
                AttributeQueryRecipe(
                    dunder_name_elements=dunder_name_elements,
                    models_in_join=(update.join_model,) if update.join_model is not None else (),
                    properties=FrozenOrderedSet(update.add_properties or ()),
                    element_type=update.set_element_type,
                    entity_link_names=entity_link_names,
                    min_time_grain=update.add_min_time_grain,
                    time_grain=update.set_time_grain_access,
                    date_part=update.set_date_part,
                )
            )
            return

        previous_recipe = self._recipe_versions[-1]

        self._recipe_versions.append(previous_recipe.append_step(update))
        return

    def pop_update(self) -> None:
        self._recipe_versions.pop()

    @property
    def latest_recipe(self) -> AttributeQueryRecipe:
        if len(self._recipe_versions) == 0:
            return AttributeQueryRecipe()

        return self._recipe_versions[-1]

    @property
    def previous_recipe(self) -> AttributeQueryRecipe:
        if len(self._recipe_versions) <= 1:
            return AttributeQueryRecipe()

        return self._recipe_versions[-2]


class QueryRecipeStepAppender(HasDisplayedProperty, ABC):
    @cached_property
    def recipe_step(self) -> QueryRecipeStep:
        return QueryRecipeStep()
