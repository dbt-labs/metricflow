from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional, Set

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity

from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


IndexedDunderName = AnyLengthTuple[str]


@fast_frozen_dataclass()
class AttributeRecipe:
    """The recipe for computing an attribute by following a path in the semantic graph."""

    indexed_dunder_name: IndexedDunderName = ()
    joined_model_ids: AnyLengthTuple[SemanticModelId] = ()
    element_properties: FrozenOrderedSet[GroupByItemProperty] = FrozenOrderedSet()
    entity_link_names: AnyLengthTuple[str] = ()

    element_type: Optional[LinkableElementType] = None
    # Maps to the time grain set for a time dimension in a semantic model
    source_time_grain: Optional[TimeGranularity] = None
    # Maps to the attribute's time grain or date part.
    recipe_time_grain: Optional[ExpandedTimeGranularity] = None
    recipe_date_part: Optional[DatePart] = None

    @cached_property
    def dunder_name_elements_set(self) -> Set[str]:
        """The elements of a dunder name as a set for fast repeated-element checks."""
        return set(self.indexed_dunder_name)

    @cached_property
    def joined_model_id_set(self) -> Set[SemanticModelId]:
        """The joined semantic model IDs as a set for fast repeated-model checks."""
        return set(self.joined_model_ids)

    @staticmethod
    def create(initial_step: AttributeRecipeStep) -> AttributeRecipe:  # noqa: D102
        dunder_name_elements: AnyLengthTuple[str] = ()
        if initial_step.add_dunder_name_element is not None:
            dunder_name_elements = (initial_step.add_dunder_name_element,)
        entity_link_names: AnyLengthTuple[str] = ()
        if initial_step.add_entity_link is not None:
            entity_link_names = (initial_step.add_entity_link,)

        models_in_join: AnyLengthTuple[SemanticModelId] = ()

        add_model_join = initial_step.add_model_join
        if add_model_join is not None:
            models_in_join = models_in_join + (add_model_join,)

        return AttributeRecipe(
            indexed_dunder_name=dunder_name_elements,
            joined_model_ids=models_in_join,
            element_properties=FrozenOrderedSet(initial_step.add_properties or ()),
            element_type=initial_step.set_element_type,
            entity_link_names=entity_link_names,
            source_time_grain=initial_step.set_source_time_grain,
            recipe_time_grain=initial_step.set_time_grain_access,
            recipe_date_part=initial_step.set_date_part_access,
        )

    @cached_property
    def last_model_id(self) -> Optional[SemanticModelId]:
        """The last model ID that was added to the join."""
        if self.joined_model_ids:
            return None

        return tuple(self.joined_model_ids)[-1]

    def append_step(self, recipe_step: AttributeRecipeStep) -> AttributeRecipe:
        """Add a step to the end of the recipe."""
        dundered_name_elements = self.indexed_dunder_name
        if recipe_step.add_dunder_name_element is not None:
            dundered_name_elements = dundered_name_elements + (recipe_step.add_dunder_name_element,)
        entity_link_names = self.entity_link_names
        if recipe_step.add_entity_link is not None:
            entity_link_names = entity_link_names + (recipe_step.add_entity_link,)

        models_in_join = self.joined_model_ids
        join_model = recipe_step.add_model_join

        if join_model is not None:
            models_in_join = models_in_join + (join_model,)

        return AttributeRecipe(
            indexed_dunder_name=dundered_name_elements,
            joined_model_ids=models_in_join,
            element_properties=self.element_properties.union(recipe_step.add_properties)
            if recipe_step.add_properties is not None
            else self.element_properties,
            element_type=recipe_step.set_element_type or self.element_type,
            entity_link_names=entity_link_names,
            source_time_grain=recipe_step.set_source_time_grain or self.source_time_grain,
            recipe_time_grain=recipe_step.set_time_grain_access or self.recipe_time_grain,
            recipe_date_part=recipe_step.set_date_part_access or self.recipe_date_part,
        )

    def push_step(self, recipe_step: AttributeRecipeStep) -> AttributeRecipe:
        """Add a step to the beginning of the recipe."""
        dundered_name_elements = self.indexed_dunder_name
        if recipe_step.add_dunder_name_element is not None:
            dundered_name_elements = (recipe_step.add_dunder_name_element,) + dundered_name_elements
        entity_link_names = self.entity_link_names
        if recipe_step.add_entity_link is not None:
            entity_link_names = (recipe_step.add_entity_link,) + entity_link_names
        models_in_join = self.joined_model_ids
        add_model_join = recipe_step.add_model_join
        if add_model_join is not None:
            if len(models_in_join) == 0:
                models_in_join = (add_model_join,)
            else:
                models_in_join = (add_model_join,) + models_in_join

        return AttributeRecipe(
            indexed_dunder_name=dundered_name_elements,
            joined_model_ids=models_in_join,
            element_properties=FrozenOrderedSet(recipe_step.add_properties).union(self.element_properties)
            if recipe_step.add_properties is not None
            else self.element_properties,
            element_type=self.element_type or recipe_step.set_element_type,
            entity_link_names=entity_link_names,
            source_time_grain=self.source_time_grain or recipe_step.set_source_time_grain,
            recipe_time_grain=self.recipe_time_grain or recipe_step.set_time_grain_access,
            recipe_date_part=self.recipe_date_part or recipe_step.set_date_part_access,
        )

    def push_steps(self, *updates: AttributeRecipeStep) -> AttributeRecipe:
        """See `push_step`."""
        result = self
        for update in updates:
            result = result.push_step(update)
        return result

    def resolve_complete_properties(self) -> OrderedSet[GroupByItemProperty]:
        """Resolve the complete set of `GroupByItemProperty` for this recipe.

        While many properties were set by recipe steps during traversal, some need to be resolved at the end as it
        is easier / faster to determine at the end.
        """
        element_type = self.element_type

        if element_type is None:
            raise ValueError(LazyFormat("Recipe is missing the element type", recipe=self))

        properties = MutableOrderedSet(self.element_properties)

        model_ids = self.joined_model_ids
        model_id_count = len(model_ids)
        if model_id_count == 0:
            if GroupByItemProperty.METRIC_TIME not in properties:
                raise ValueError(LazyFormat("Recipe is missing context on accessed semantic models", recipe=self))
        elif model_id_count == 1:
            if element_type is not LinkableElementType.METRIC and GroupByItemProperty.METRIC_TIME not in properties:
                properties.add(GroupByItemProperty.LOCAL)
        elif model_id_count == 2:
            properties.add(GroupByItemProperty.JOINED)
        elif model_id_count >= 3:
            properties.update(
                (
                    GroupByItemProperty.JOINED,
                    GroupByItemProperty.MULTI_HOP,
                )
            )
        else:
            raise MetricFlowInternalError(
                LazyFormat("Reached unhandled case", model_id_count=model_id_count, recipe=self)
            )

        # Add `DERIVED_TIME_GRANULARITY` if the grain is different from the element's grain.
        source_time_grain = self.source_time_grain
        recipe_time_grain = self.recipe_time_grain
        if source_time_grain is not None:
            if recipe_time_grain is None and self.recipe_date_part is None:
                raise ValueError(
                    LazyFormat(
                        "Recipe has a source time-grain, but no recipe time-grain or recipe date-part", recipe=self
                    )
                )
            if recipe_time_grain is not None and source_time_grain is not recipe_time_grain.base_granularity:
                properties.add(GroupByItemProperty.DERIVED_TIME_GRANULARITY)

        return properties

    def resolve_element_name(self) -> Optional[str]:
        """Resolve the element name.

        Currently, the recipe stores the dunder-name elements (e.g. ["metric_time", "day"]), but not the element name.
        Since the position of the element name in the list depends on the type of element, this method helps to resolve
        that.
        """
        element_type = self.element_type

        # Incomplete recipe.
        if element_type is None:
            return None

        dunder_name_elements = self.indexed_dunder_name
        dunder_name_element_count = len(dunder_name_elements)

        # Incomplete recipe.
        if dunder_name_element_count == 0:
            return None

        if element_type is LinkableElementType.TIME_DIMENSION:
            # e.g. ['metric_time']
            if dunder_name_element_count == 1:
                return dunder_name_elements[-1]
            # e.g. ['metric_time', 'day']
            else:
                return dunder_name_elements[-2]
        elif (
            element_type is LinkableElementType.ENTITY
            or element_type is LinkableElementType.DIMENSION
            or element_type is LinkableElementType.TIME_DIMENSION
            or element_type is LinkableElementType.METRIC
        ):
            return dunder_name_elements[-1]
        else:
            assert_values_exhausted(element_type)
