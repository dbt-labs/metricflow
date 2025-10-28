from __future__ import annotations

import itertools
import logging
from functools import cached_property
from typing import Mapping, Optional, Set

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe import AttributeRecipe
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.semantic_graph.nodes.node_labels import (
    GroupByAttributeLabel,
    TimeClusterLabel,
    TimeDimensionLabel,
)
from metricflow_semantics.semantic_graph.sg_exceptions import SemanticGraphTraversalError
from metricflow_semantics.semantic_graph.sg_interfaces import SemanticGraphEdge, SemanticGraphNode
from metricflow_semantics.toolkit.mf_graph.path_finding.weight_function import WeightFunction
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


class AttributeRecipeWriterWeightFunction(
    WeightFunction[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]
):
    """The weight function to use with `AttributeRecipeWriterPath`.

    The weight function maps a path in the semantic graph to the number of entity links in the dunder name associated
    with the path / attribute.

    In addition, this weight function models the characteristics of the query interface by returning `None` in certain
    cases. `None` signals to the pathfinder that the edge is blocked. This weight function returns `None` to prohibit:

    * Repeated name elements in a dunder name (e.g. `listing__listing`).
    * Repeated semantic models in a join.
    * Querying a time dimension at a time grain that's smaller than the one configured in the semantic model.

    In addition, this can be initialized with a `GroupByItemSetFilter` to handle filtering of results as required by
    the MF engine API.

    As the methods in this class are called repeatedly in a relatively tight loop during resolution, the performance of
    this class is critical. Hence, there are a number of early-exit checks.
    """

    def __init__(  # noqa: D107
        self, element_filter: Optional[GroupByItemSetFilter] = None, max_path_model_count: Optional[int] = None
    ) -> None:
        """Initializer.

        Args:
            element_filter: If specified, limits paths to align with the given filter.
            max_path_model_count: To handle some edge cases, this can be specified to limit the number of semantic
            models in the path.
        """
        self._element_filter = element_filter
        self._group_by_attribute_label = GroupByAttributeLabel.get_instance()
        self._time_dimension_label = TimeDimensionLabel.get_instance()
        self._time_cluster_label = TimeClusterLabel.get_instance()
        self._max_path_model_count = max_path_model_count

        self._verbose_debug_logs = False

    @override
    def incremental_weight(
        self,
        path_to_node: AttributeRecipeWriterPath,
        next_edge: SemanticGraphEdge,
    ) -> Optional[int]:
        next_node = next_edge.head_node
        current_recipe = path_to_node.latest_recipe
        # First run checks that can be done without the next recipe (the profiler showed non-insignificant time spent
        # generating recipes).
        next_edge_step = next_edge.recipe_step_to_append
        next_node_step = next_node.recipe_step_to_append

        element_filter = self._element_filter

        # We do not allow repeated element names in the dundered name (e.g. `listing__listing`).
        if AttributeRecipeWriterWeightFunction.repeated_dunder_name_elements(
            current_recipe, next_edge_step, next_node_step
        ):
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Blocking edge due to a repeated name element.",
                        next_edge_step=next_edge_step,
                        next_node_step=next_node_step,
                    )
                )
            return None

        # Don't allow joining a semantic model multiple times.
        if AttributeRecipeWriterWeightFunction.repeated_model_join(current_recipe, next_edge_step, next_node_step):
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Blocking edge due to a repeated model.",
                        models_in_join=current_recipe.joined_model_ids,
                        next_edge_step=next_edge_step,
                        next_node_step=next_node_step,
                    )
                )
            return None

        # A quick check to see if the filter will block the edge.
        if element_filter and self._filter_denies_edge(element_filter, next_node, (next_edge_step, next_node_step)):
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Blocking edge due to the element filter.",
                        next_edge=next_edge,
                        next_edge_step=next_edge_step,
                        next_node_step=next_node_step,
                        element_filter=self._element_filter,
                    )
                )
            return None

        weight_added_by_taking_edge = 0
        if next_edge.recipe_step_to_append.add_entity_link is not None:
            weight_added_by_taking_edge += 1
        if next_edge.head_node.recipe_step_to_append.add_entity_link is not None:
            weight_added_by_taking_edge += 1

        if len(current_recipe.entity_link_names) + weight_added_by_taking_edge > MAX_JOIN_HOPS:
            return None

        # Check if this needs to limit joins.
        if self._max_path_model_count is not None:
            current_path_model_count = len(current_recipe.joined_model_ids)
            if next_edge_step.add_model_join:
                current_path_model_count += 1
            if next_node_step.add_model_join:
                current_path_model_count += 1
            if current_path_model_count > self._max_path_model_count:
                return None

        # If the current path is not yet at an attribute node, we can't run the checks below so return early.
        next_node_is_attribute = self._group_by_attribute_label in next_node.labels
        if not next_node_is_attribute:
            return weight_added_by_taking_edge

        next_edge_update = next_edge.recipe_step_to_append
        next_node_update = next_edge.head_node.recipe_step_to_append
        next_recipe = current_recipe.append_step(next_edge_update).append_step(next_node_update)

        # Require entity links for dimensions / time dimensions, except for metric time
        if self._invalid_entity_links(next_recipe):
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Blocking edge due to entity links being outside of range.",
                        element_type=next_recipe.element_type,
                        entity_link_names=next_recipe.entity_link_names,
                        models_in_join=next_recipe.joined_model_ids,
                    )
                )
            return None

        if self._source_time_grain_mismatch(next_recipe):
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Blocking edge due to source time-grain mismatch.",
                        next_edge=next_edge,
                        next_recipe=next_recipe,
                    )
                )
            return None

        if element_filter and self._filter_denies_recipe(element_filter, next_recipe):
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Blocking edge due to the element filter.",
                        next_edge=next_edge,
                        next_recipe=next_recipe,
                        element_filter=self._element_filter,
                    )
                )
            return None

        return weight_added_by_taking_edge

    def _filter_denies_edge(
        self,
        element_filter: GroupByItemSetFilter,
        next_node: SemanticGraphNode,
        steps: AnyLengthTuple[AttributeRecipeStep],
    ) -> bool:
        any_properties_denylist = element_filter.any_properties_denylist
        if any_properties_denylist and any(
            element_property in any_properties_denylist
            for element_property in itertools.chain.from_iterable(
                step.add_properties for step in steps if step.add_properties
            )
        ):
            return True

        name_element_allow_set = element_filter.element_name_allowlist
        # Generally, element name should be checked at the attribute node, but it can be done at time dimension nodes
        # as they are an entity node that adds an element name. This can speed up traversal by pruning edges.
        if (
            name_element_allow_set
            and self._time_dimension_label in next_node.labels
            and not all(
                name_element in name_element_allow_set
                for name_element in (
                    step.add_dunder_name_element for step in steps if step.add_dunder_name_element is not None
                )
            )
        ):
            return True

        return False

    def _filter_denies_recipe(
        self,
        element_filter: GroupByItemSetFilter,
        next_recipe: AttributeRecipe,
    ) -> bool:
        """Check if the element filter denies the recipe associated with the path.

        As per call in the incremental weight function, this should only be called when the next node is an attribute
        node.
        """
        next_property_set = next_recipe.resolve_complete_properties()
        next_element_name = next_recipe.resolve_element_name()
        if next_property_set is None or next_element_name is None:
            raise SemanticGraphTraversalError(
                LazyFormat(
                    "Expected the recipe at an attribute node or a time-attribute node to have complete"
                    " properties and an element name, but it is missing at least one of them.",
                    next_property_set=next_property_set,
                    element_name=next_element_name,
                    next_recipe=next_recipe,
                )
            )

        return not element_filter.allow(next_element_name, next_property_set)

    @staticmethod
    def repeated_dunder_name_elements(
        recipe: AttributeRecipe,
        step: AttributeRecipeStep,
        other_step: AttributeRecipeStep,
    ) -> bool:
        """Return true if the recipe + steps would cause a repeated element in the dunder name."""
        next_edge_add_dunder_name_element = step.add_dunder_name_element
        next_node_add_dunder_name_element = other_step.add_dunder_name_element
        if next_edge_add_dunder_name_element is None and next_node_add_dunder_name_element is None:
            return False

        current_dunder_name_elements = recipe.dunder_name_elements_set
        return (
            next_node_add_dunder_name_element
            and next_node_add_dunder_name_element in current_dunder_name_elements
            or next_edge_add_dunder_name_element
            and next_edge_add_dunder_name_element in current_dunder_name_elements
            or next_edge_add_dunder_name_element == next_node_add_dunder_name_element
        )

    @staticmethod
    def repeated_model_join(
        recipe: AttributeRecipe,
        step: AttributeRecipeStep,
        other_step: AttributeRecipeStep,
    ) -> bool:
        """Return true if the recipe + steps would cause a repeated semantic model in the join."""
        edge_add_model_join = step.add_model_join
        node_add_model_join = other_step.add_model_join
        if edge_add_model_join is None and node_add_model_join is None:
            return False
        if edge_add_model_join == node_add_model_join:
            return True

        for model_in_join in recipe.joined_model_ids:
            if model_in_join == edge_add_model_join or model_in_join == node_add_model_join:
                return True
        return False

    def _invalid_entity_links(self, next_recipe: AttributeRecipe) -> bool:
        # Require entity links for dimensions / time dimensions, except for metric time
        next_recipe_element_type = next_recipe.element_type
        if next_recipe_element_type is None:
            return False

        # Doing a max for the join count to allow this weight function to be used for paths that haven't yet
        # included a semantic model.
        join_count = max(0, len(next_recipe.joined_model_ids) - 1)
        min_entity_link_length = 1
        if join_count == 0:
            max_entity_link_length = 1
        else:
            max_entity_link_length = join_count

        if next_recipe_element_type is LinkableElementType.ENTITY:
            min_entity_link_length = 0
        elif (
            next_recipe_element_type is LinkableElementType.TIME_DIMENSION
            or next_recipe_element_type is LinkableElementType.DIMENSION
        ):
            if GroupByItemProperty.METRIC_TIME in next_recipe.element_properties:
                min_entity_link_length = 0

        elif next_recipe_element_type is LinkableElementType.METRIC:
            pass
        else:
            assert_values_exhausted(next_recipe_element_type)

        next_entity_link_length = len(next_recipe.entity_link_names)
        return not (min_entity_link_length <= next_entity_link_length <= max_entity_link_length)

    def _source_time_grain_mismatch(self, next_recipe: AttributeRecipe) -> bool:
        source_time_grain = next_recipe.source_time_grain
        recipe_time_grain = next_recipe.recipe_time_grain
        if source_time_grain is None:
            return False

        if recipe_time_grain is not None and recipe_time_grain.base_granularity.to_int() < source_time_grain.to_int():
            return True

        date_part = next_recipe.recipe_date_part
        if date_part is None:
            return False

        return source_time_grain not in self._date_part_to_min_time_grain[date_part]

    @cached_property
    def _date_part_to_min_time_grain(self) -> Mapping[DatePart, Set[TimeGranularity]]:
        return {date_part: set(date_part.compatible_granularities) for date_part in DatePart}
