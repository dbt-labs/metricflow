from __future__ import annotations

import logging
from abc import ABC
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.toolkit.mf_graph.graph_element import HasDisplayedProperty
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class AttributeRecipeStep(HasDisplayedProperty, Comparable):
    """A step that should be added to the `AttributeRecipe`.

    A path from a metric / measure node to an attribute node in the semantic graph describes the computation that is
    required to generate the corresponding query. The computation is represented by a `AttributeRecipe`.

    The nodes / edges in the path specify additions to the recipe. For example, an edge from one entity node to
    another entity node would specify a step to join to a semantic model.

    Currently, the recipe stores aggregated properties, and each step generally adds or sets some of those
    properties. However, the recipe can be updated to include more context so that it can be used in the
    `DataflowPlanBuilder`.
    """

    # Adds an element to the dunder name. For example, the edge from the `user` entity node to the
    # `country_latest` attribute node would add the `country_latest` element.
    add_dunder_name_element: Optional[str] = None
    # Adds a property that describes that attribute. For example, the metric-time entity node would add the
    # `METRIC_TIME` property.
    add_properties: Optional[AnyLengthTuple[GroupByItemProperty]] = None
    add_model_join: Optional[SemanticModelId] = None
    add_entity_link: Optional[str] = None

    # Set the element type. For example. visiting a time-dimension entity node would set this to `TIME_DIMENSION`.
    set_element_type: Optional[LinkableElementType] = None
    # To handle the case where time dimensions have a defined grain, the minimum time grain can be set to limit the
    # attribute nodes accessible later in the path.
    set_source_time_grain: Optional[TimeGranularity] = None
    # With `set_min_time_grain`, `set_time_grain_access` can be used to check for valid paths. e.g. the node associated
    # with a time dimension with a `month` defined grain can block access to edges that specify access to `day`.
    set_time_grain_access: Optional[ExpandedTimeGranularity] = None
    set_date_part_access: Optional[DatePart] = None
    # Some edges prevent access of date part attributes (e.g. cumulative metrics)
    set_deny_date_part: Optional[bool] = None

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (
            self.add_dunder_name_element,
            self.add_properties,
            self.add_model_join,
            self.set_source_time_grain.value if self.set_source_time_grain is not None else None,
            self.set_element_type,
            self.add_entity_link,
            self.set_time_grain_access,
            self.set_date_part_access.to_int() if self.set_date_part_access is not None else None,
            self.set_deny_date_part,
        )

    @override
    @cached_property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties = list(super().displayed_properties)

        if self.add_dunder_name_element is not None:
            properties.append(
                DisplayedProperty("add_name", self.add_dunder_name_element),
            )
        for linkable_element_property_addition in self.add_properties or ():
            properties.append(DisplayedProperty("add_prop", linkable_element_property_addition.name))
        if self.add_model_join is not None:
            properties.append(DisplayedProperty("add_model_join", self.add_model_join.model_name))
        if self.set_source_time_grain is not None:
            properties.append(DisplayedProperty("set_min_grain", self.set_source_time_grain.name))
        if self.set_element_type is not None:
            properties.append(DisplayedProperty("add_type", self.set_element_type.name))
        if self.add_entity_link is not None:
            properties.append(DisplayedProperty("add_entity_link", self.add_entity_link))
        if self.set_time_grain_access is not None:
            properties.append(DisplayedProperty("set_time_grain", self.set_time_grain_access.name))
        if self.set_date_part_access is not None:
            properties.append(DisplayedProperty("set_date_part", self.set_date_part_access.name))
        if self.set_deny_date_part is not None:
            properties.append(DisplayedProperty("set_deny_date_part", self.set_deny_date_part))
        return tuple(properties)


class AttributeRecipeStepProvider(HasDisplayedProperty, ABC):
    """Interface for a class that provides a step that can be appended to a recipe."""

    @cached_property
    def recipe_step_to_append(self) -> AttributeRecipeStep:  # noqa: D102
        return AttributeRecipeStep()
