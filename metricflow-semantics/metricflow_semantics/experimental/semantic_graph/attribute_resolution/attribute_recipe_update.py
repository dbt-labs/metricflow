from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional, override

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.experimental.mf_graph.graph_element import HasDisplayedProperty
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_set import KeyQueryGroup
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class QueryRecipeStep(HasDisplayedProperty, Comparable):
    add_dunder_name_element: Optional[str] = None
    add_properties: Optional[AnyLengthTuple[LinkableElementProperty]] = None
    join_model: Optional[SemanticModelId] = None
    add_min_time_grain: Optional[TimeGranularity] = None
    provide_key_query_group: Optional[KeyQueryGroup] = None

    # The fields below are specifically to support current definition of `*Spec` objects.
    set_element_type: Optional[LinkableElementType] = None
    add_entity_link: Optional[str] = None
    set_time_grain_access: Optional[ExpandedTimeGranularity] = None
    set_date_part: Optional[DatePart] = None
    set_deny_date_part: Optional[bool] = None

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (
            self.add_dunder_name_element,
            self.add_properties,
            self.join_model,
            self.add_min_time_grain.value if self.add_min_time_grain is not None else None,
            self.provide_key_query_group,
            self.set_element_type,
            self.add_entity_link,
            self.set_time_grain_access,
            self.set_date_part.to_int() if self.set_date_part is not None else None,
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

        if self.join_model is not None:
            properties.append(DisplayedProperty("join_model", self.join_model.model_name))
        if self.add_min_time_grain is not None:
            properties.append(DisplayedProperty("set_min_grain", self.add_min_time_grain.name))
        if self.provide_key_query_group is not None:
            for i, (key_query, model_ids) in enumerate(self.provide_key_query_group.items()):
                properties.append(DisplayedProperty(f"key_query_{i}", key_query))
                properties.append(DisplayedProperty(f"key_query_{i}_models", model_ids))
        if self.set_element_type is not None:
            properties.append(DisplayedProperty("add_type", self.set_element_type.name))
        if self.add_entity_link is not None:
            properties.append(DisplayedProperty("add_entity_link", self.add_entity_link))
        if self.set_time_grain_access is not None:
            properties.append(DisplayedProperty("set_time_grain", self.set_time_grain_access.name))
        if self.set_date_part is not None:
            properties.append(DisplayedProperty("set_date_part", self.set_date_part.name))
        if self.set_deny_date_part is not None:
            properties.append(DisplayedProperty("set_deny_date_part", self.set_deny_date_part))
        return tuple(properties)
