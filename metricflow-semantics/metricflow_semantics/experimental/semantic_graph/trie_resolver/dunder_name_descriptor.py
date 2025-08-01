from __future__ import annotations

import logging
from typing import Optional

from dbt_semantic_interfaces.type_enums import DatePart

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe import IndexedDunderName
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class EntityKeyQueryForGroupByMetric:
    """Represents a possible entity-key query for group-by metric."""

    entity_key_query: IndexedDunderName
    derived_from_model_ids: AnyLengthTuple[SemanticModelId]


@fast_frozen_dataclass()
class DunderNameDescriptor:
    """A descriptor used in `DunderNameTrie` that can be used to build a `AnnotatedSpec` later.

    The fields here are analogous to the fields in `AnnotatedSpec`.

    These descriptors are used in trie-based data structures that describe the available group-by items for a metric (
    see `DunderNameTrie`). In these structures, the dunder name maps to this descriptor.

    Some consolidation is needed between similar descriptor types.
    """

    element_type: LinkableElementType
    time_grain: Optional[ExpandedTimeGranularity]
    date_part: Optional[DatePart]

    element_properties: AnyLengthTuple[LinkableElementProperty]
    origin_model_ids: AnyLengthTuple[SemanticModelId]
    derived_from_model_ids: AnyLengthTuple[SemanticModelId]
    entity_key_queries_for_group_by_metric: AnyLengthTuple[EntityKeyQueryForGroupByMetric]

    def union(self, other: DunderNameDescriptor) -> DunderNameDescriptor:
        """Combine the properties of this with the other descriptor.

        To generate the trie for a metric from the corresponding trie for the constituent measures, there
        needs to be a mechanism for combining them to reflect correct properties like `derived_from_model_ids`.

        For example, a metric like `views_per_booking` would be based on the `views` measure (in the `views_source`
        model) and the `booking` measure (in the `booking_source` model). Combining the trie from each model should
        result in descriptors that show that the `derived_from_model_ids` includes both `views_source` and
        `bookings_source`.
        """
        return DunderNameDescriptor(
            element_type=self.element_type,
            time_grain=self.time_grain,
            date_part=self.date_part,
            element_properties=self.element_properties + other.element_properties,
            origin_model_ids=self.origin_model_ids + other.origin_model_ids,
            derived_from_model_ids=self.derived_from_model_ids + other.derived_from_model_ids,
            entity_key_queries_for_group_by_metric=self.entity_key_queries_for_group_by_metric,
        )

    def union_derived_from_model_ids(
        self, derived_from_model_ids: AnyLengthTuple[SemanticModelId]
    ) -> DunderNameDescriptor:
        """Return a new descriptor that is a copy of this but with additional `derived_from_model_ids`."""
        return DunderNameDescriptor(
            element_type=self.element_type,
            time_grain=self.time_grain,
            date_part=self.date_part,
            element_properties=self.element_properties,
            origin_model_ids=self.origin_model_ids,
            derived_from_model_ids=self.derived_from_model_ids + derived_from_model_ids,
            entity_key_queries_for_group_by_metric=self.entity_key_queries_for_group_by_metric,
        )
