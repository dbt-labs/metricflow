from __future__ import annotations

import logging
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.errors.error_classes import FeatureNotSupportedError
from metricflow_semantics.toolkit.cache.lru_cache import typed_lru_cache
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

DUNDER = "__"

logger = logging.getLogger(__name__)


class StructuredLinkableSpecName:
    """Parse a qualified name into different parts.

    e.g. listing__ds__week ->
    entity_links: ["listing"]
    element_name: "ds"
    granularity: TimeGranularity.WEEK
    """

    def __init__(
        self,
        entity_link_names: AnyLengthTuple[str],
        element_name: str,
        time_granularity_name: Optional[str] = None,
        date_part: Optional[DatePart] = None,
        metric_subquery_entity_link_names: Optional[AnyLengthTuple[str]] = None,
    ) -> None:
        """Set attributes, ensuring names are lowercased."""
        self.entity_link_names = tuple(entity_link_name.lower() for entity_link_name in entity_link_names)
        self.element_name = element_name.lower()
        self.time_granularity_name = time_granularity_name.lower() if time_granularity_name else None
        self.date_part = date_part
        self.metric_subquery_entity_link_names = metric_subquery_entity_link_names

    @staticmethod
    @typed_lru_cache
    def from_name(qualified_name: str, custom_granularity_names: Sequence[str]) -> StructuredLinkableSpecName:
        """Construct from a name e.g. listing__ds__month."""
        name_parts = qualified_name.split(DUNDER)

        # No dunder, e.g. "ds"
        if len(name_parts) == 1:
            return StructuredLinkableSpecName(entity_link_names=(), element_name=name_parts[0])

        for date_part in DatePart:
            if name_parts[-1] == StructuredLinkableSpecName.date_part_suffix(date_part=date_part):
                raise FeatureNotSupportedError(
                    "Dunder syntax not supported for querying date_part. Use `group_by` object syntax instead."
                )

        associated_granularity: Optional[str] = None
        for granularity in TimeGranularity:
            if name_parts[-1] == granularity.value:
                associated_granularity = granularity.value
                break

        if associated_granularity is None:
            for custom_grain in custom_granularity_names:
                if name_parts[-1] == custom_grain:
                    associated_granularity = custom_grain
                    break

        # Has a time granularity
        if associated_granularity:
            #  e.g. "ds__month"
            if len(name_parts) == 2:
                return StructuredLinkableSpecName(
                    entity_link_names=(), element_name=name_parts[0], time_granularity_name=associated_granularity
                )
            # e.g. "messages__ds__month"
            return StructuredLinkableSpecName(
                entity_link_names=tuple(name_parts[:-2]),
                element_name=name_parts[-2],
                time_granularity_name=associated_granularity,
            )

        # e.g. "messages__ds"
        else:
            return StructuredLinkableSpecName(entity_link_names=tuple(name_parts[:-1]), element_name=name_parts[-1])

    @property
    def dunder_name(self) -> str:
        """Return the full dunder name. e.g. ds or listing__ds__month.

        If date_part is specified, don't include granularity in dunder_name since it will not impact the result.

        If `metric_subquery_entity_link_names` is specified, this represents a metric. Metrics follow a different
        format - if same entity links are used in inner & outer query, use standard qualified name (country__bookings).
        Else, specify both sets of entity links (listing__country__user__country__bookings).
        """
        entity_link_names = self.entity_link_names
        if self.metric_subquery_entity_link_names is not None:
            if self.entity_link_names != self.metric_subquery_entity_link_names:
                entity_link_names = self.entity_link_names + self.metric_subquery_entity_link_names

        items = list(entity_link_names) + [self.element_name]
        if self.date_part:
            items.append(self.date_part_suffix(date_part=self.date_part))
        elif self.time_granularity_name:
            items.append(self.time_granularity_name)
        return DUNDER.join(items)

    @property
    def entity_prefix(self) -> Optional[str]:
        """Return the entity prefix. e.g. listing__ds__month -> listing."""
        if len(self.entity_link_names) > 0:
            return DUNDER.join(self.entity_link_names)

        return None

    @staticmethod
    def date_part_suffix(date_part: DatePart) -> str:
        """Suffix used for names with a date_part."""
        return f"extract_{date_part.value}"

    @property
    def entity_links(self) -> Tuple[EntityReference, ...]:
        """Returns the entity link references."""
        return tuple(EntityReference(entity_link_name.lower()) for entity_link_name in self.entity_link_names)

    @property
    def granularity_free_dunder_name(self) -> str:
        """Renders the qualified name without the granularity suffix.

        In the list metrics and list dimensions outputs we want to render the qualified name of the dimension, but
        without including the base granularity for time dimensions. This method is useful in those contexts.
        Note: in most cases you should be using the dunder_name - this is only useful in cases where the
        Dimension set has de-duplicated TimeDimensions such that you never have more than one granularity
        in your set for each TimeDimension.
        """
        return StructuredLinkableSpecName(
            entity_link_names=self.entity_link_names, element_name=self.element_name
        ).dunder_name

    @property
    def is_element_name(self) -> bool:
        """Indicates whether or not this is an unadorned element name, i.e., one without links or time annotations."""
        return self.dunder_name == self.element_name
