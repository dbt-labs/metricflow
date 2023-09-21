from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.time.date_part import DatePart

DUNDER = "__"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StructuredLinkableSpecName:
    """Parse a qualified name into different parts.

    e.g. listing__ds__week ->
    entity_links: ["listing"]
    element_name: "ds"
    granularity: TimeGranularity.WEEK
    """

    entity_link_names: Tuple[str, ...]
    element_name: str
    time_granularity: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    @staticmethod
    def from_name(qualified_name: str) -> StructuredLinkableSpecName:
        """Construct from a name e.g. listing__ds__month."""
        name_parts = qualified_name.split(DUNDER)

        # No dunder, e.g. "ds"
        if len(name_parts) == 1:
            return StructuredLinkableSpecName(entity_link_names=(), element_name=name_parts[0])

        for date_part in DatePart:
            if name_parts[-1] == StructuredLinkableSpecName.date_part_suffix(date_part=date_part):
                raise ValueError(
                    "Dunder syntax not supported for querying date_part. Use `group_by` object syntax instead."
                )

        associated_granularity = None
        for granularity in TimeGranularity:
            if name_parts[-1] == granularity.value:
                associated_granularity = granularity

        # Has a time granularity
        if associated_granularity:
            #  e.g. "ds__month"
            if len(name_parts) == 2:
                return StructuredLinkableSpecName(
                    entity_link_names=(), element_name=name_parts[0], time_granularity=associated_granularity
                )
            # e.g. "messages__ds__month"
            return StructuredLinkableSpecName(
                entity_link_names=tuple(name_parts[:-2]),
                element_name=name_parts[-2],
                time_granularity=associated_granularity,
            )

        # e.g. "messages__ds"
        else:
            return StructuredLinkableSpecName(entity_link_names=tuple(name_parts[:-1]), element_name=name_parts[-1])

    @property
    def qualified_name(self) -> str:
        """Return the full name form. e.g. ds or listing__ds__month.

        If date_part is specified, don't include granularity in qualified_name since it will not impact the result.
        """
        items = list(self.entity_link_names) + [self.element_name]
        if self.date_part:
            items.append(self.date_part_suffix(date_part=self.date_part))
        elif self.time_granularity:
            items.append(self.time_granularity.value)
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
    def granularity_free_qualified_name(self) -> str:
        """Renders the qualified name without the granularity suffix.

        In the list metrics and list dimensions outputs we want to render the qualified name of the dimension, but
        without including the base granularity for time dimensions. This method is useful in those contexts.
        Note: in most cases you should be using the qualified_name - this is only useful in cases where the
        Dimension set has de-duplicated TimeDimensions such that you never have more than one granularity
        in your set for each TimeDimension.
        """
        return StructuredLinkableSpecName(
            entity_link_names=self.entity_link_names, element_name=self.element_name
        ).qualified_name
