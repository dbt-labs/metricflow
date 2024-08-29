from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.time.granularity import ExpandedTimeGranularity

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
    time_granularity_name: Optional[str] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:
        """Post init method to ensure all name values are lower-case.

        This implies that all values in the TimeGranularity enumeration must be lower-case.
        """
        assert self.element_name.lower() == self.element_name, (
            f"Element name {self.element_name} is not all lower-case. This should have been addressed at model "
            "parse time!"
        )
        assert all([link_name.lower() == link_name for link_name in self.entity_link_names]), (
            f"Found entity link name that includes upper-case letters. Entity link names: {self.entity_link_names}. "
            "This should have been addressed at model parse time!"
        )

        if self.time_granularity_name:
            is_standard_name = ExpandedTimeGranularity.is_standard_granularity_name(self.time_granularity_name)
            if is_standard_name:
                follow_up = (
                    "This indicates a bug where someone added a non-lowercase value to the TimeGranularity "
                    "enum in dbt-semantic-interfaces!"
                )
            else:
                follow_up = "This should have been addressed at model parse time!"
            assert (
                self.time_granularity_name.lower() == self.time_granularity_name
            ), f"The time_granularity_name {self.time_granularity_name} includes upper-case letters. {follow_up}"

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
        # TODO: [custom granularity] Update parsing to account for custom granularities
        for granularity in TimeGranularity:
            if name_parts[-1] == granularity.value:
                associated_granularity = granularity

        # Has a time granularity
        if associated_granularity:
            #  e.g. "ds__month"
            if len(name_parts) == 2:
                return StructuredLinkableSpecName(
                    entity_link_names=(), element_name=name_parts[0], time_granularity_name=associated_granularity.value
                )
            # e.g. "messages__ds__month"
            return StructuredLinkableSpecName(
                entity_link_names=tuple(name_parts[:-2]),
                element_name=name_parts[-2],
                time_granularity_name=associated_granularity.value,
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

    @property
    def is_element_name(self) -> bool:
        """Indicates whether or not this is an unadorned element name, i.e., one without links or time annotations."""
        return self.qualified_name == self.element_name
