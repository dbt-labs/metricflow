from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from metricflow.time.time_granularity import TimeGranularity

DUNDER = "__"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StructuredLinkableSpecName:
    """Parse a qualified name into different parts.

    e.g. listing__ds__week ->
    identifier_links: ["listing"]
    element_name: "ds"
    granularity: TimeGranularity.WEEK
    """

    identifier_link_names: Tuple[str, ...]
    element_name: str
    time_granularity: Optional[TimeGranularity] = None

    @staticmethod
    def from_name(qualified_name: str) -> StructuredLinkableSpecName:
        """Construct from a name e.g. listing__ds__month."""
        name_parts = qualified_name.split(DUNDER)

        # No dunder, e.g. "ds"
        if len(name_parts) == 1:
            return StructuredLinkableSpecName((), name_parts[0])

        associated_granularity = None
        granularity: TimeGranularity
        for granularity in TimeGranularity:
            if name_parts[-1] == granularity.value:
                associated_granularity = granularity

        # Has a time granularity
        if associated_granularity:
            #  e.g. "ds__month"
            if len(name_parts) == 2:
                return StructuredLinkableSpecName((), name_parts[0], associated_granularity)
            # e.g. "messages__ds__month"
            return StructuredLinkableSpecName(tuple(name_parts[:-2]), name_parts[-2], associated_granularity)
        # e.g. "messages__ds"
        else:
            return StructuredLinkableSpecName(tuple(name_parts[:-1]), name_parts[-1])

    @property
    def qualified_name(self) -> str:
        """Return the full name form. e.g. ds or listing__ds__month"""
        items = list(self.identifier_link_names) + [self.element_name]
        if self.time_granularity and self.time_granularity != TimeGranularity.DAY:
            items.append(self.time_granularity.value)
        return DUNDER.join(items)

    @property
    def qualified_name_without_granularity(self) -> str:
        """Return the name without the time granularity. e.g. listing__ds__month -> listing__ds"""
        return DUNDER.join(list(self.identifier_link_names) + [self.element_name])

    @property
    def qualified_name_without_identifier(self) -> str:
        """Return the name without the identifier. e.g. listing__ds__month -> ds__month"""
        return DUNDER.join([self.element_name] + ([self.time_granularity.value] if self.time_granularity else []))

    @property
    def identifier_prefix(self) -> Optional[str]:
        """Return the identifier prefix. e.g. listing__ds__month -> listing"""
        if len(self.identifier_link_names) > 0:
            return DUNDER.join(self.identifier_link_names)

        return None
