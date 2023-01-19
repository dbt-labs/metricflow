from __future__ import annotations
from collections import OrderedDict

import logging
import re
import typing
from dataclasses import dataclass
from typing import Optional, Tuple

from metricflow.time.time_granularity import TimeGranularity

DUNDER = "__"

logger = logging.getLogger(__name__)

GRANULARITY_FUNCTION_NAME = "granularity"


@dataclass(frozen=True)
class StructuredModifiedSpecName:
    """Spec that contains and parses data for a 'modified' or 'transformed' name.

    This spec and parsing
    can be used for dimensions and metrics, though currently it is only used for linkable specs and granularity.

    Ex: revenue.granularity(week) ->
    qualified_name = revenue
    modifiers = {'granularity': 'week'}

    user_id__country.pop(mom).trim() ->
    qualified_name = user_id__country
    modifiers = {'pop': 'mom', 'trim':''}
    """

    qualified_name: str
    modifiers: typing.OrderedDict[str, str]

    @staticmethod
    def from_name(name_with_functions: str) -> StructuredModifiedSpecName:
        """Helper function that splits a name that has 'functions' acting on it for the new syntax."""

        function_pattern = r"(.*)\((.*)\)$"  # I wrote this regex so it might be wrong - tries to match things in format function_name(parameter).

        modifier_parts = name_with_functions.split(".")
        name = modifier_parts.pop(0)
        modifiers = OrderedDict()
        for part in modifier_parts:
            function_match = re.fullmatch(function_pattern, part)
            if not function_match:
                raise RuntimeError(
                    f"Modified name '{name_with_functions}', specifically modifier '{part}',"
                    "does not follow function_name(parameter) syntax."
                )

            modifiers[function_match.group(1).lower()] = function_match.group(2).lower()

        return StructuredModifiedSpecName(qualified_name=name, modifiers=modifiers)


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
        """Construct from a name e.g. listing__ds__month. Modified to point to new version."""
        if "." in qualified_name:
            return StructuredLinkableSpecName.from_name_v2(qualified_name)
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

    @staticmethod
    def from_name_v2(modified_name: str) -> StructuredLinkableSpecName:
        """Construct from a name with newer syntax. e.g. listing__ds.granularity(month)."""

        structured_modified_spec = StructuredModifiedSpecName.from_name(modified_name)
        linked_name = structured_modified_spec.qualified_name.split(DUNDER)

        granularity_str = structured_modified_spec.modifiers.get(GRANULARITY_FUNCTION_NAME)

        return StructuredLinkableSpecName(
            identifier_link_names=tuple(linked_name[:-1]),
            element_name=linked_name[-1],
            time_granularity=TimeGranularity(granularity_str) if granularity_str else None,
        )

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
