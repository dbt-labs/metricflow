from __future__ import annotations

from typing import FrozenSet, Iterable, Optional

from typing_extensions import Self, override

from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.merger import Mergeable


@fast_frozen_dataclass()
class GroupByItemSetFilter(Mergeable):
    """Describes a way to filter the items in a `BaseGroupByItemSet`."""

    # A `None` value for element names means no filtering on element names.
    element_name_allowlist: Optional[FrozenSet[str]] = None
    any_properties_allowlist: FrozenSet[GroupByItemProperty] = GroupByItemProperty.all_properties()
    any_properties_denylist: FrozenSet[GroupByItemProperty] = frozenset()

    @staticmethod
    def create(  # noqa: D102
        element_name_allowlist: Optional[Iterable[str]] = None,
        any_properties_allowlist: Iterable[GroupByItemProperty] = GroupByItemProperty.all_properties(),
        any_properties_denylist: Iterable[GroupByItemProperty] = frozenset(),
    ) -> GroupByItemSetFilter:
        return GroupByItemSetFilter(
            element_name_allowlist=frozenset(element_name_allowlist) if element_name_allowlist is not None else None,
            any_properties_allowlist=frozenset(any_properties_allowlist),
            any_properties_denylist=frozenset(any_properties_denylist),
        )

    def copy(
        self,
        element_name_allowlist: Optional[FrozenSet[str]] = None,
        any_properties_allowlist: Optional[FrozenSet[GroupByItemProperty]] = None,
        any_properties_denylist: Optional[FrozenSet[GroupByItemProperty]] = None,
    ) -> GroupByItemSetFilter:
        """Create a copy of this with the given non-None fields replaced."""
        return GroupByItemSetFilter(
            element_name_allowlist=element_name_allowlist
            if element_name_allowlist is not None
            else self.element_name_allowlist,
            any_properties_allowlist=any_properties_allowlist
            if any_properties_allowlist is not None
            else self.any_properties_allowlist,
            any_properties_denylist=any_properties_denylist
            if any_properties_denylist is not None
            else self.any_properties_denylist,
        )

    @override
    def merge(self: Self, other: GroupByItemSetFilter) -> GroupByItemSetFilter:
        if self.element_name_allowlist is None and other.element_name_allowlist is None:
            element_names = None
        else:
            element_names = (self.element_name_allowlist or frozenset()).union(
                other.element_name_allowlist or frozenset()
            )
        return GroupByItemSetFilter(
            element_name_allowlist=element_names,
            any_properties_allowlist=self.any_properties_allowlist.union(other.any_properties_allowlist),
            any_properties_denylist=self.any_properties_denylist.union(other.any_properties_denylist),
        )

    @classmethod
    @override
    def empty_instance(cls) -> GroupByItemSetFilter:
        return GroupByItemSetFilter.create()

    def without_element_name_allowlist(self) -> GroupByItemSetFilter:
        """Return this filter without the `element_name_allowlist` filter set."""
        return GroupByItemSetFilter.create(
            element_name_allowlist=None,
            any_properties_denylist=self.any_properties_denylist,
            any_properties_allowlist=self.any_properties_allowlist,
        )

    def allow(self, element_name: Optional[str], element_properties: Optional[Iterable[GroupByItemProperty]]) -> bool:
        """Return true if this allows an item with the given name and properties.

        `None` can be specified in cases of incomplete context.
        """
        if element_name is not None:
            allowed_element_name_set = self.element_name_allowlist
            if allowed_element_name_set is not None and element_name not in allowed_element_name_set:
                return False

        if element_properties is not None:
            if len(self.any_properties_allowlist.intersection(element_properties)) == 0:
                return False
            denied_property_set = self.any_properties_denylist
            if denied_property_set and len(denied_property_set.intersection(element_properties)) > 0:
                return False

        return True
