from __future__ import annotations

from typing import FrozenSet, Iterable, Optional

from typing_extensions import Self, override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty


@fast_frozen_dataclass()
class GroupByItemSetFilter(Mergeable):
    """Describes a way to filter the items in a `BaseGroupByItemSet`."""

    # A `None` value for element names means no filtering on element names.
    element_names: Optional[FrozenSet[str]] = None
    with_any_of: FrozenSet[GroupByItemProperty] = GroupByItemProperty.all_properties()
    without_any_of: FrozenSet[GroupByItemProperty] = frozenset()

    def copy(
        self,
        element_names: Optional[FrozenSet[str]] = None,
        with_any_of: Optional[FrozenSet[GroupByItemProperty]] = None,
        without_any_of: Optional[FrozenSet[GroupByItemProperty]] = None,
    ) -> GroupByItemSetFilter:
        """Create a copy of this with the given non-None fields replaced."""
        return GroupByItemSetFilter(
            element_names=element_names if element_names is not None else self.element_names,
            with_any_of=with_any_of if with_any_of is not None else self.with_any_of,
            without_any_of=without_any_of if without_any_of is not None else self.without_any_of,
        )

    @override
    def merge(self: Self, other: GroupByItemSetFilter) -> GroupByItemSetFilter:
        if self.element_names is None and other.element_names is None:
            element_names = None
        else:
            element_names = (self.element_names or frozenset()).union(other.element_names or frozenset())
        return GroupByItemSetFilter(
            element_names=element_names,
            with_any_of=self.with_any_of.union(other.with_any_of),
            without_any_of=self.without_any_of.union(other.without_any_of),
        )

    @classmethod
    @override
    def empty_instance(cls) -> GroupByItemSetFilter:
        return GroupByItemSetFilter()

    def without_element_names(self) -> GroupByItemSetFilter:
        """Return this filter without the `element_names` filter set."""
        return GroupByItemSetFilter(
            with_any_of=self.with_any_of,
            without_any_of=self.without_any_of,
        )

    def allow(self, element_name: Optional[str], element_properties: Optional[Iterable[GroupByItemProperty]]) -> bool:
        """Return true if this allows an item with the given name and properties.

        `None` can be specified in cases of incomplete context.
        """
        if element_name is not None:
            allowed_element_name_set = self.element_names
            if allowed_element_name_set is not None and element_name not in allowed_element_name_set:
                return False

        if element_properties is not None:
            if len(self.with_any_of.intersection(element_properties)) == 0:
                return False
            denied_property_set = self.without_any_of
            if denied_property_set and len(denied_property_set.intersection(element_properties)) > 0:
                return False

        return True
