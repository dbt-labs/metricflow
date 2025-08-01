from __future__ import annotations

from typing import FrozenSet, Iterable, Optional

from typing_extensions import Self, override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty


@fast_frozen_dataclass()
class LinkableElementFilter(Mergeable):
    """Describes a way to filter the `LinkableElements` in a `LinkableElementSet`."""

    # A `None` value for element names means no filtering on element names.
    element_names: Optional[FrozenSet[str]] = None
    with_any_of: FrozenSet[LinkableElementProperty] = LinkableElementProperty.all_properties()
    without_any_of: FrozenSet[LinkableElementProperty] = frozenset()
    without_all_of: FrozenSet[LinkableElementProperty] = frozenset()

    def copy(
        self,
        element_names: Optional[FrozenSet[str]] = None,
        with_any_of: Optional[FrozenSet[LinkableElementProperty]] = None,
        without_any_of: Optional[FrozenSet[LinkableElementProperty]] = None,
        without_all_of: Optional[FrozenSet[LinkableElementProperty]] = None,
    ) -> LinkableElementFilter:
        """Create a copy of this with the given non-None fields replaced."""
        return LinkableElementFilter(
            element_names=element_names if element_names is not None else self.element_names,
            with_any_of=with_any_of if with_any_of is not None else self.with_any_of,
            without_any_of=without_any_of if without_any_of is not None else self.without_any_of,
            without_all_of=without_all_of if without_all_of is not None else self.without_all_of,
        )

    @override
    def merge(self: Self, other: LinkableElementFilter) -> LinkableElementFilter:
        if self.element_names is None and other.element_names is None:
            element_names = None
        else:
            element_names = (self.element_names or frozenset()).union(other.element_names or frozenset())
        return LinkableElementFilter(
            element_names=element_names,
            with_any_of=self.with_any_of.union(other.with_any_of),
            without_any_of=self.without_any_of.union(other.without_any_of),
            without_all_of=self.without_all_of.union(other.without_all_of),
        )

    @classmethod
    @override
    def empty_instance(cls) -> LinkableElementFilter:
        return LinkableElementFilter()

    def without_element_names(self) -> LinkableElementFilter:
        """Return this filter without the `element_names` filter set."""
        return LinkableElementFilter(
            with_any_of=self.with_any_of,
            without_any_of=self.without_any_of,
            without_all_of=self.without_all_of,
        )

    def allow(
        self, element_name: Optional[str], element_properties: Optional[Iterable[LinkableElementProperty]]
    ) -> bool:
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
            denied_full_match_property_set = self.without_all_of
            if denied_full_match_property_set:
                denied_full_match_property_set_length = len(denied_full_match_property_set)
                if denied_full_match_property_set_length > 0 and denied_full_match_property_set_length == len(
                    denied_full_match_property_set.intersection(element_properties)
                ):
                    return False

        return True
