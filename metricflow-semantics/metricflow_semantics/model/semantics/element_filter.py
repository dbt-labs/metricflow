from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet, Optional

from typing_extensions import Self, override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty


@dataclass(frozen=True)
class LinkableElementFilter(Mergeable):
    """Describes a way to filter the `LinkableElements` in a `LinkableElementSet`."""

    # A `None` value for element names means no filtering on element names.
    element_names: Optional[FrozenSet[str]] = None
    with_any_of: FrozenSet[LinkableElementProperty] = LinkableElementProperty.all_properties()
    without_any_of: FrozenSet[LinkableElementProperty] = frozenset()
    without_all_of: FrozenSet[LinkableElementProperty] = frozenset()

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
