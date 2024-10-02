from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet

from typing_extensions import Self, override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty


@dataclass(frozen=True)
class LinkableElementFilter(Mergeable):
    """Describes a way to filter the `LinkableElements` in a `LinkableElementSet`."""

    with_any_of: FrozenSet[LinkableElementProperty] = LinkableElementProperty.all_properties()
    without_any_of: FrozenSet[LinkableElementProperty] = frozenset()
    without_all_of: FrozenSet[LinkableElementProperty] = frozenset()

    @override
    def merge(self: Self, other: LinkableElementFilter) -> LinkableElementFilter:
        return LinkableElementFilter(
            with_any_of=self.with_any_of.union(other.with_any_of),
            without_any_of=self.without_any_of.union(other.without_any_of),
            without_all_of=self.without_all_of.union(other.without_all_of),
        )

    @classmethod
    @override
    def empty_instance(cls) -> LinkableElementFilter:
        return LinkableElementFilter()
