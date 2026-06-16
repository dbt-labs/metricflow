from __future__ import annotations

import collections
from typing import Dict, Generic, List, TypeVar

ElementX = TypeVar("ElementX")
ElementY = TypeVar("ElementY")


class ElementGrouper(Generic[ElementX, ElementY]):
    """Groups an Element pair such that we have (ElementX, List[ElementY])."""

    def __init__(self) -> None:  # noqa: D107
        self._groups: Dict[ElementX, List[ElementY]] = collections.defaultdict(list)

    def add_value(self, key: ElementX, value: ElementY) -> None:  # noqa: D102
        self._groups[key].append(value)

    def get_values(self, key: ElementX) -> List[ElementY]:  # noqa: D102
        if key not in self._groups:
            raise KeyError(f"Unable to find `{key}` in ElementGrouper")
        return self._groups[key]

    @property
    def keys(self) -> List[ElementX]:  # noqa :D
        return list(self._groups.keys())
