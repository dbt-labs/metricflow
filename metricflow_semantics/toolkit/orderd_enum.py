from __future__ import annotations

import functools
from enum import Enum

from metricflow_semantics.toolkit.comparison_helpers import ComparisonOtherType


@functools.total_ordering
class OrderedEnum(Enum):
    """An `Enum` that can be sorted by definition order.

    * This is useful dataclasses that have enums as fields as the default `Enum` isn't sortable.
    * Considering using the `OrderedEnum` from `aenum`.
    """

    @classmethod
    @functools.cache
    def _enum_to_index(cls) -> dict[OrderedEnum, int]:
        return {member: i for i, member in enumerate(cls)}

    def __lt__(self, other: ComparisonOtherType) -> bool:  # noqa: D105
        if type(other) is not type(self):
            return NotImplemented
        enum_to_index = self._enum_to_index()

        return enum_to_index[self] < enum_to_index[other]
