from __future__ import annotations

import functools
from enum import Enum
from functools import cached_property
from typing import Mapping

from typing_extensions import Self

from metricflow_semantics.experimental.comparison_helpers import ComparisonOtherType


@functools.total_ordering
class OrderedEnum(Enum):
    """An `Enum` that provides ordering by the definition order.

    This is useful for sorting dataclasses that have enums as fields. The implementation can be improved / replaced with
    something more standard.
    """

    @cached_property
    def _enum_to_index(self) -> Mapping[Self, int]:
        return {enum: i for i, enum in enumerate(self.__class__)}

    def __lt__(self, other: ComparisonOtherType) -> bool:  # noqa: D105
        if not isinstance(other, OrderedEnum):
            return NotImplemented

        return self._enum_to_index[self] < self._enum_to_index[other]
