from __future__ import annotations

from enum import Enum


class AggregationState(Enum):
    """Represents how the measure is aggregated."""

    # When reading from the source, the measure is considered non-aggregated.
    NON_AGGREGATED = "NON_AGGREGATED"
    PARTIAL = "PARTIAL"
    # Aggregated to the grain of the group-by-items
    COMPLETE = "COMPLETE"

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}.{self.name}"
