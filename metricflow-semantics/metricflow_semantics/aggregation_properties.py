from __future__ import annotations

from enum import Enum

from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType


def fill_nulls_with_0(agg_type: AggregationType) -> bool:
    """Indicates if charts should show 0 instead of null where there are gaps in data."""
    return agg_type in (
        AggregationType.SUM,
        AggregationType.COUNT_DISTINCT,
        AggregationType.SUM_BOOLEAN,
        AggregationType.COUNT,
    )


def can_limit_dimension_values(agg_type: AggregationType) -> bool:
    """Indicates if we can limit dimension values in charts.

    Currently, this means:
    1. The dimensions we care about most are the ones with the highest numeric values
    2. We can calculate the "other" column in the postprocessor (meaning the metric is expansive)
    """
    return agg_type in (AggregationType.SUM, AggregationType.SUM_BOOLEAN, AggregationType.COUNT)


class AggregationState(Enum):
    """Represents how the measure is aggregated."""

    # When reading from the source, the measure is considered non-aggregated.
    NON_AGGREGATED = "NON_AGGREGATED"
    PARTIAL = "PARTIAL"
    # Aggregated to the grain of the group-by-items
    COMPLETE = "COMPLETE"

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}.{self.name}"
