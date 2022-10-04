from __future__ import annotations

from enum import Enum

from metricflow.object_utils import ExtendedEnum


class AggregationType(ExtendedEnum):
    """Aggregation methods for measures"""

    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"
    # BOOLEAN is deprecated. Remove when customers have migrated.
    BOOLEAN = "boolean"
    SUM_BOOLEAN = "sum_boolean"
    AVERAGE = "average"

    COUNT = "count"
    # AVERAGE = "average"  not yet implemented ...requires us to keep track of two quantities, count and sum

    @property
    def is_expansive(self) -> bool:
        """Expansive ≝ Op( X ∪ Y ∪ ...) = Op( Op(X) ∪ Op(Y) ∪ ...)

        NOTE: COUNT is only expansive because it's transformed into a SUM agg during model transformation
        """
        return self in (
            AggregationType.SUM,
            AggregationType.MIN,
            AggregationType.MAX,
            AggregationType.BOOLEAN,
            AggregationType.COUNT,
        )

    @property
    def fill_nulls_with_0(self) -> bool:
        """Indicates if charts should show 0 instead of null where there are gaps in data."""
        return self in (
            AggregationType.SUM,
            AggregationType.COUNT_DISTINCT,
            AggregationType.SUM_BOOLEAN,
            AggregationType.COUNT,
        )

    @property
    def can_limit_dimension_values(self) -> bool:
        """Indicates if we can limit dimension values in charts.

        Currently, this means:
        1. The dimensions we care about most are the ones with the highest numeric values
        2. We can calculate the "other" column in the postprocessor (meaning the metric is expansive)
        """
        return self in (AggregationType.SUM, AggregationType.SUM_BOOLEAN, AggregationType.COUNT)


class AggregationState(Enum):
    """Represents how the measure is aggregated."""

    # When reading from the source, the measure is considered non-aggregated.
    NON_AGGREGATED = "NON_AGGREGATED"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}.{self.name}"
