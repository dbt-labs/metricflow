from __future__ import annotations

from enum import Enum

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType


def is_expansive(agg_type: AggregationType) -> bool:
    """Expansive ≝ Op( X ∪ Y ∪ ...) = Op( Op(X) ∪ Op(Y) ∪ ...).

    NOTE: COUNT is only expansive because it's transformed into a SUM agg during model transformation
    """
    return agg_type in (
        AggregationType.SUM,
        AggregationType.MIN,
        AggregationType.MAX,
        AggregationType.SUM_BOOLEAN,
        AggregationType.COUNT,
    )


def is_additive(agg_type: AggregationType) -> bool:
    """Indicates that if you sum values over a dimension grouping, you will still get an accurate result for this metric."""
    if agg_type is AggregationType.SUM or agg_type is AggregationType.SUM_BOOLEAN or agg_type is AggregationType.COUNT:
        return True
    elif (
        agg_type is AggregationType.MIN
        or agg_type is AggregationType.MAX
        or agg_type is AggregationType.COUNT_DISTINCT
        or agg_type is AggregationType.AVERAGE
        or agg_type is AggregationType.PERCENTILE
        or agg_type is AggregationType.MEDIAN
    ):
        return False
    else:
        assert_values_exhausted(agg_type)


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
    COMPLETE = "COMPLETE"

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}.{self.name}"
