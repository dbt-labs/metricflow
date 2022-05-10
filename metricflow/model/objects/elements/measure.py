from __future__ import annotations

from typing import Optional

from metricflow.model.objects.utils import ParseableObject, HashableBaseModel
from metricflow.object_utils import ExtendedEnum
from metricflow.specs import MeasureReference, TimeDimensionReference


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

    # COUNT = "count" not yet implemented ... non-expansive as COUNT(COUNT(X), COUNT(Y)) != COUNT(COUNT(X ∪ Y))
    # AVERAGE = "average"  not yet implemented ...requires us to keep track of two quantities, count and sum

    @property
    def is_expansive(self) -> bool:
        """Expansive ≝ Op( X ∪ Y ∪ ...) = Op( Op(X) ∪ Op(Y) ∪ ...)"""
        return self in (AggregationType.SUM, AggregationType.MIN, AggregationType.MAX, AggregationType.BOOLEAN)

    @property
    def fill_nulls_with_0(self) -> bool:
        """Indicates if charts should show 0 instead of null where there are gaps in data."""
        return self in (AggregationType.SUM, AggregationType.COUNT_DISTINCT, AggregationType.SUM_BOOLEAN)

    @property
    def can_limit_dimension_values(self) -> bool:
        """Indicates if we can limit dimension values in charts.

        Currently, this means:
        1. The dimensions we care about most are the ones with the highest numeric values
        2. We can calculate the "other" column in the postprocessor (meaning the metric is expansive)
        """
        return self in (AggregationType.SUM, AggregationType.SUM_BOOLEAN)


class Measure(HashableBaseModel, ParseableObject):
    """Describes a measure"""

    agg: AggregationType
    create_metric: Optional[bool]
    name: MeasureReference
    expr: Optional[str] = None

    # Defines the time dimension to aggregate this measure by. If not specified, it means to use the primary time
    # dimension in the data source.
    agg_time_dimension: Optional[TimeDimensionReference] = None

    @property
    def checked_agg_time_dimension(self) -> TimeDimensionReference:
        """Returns the aggregation time dimension, throwing an exception if it's not set."""
        assert (
            self.agg_time_dimension
        ), f"Aggregation time dimension for {self.name} should have been set during model transformation"
        return self.agg_time_dimension
