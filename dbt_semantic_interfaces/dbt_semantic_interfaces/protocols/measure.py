from __future__ import annotations

from abc import abstractmethod
from typing import List, Optional, Protocol

from dbt_semantic_interfaces.references import MeasureReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType


class NonAdditiveDimensionParameters(Protocol):
    """Describes the params for specifying non-additive dimensions in a measure."""

    name: str
    window_choice: AggregationType
    window_groupings: List[str]


class MeasureAggregationParameters(Protocol):
    """Describes parameters for aggregations."""

    percentile: Optional[float]
    use_discrete_percentile: bool
    use_approximate_percentile: bool


class Measure(Protocol):
    """Describes a measure.

    Measure is a field in the underlying semantic model that can be aggregated
    in a specific way.
    """

    name: str
    agg: AggregationType
    description: Optional[str]
    create_metric: Optional[bool]
    expr: Optional[str]
    agg_params: Optional[MeasureAggregationParameters]
    non_additive_dimension: Optional[NonAdditiveDimensionParameters]
    agg_time_dimension: Optional[str]

    @property
    @abstractmethod
    def checked_agg_time_dimension(self) -> TimeDimensionReference:
        """Returns the aggregation time dimension, throwing an exception if it's not set."""
        ...

    @property
    @abstractmethod
    def reference(self) -> MeasureReference:
        """Returns a reference to this measure."""
        ...


class _MeasureMixin:
    """Some useful default implementation details of MeasureProtocol."""

    name: str
    expr: Optional[str] = None
    non_additive_dimension: Optional[NonAdditiveDimensionParameters] = None
    agg_time_dimension: Optional[str] = None

    @property
    def checked_agg_time_dimension(self) -> TimeDimensionReference:
        assert self.agg_time_dimension, (
            f"Aggregation time dimension for measure {self.name} is not set! This should either be set directly on "
            f"the measure specification in the model, or else defaulted to the primary time dimension in the data "
            f"source containing the measure."
        )
        return TimeDimensionReference(element_name=self.agg_time_dimension)

    @property
    def reference(self) -> MeasureReference:  # noqa: D
        return MeasureReference(element_name=self.name)
