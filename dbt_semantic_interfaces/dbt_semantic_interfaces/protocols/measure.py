from __future__ import annotations

from typing import List, Optional, Protocol
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType


class NonAdditiveDimensionParametersProtocol(Protocol):
    """Describes the params for specifying non-additive dimensions in a measure."""
    name: str
    window_choice: AggregationType
    window_groupings: List[str]


class MeasureAggregationParametersProtocol(Protocol):
    """Describes parameters for aggregations""" 
    percentile: Optional[float]
    use_discrete_percentile: bool
    use_approximate_percentile: bool


class MeasureProtocol(Protocol):
    """Describes a measure, which is a field in the underlying semantic model that can be 
    aggregated in a specific way."""
    name: str
    agg: AggregationType
    description: Optional[str]
    create_metric: Optional[bool]
    expr: Optional[str]
    agg_params: Optional[MeasureAggregationParametersProtocol]
    non_additive_dimension: Optional[NonAdditiveDimensionParametersProtocol]
    agg_time_dimension: Optional[str]
    # checked_agg_time_dimension: TimeDimensionReference
    # reference: MeasureReference