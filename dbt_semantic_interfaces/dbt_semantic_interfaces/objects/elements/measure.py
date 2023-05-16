from __future__ import annotations

from typing import List, Optional

from dbt_semantic_interfaces.objects.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.protocols.measure import _MeasureMixin
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType


class NonAdditiveDimensionParameters(HashableBaseModel):
    """Describes the params for specifying non-additive dimensions in a measure.

    NOTE: Currently, only TimeDimensions are supported for this filter
    """

    name: str

    # Optional Fields
    window_choice: AggregationType = AggregationType.MIN
    window_groupings: List[str] = []


class MeasureAggregationParameters(HashableBaseModel):
    """Describes parameters for aggregations."""

    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


class Measure(_MeasureMixin, HashableBaseModel, ModelWithMetadataParsing):
    """Describes a measure."""

    name: str
    agg: AggregationType
    description: Optional[str]
    create_metric: Optional[bool]
    expr: Optional[str] = None
    agg_params: Optional[MeasureAggregationParameters]
    metadata: Optional[Metadata]
    non_additive_dimension: Optional[NonAdditiveDimensionParameters] = None
    agg_time_dimension: Optional[str] = None
