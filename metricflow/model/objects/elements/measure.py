from __future__ import annotations

from typing import Optional, List
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.objects.base import ModelWithMetadataParsing, HashableBaseModel
from dbt_semantic_interfaces.references import MeasureReference, TimeDimensionReference
from dbt_semantic_interfaces.protocols.measure import (
    Measure as MeasureProtocol,
    NonAdditiveDimensionParameters as NonAdditiveDimensionParametersProtocol,
    MeasureAggregationParameters as MeasureAggregationParametersProtocol,
)

class NonAdditiveDimensionParameters(NonAdditiveDimensionParametersProtocol, HashableBaseModel):
    """Describes the params for specifying non-additive dimensions in a measure.

    NOTE: Currently, only TimeDimensions are supported for this filter
    """

    name: str

    # Optional Fields
    window_choice: AggregationType = AggregationType.MIN
    window_groupings: List[str] = []


class MeasureAggregationParameters(MeasureAggregationParametersProtocol, HashableBaseModel):
    """Describes parameters for aggregations"""

    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


class Measure(HashableBaseModel, ModelWithMetadataParsing, MeasureProtocol):
    """Describes a measure"""

    name: str
    agg: AggregationType
    description: Optional[str] = None
    create_metric: Optional[bool] = None
    expr: Optional[str] = None
    agg_params: Optional[MeasureAggregationParameters] = None
    non_additive_dimension: Optional[NonAdditiveDimensionParameters] = None
    agg_time_dimension: Optional[str] = None
    metadata: Optional[Metadata]

    # Defines the time dimension to aggregate this measure by. If not specified, it means to use the primary time
    # dimension in the data source.
    agg_time_dimension: Optional[str] = None

    @property
    def checked_agg_time_dimension(self) -> TimeDimensionReference:
        """Returns the aggregation time dimension, throwing an exception if it's not set."""
        assert self.agg_time_dimension, (
            f"Aggregation time dimension for measure {self.name} is not set! This should either be set directly on "
            f"the measure specification in the model, or else defaulted to the primary time dimension in the data "
            f"source containing the measure."
        )
        return TimeDimensionReference(element_name=self.agg_time_dimension)

    @property
    def reference(self) -> MeasureReference:  # noqa: D
        return MeasureReference(element_name=self.name)