from __future__ import annotations

from typing import Optional, List
from metricflow.aggregation_properties import AggregationType
from metricflow.model.objects.common import Metadata
from metricflow.model.objects.base import ModelWithMetadataParsing, HashableBaseModel
from metricflow.object_utils import hash_strings
from metricflow.references import MeasureReference, TimeDimensionReference


class NonAdditiveDimensionParameters(HashableBaseModel):
    """Describes the params for specifying non-additive dimensions in a measure.

    NOTE: Currently, only TimeDimensions are supported for this filter
    """

    name: str
    window_choice: AggregationType
    window_groupings: List[str] = []

    @property
    def bucket_hash(self) -> str:
        """Returns the hash value used for grouping equivalent params."""
        values = [self.window_choice.name, self.name]
        values.extend(sorted(self.window_groupings))
        return hash_strings(values)


class Measure(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a measure"""

    name: str
    agg: AggregationType
    description: Optional[str]
    create_metric: Optional[bool]
    expr: Optional[str] = None
    metadata: Optional[Metadata]
    non_additive_dimension: Optional[NonAdditiveDimensionParameters] = None

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
