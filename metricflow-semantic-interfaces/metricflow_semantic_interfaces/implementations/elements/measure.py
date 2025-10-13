from __future__ import annotations

from typing import List, Optional

from msi_pydantic_shim import Field

from metricflow_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from metricflow_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)
from metricflow_semantic_interfaces.implementations.metadata import PydanticMetadata
from metricflow_semantic_interfaces.references import MeasureReference
from metricflow_semantic_interfaces.type_enums import AggregationType


class PydanticNonAdditiveDimensionParameters(HashableBaseModel):
    """Describes the params for specifying non-additive dimensions in a measure.

    NOTE: Currently, only TimeDimensions are supported for this filter
    """

    name: str

    # Optional Fields
    window_choice: AggregationType = AggregationType.MIN
    window_groupings: List[str] = Field(default_factory=list)


class PydanticMeasureAggregationParameters(HashableBaseModel):
    """Describes parameters for aggregations."""

    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


class PydanticMeasure(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a measure."""

    name: str
    agg: AggregationType
    description: Optional[str]
    create_metric: Optional[bool]
    expr: Optional[str] = None
    agg_params: Optional[PydanticMeasureAggregationParameters]
    metadata: Optional[PydanticMetadata]
    non_additive_dimension: Optional[PydanticNonAdditiveDimensionParameters] = None
    agg_time_dimension: Optional[str] = None
    label: Optional[str] = None
    config: Optional[PydanticSemanticLayerElementConfig] = None

    @property
    def reference(self) -> MeasureReference:  # noqa: D102
        return MeasureReference(element_name=self.name)
