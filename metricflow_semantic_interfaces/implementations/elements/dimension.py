from __future__ import annotations

from typing import Optional

from metricflow_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from metricflow_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)
from metricflow_semantic_interfaces.implementations.metadata import PydanticMetadata
from metricflow_semantic_interfaces.references import (
    DimensionReference,
    TimeDimensionReference,
)
from metricflow_semantic_interfaces.type_enums import DimensionType, TimeGranularity

ISO8601_FMT = "YYYY-MM-DD"


class PydanticDimensionValidityParams(HashableBaseModel):
    """Parameters identifying a given dimension as an entity for validity state.

    This construct is used for supporting SCD Type II tables, such as might be
    created via dbt's snapshot feature, or generated via periodic loads from external
    dimension semantic models. In either of those cases, there is typically a time dimension
    associated with the SCD semantic model that indicates the start and end times of a
    validity window, where the dimension value is valid for any time within that range.
    """

    is_start: bool = False
    is_end: bool = False


class PydanticDimensionTypeParams(HashableBaseModel):
    """PydanticDimension type params add additional context to some types (time) of dimensions."""

    time_granularity: TimeGranularity
    validity_params: Optional[PydanticDimensionValidityParams] = None


class PydanticDimension(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a dimension."""

    name: str
    description: Optional[str]
    type: DimensionType
    is_partition: bool = False
    type_params: Optional[PydanticDimensionTypeParams]
    expr: Optional[str] = None
    metadata: Optional[PydanticMetadata]
    label: Optional[str] = None
    config: Optional[PydanticSemanticLayerElementConfig]

    @property
    def reference(self) -> DimensionReference:  # noqa: D102
        return DimensionReference(element_name=self.name)

    @property
    def time_dimension_reference(self) -> Optional[TimeDimensionReference]:  # noqa: D102
        return TimeDimensionReference(element_name=self.name) if self.type is DimensionType.TIME else None

    @property
    def validity_params(self) -> Optional[PydanticDimensionValidityParams]:
        """Returns the PydanticDimensionValidityParams property, if it exists.

        This is to avoid repeatedly checking that type params is not None before doing anything with ValidityParams
        """
        if self.type_params:
            return self.type_params.validity_params

        return None
