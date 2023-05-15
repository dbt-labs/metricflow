from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.objects.base import HashableBaseModel, ModelWithMetadataParsing
from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.protocols.dimension import _DimensionMixin

ISO8601_FMT = "YYYY-MM-DD"


class DimensionValidityParams(HashableBaseModel):
    """Parameters identifying a given dimension as an entity for validity state

    This construct is used for supporting SCD Type II tables, such as might be
    created via dbt's snapshot feature, or generated via periodic loads from external
    dimension data sources. In either of those cases, there is typically a time dimension
    associated with the SCD data source that indicates the start and end times of a
    validity window, where the dimension value is valid for any time within that range.
    """

    is_start: bool = False
    is_end: bool = False


class DimensionTypeParams(HashableBaseModel):
    """Dimension type params add additional context to some types (time) of dimensions"""

    is_primary: bool = False
    time_granularity: TimeGranularity
    validity_params: Optional[DimensionValidityParams] = None


class Dimension(_DimensionMixin, HashableBaseModel, ModelWithMetadataParsing):
    """Describes a dimension"""

    name: str
    description: Optional[str]
    type: DimensionType
    is_partition: bool = False
    type_params: Optional[DimensionTypeParams]
    expr: Optional[str] = None
    metadata: Optional[Metadata]
