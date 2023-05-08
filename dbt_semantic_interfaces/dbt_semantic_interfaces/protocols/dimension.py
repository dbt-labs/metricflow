from __future__ import annotations

from enum import Enum
from typing import Optional, Protocol

from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity


class DimensionType(Enum):
    """Determines types of values expected of dimensions."""

    CATEGORICAL = "categorical"
    TIME = "time"


class DimensionValidityParams(Protocol):
    """Parameters identifying a given dimension as an entity for validity state

    This construct is used for supporting SCD Type II tables, such as might be
    created via dbt's snapshot feature, or generated via periodic loads from external
    dimension data sources. In either of those cases, there is typically a time dimension
    associated with the SCD data source that indicates the start and end times of a
    validity window, where the dimension value is valid for any time within that range.
    """

    is_start: bool
    is_end: bool


class DimensionTypeParams(Protocol):
    """Dimension type params add context to some types of dimensions (like time)"""

    is_primary: bool
    time_granularity: TimeGranularity
    validity_params: Optional[DimensionValidityParams]


class Dimension(Protocol):
    """Describes a dimension"""

    name: str
    description: Optional[str]
    type: DimensionType
    is_partition: bool
    type_params: Optional[DimensionTypeParams]
    expr: Optional[str]
    metadata: Optional[Metadata]
