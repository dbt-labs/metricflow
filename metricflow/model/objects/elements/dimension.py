from __future__ import annotations

from typing import Optional

from metricflow.model.objects.common import Element
from metricflow.model.objects.utils import ParseableObject, HashableBaseModel
from metricflow.specs import DimensionReference
from metricflow.time.time_granularity import TimeGranularity
from metricflow.object_utils import ExtendedEnum

ISO8601_FMT = "YYYY-MM-DD"


class DimensionType(ExtendedEnum):
    """Determines types of values expected of dimensions."""

    CATEGORICAL = "categorical"
    TIME = "time"

    def is_time_type(self) -> bool:
        """Checks if this type of dimension is a time type"""
        return self in [DimensionType.TIME]


class DimensionTypeParams(HashableBaseModel, ParseableObject):
    """Dimension type params add additional context to some types (time) of dimensions"""

    is_primary: bool = False
    # For legacy support. This is not used.
    time_format: str = ISO8601_FMT
    time_granularity: TimeGranularity


class Dimension(HashableBaseModel, Element, ParseableObject):
    """Describes a dimension"""

    name: DimensionReference
    type: DimensionType
    is_partition: bool = False
    type_params: Optional[DimensionTypeParams]
    expr: Optional[str] = None

    @property
    def is_primary_time(self) -> bool:  # noqa: D
        if self.type == DimensionType.TIME and self.type_params is not None:
            return self.type_params.is_primary

        return False
