from __future__ import annotations

from typing import Optional, Protocol
from abc import abstractmethod

from dbt_semantic_interfaces.protocols.metadata import Metadata
from dbt_semantic_interfaces.references import DimensionReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


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

    @property
    @abstractmethod
    def is_primary_time(self) -> bool:  # noqa: D
        ...

    @property
    @abstractmethod
    def reference(self) -> DimensionReference:  # noqa: D
        ...

    @property
    @abstractmethod
    def time_dimension_reference(self) -> Optional[TimeDimensionReference]:  # noqa: D
        ...

    @property
    @abstractmethod
    def validity_params(self) -> Optional[DimensionValidityParams]:  # noqa: D
        ...


class _DimensionMixin:
    """Some useful default implementation details of DimensionProtocol"""

    name: str
    type: DimensionType
    type_params: Optional[DimensionTypeParams]
    expr: Optional[str] = None

    @property
    def is_primary_time(self) -> bool:  # noqa: D
        if self.type == DimensionType.TIME and self.type_params is not None:
            return self.type_params.is_primary

        return False

    @property
    def reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=self.name)

    @property
    def time_dimension_reference(self) -> Optional[TimeDimensionReference]:  # noqa: D
        if self.type == DimensionType.TIME:
            return TimeDimensionReference(element_name=self.name)
        return None

    @property
    def validity_params(self) -> Optional[DimensionValidityParams]:  # noqa: D
        """Returns the DimensionValidityParams property, if it exists.

        This is to avoid repeatedly checking that type params is not None before doing anything with ValidityParams
        """
        if self.type_params:
            return self.type_params.validity_params

        return None
