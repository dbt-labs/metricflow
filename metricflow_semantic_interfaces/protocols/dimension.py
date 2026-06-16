from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol

from metricflow_semantic_interfaces.protocols.meta import SemanticLayerElementConfig
from metricflow_semantic_interfaces.protocols.metadata import Metadata
from metricflow_semantic_interfaces.references import (
    DimensionReference,
    TimeDimensionReference,
)
from metricflow_semantic_interfaces.type_enums import DimensionType, TimeGranularity


class DimensionValidityParams(Protocol):
    """Parameters identifying a given dimension as an entity for validity state.

    This construct is used for supporting SCD Type II tables, such as might be
    created via dbt's snapshot feature, or generated via periodic loads from external
    dimension data sources. In either of those cases, there is typically a time dimension
    associated with the SCD data source that indicates the start and end times of a
    validity window, where the dimension value is valid for any time within that range.
    """

    @property
    @abstractmethod
    def is_start(self) -> bool:  # noqa: D102
        pass

    @property
    @abstractmethod
    def is_end(self) -> bool:  # noqa: D102
        pass


class DimensionTypeParams(Protocol):
    """PydanticDimension type params add context to some types of dimensions (like time)."""

    @property
    @abstractmethod
    def time_granularity(self) -> TimeGranularity:  # noqa: D102
        pass

    @property
    @abstractmethod
    def validity_params(self) -> Optional[DimensionValidityParams]:  # noqa: D102
        pass


class Dimension(Protocol):
    """Describes a dimension."""

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D102
        pass

    @property
    @abstractmethod
    def description(self) -> Optional[str]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def type(self) -> DimensionType:  # noqa: D102
        pass

    @property
    @abstractmethod
    def is_partition(self) -> bool:  # noqa: D102
        pass

    @property
    @abstractmethod
    def type_params(self) -> Optional[DimensionTypeParams]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def expr(self) -> Optional[str]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def metadata(self) -> Optional[Metadata]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def reference(self) -> DimensionReference:
        """Returns a DimensionReference object for the dimension implementation."""
        ...

    @property
    @abstractmethod
    def time_dimension_reference(self) -> Optional[TimeDimensionReference]:
        """Returns a TimeDimensionReference if the dimension implementation is a time dimension."""
        ...

    @property
    @abstractmethod
    def validity_params(self) -> Optional[DimensionValidityParams]:
        """Returns the DimensionValidityParams if they exist for the dimension implementation."""
        ...

    @property
    @abstractmethod
    def label(self) -> Optional[str]:
        """Returns a string representing a human readable label for the dimension."""
        pass

    @property
    @abstractmethod
    def config(self) -> Optional[SemanticLayerElementConfig]:  # noqa: D102
        pass
