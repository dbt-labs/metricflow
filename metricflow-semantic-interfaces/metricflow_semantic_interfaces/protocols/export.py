from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol

from metricflow_semantic_interfaces.type_enums.export_destination_type import (
    ExportDestinationType,
)


class Export(Protocol):
    """Configuration for writing query results to a table."""

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D102
        pass

    @property
    @abstractmethod
    def config(self) -> ExportConfig:  # noqa: D102
        pass


class ExportConfig(Protocol):
    """Nested configuration attributes for exports."""

    @property
    @abstractmethod
    def export_as(self) -> ExportDestinationType:
        """Type of destination to write export to."""
        pass

    @property
    @abstractmethod
    def schema_name(self) -> Optional[str]:
        """Schema to write export to. Defaults to deployment schema."""
        pass

    @property
    @abstractmethod
    def alias(self) -> Optional[str]:
        """Name for table/filte export is written to. Defaults to export name."""
        pass
