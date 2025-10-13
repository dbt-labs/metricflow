from __future__ import annotations

from metricflow_semantic_interfaces.enum_extension import ExtendedEnum


class ExportDestinationType(ExtendedEnum):
    """Types of destinations that exports can be written to."""

    TABLE = "table"
    VIEW = "view"
