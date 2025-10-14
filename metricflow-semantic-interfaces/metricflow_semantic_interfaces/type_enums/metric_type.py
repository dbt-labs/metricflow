from __future__ import annotations

from metricflow_semantic_interfaces.enum_extension import ExtendedEnum


class MetricType(ExtendedEnum):
    """Currently supported metric types."""

    SIMPLE = "simple"
    RATIO = "ratio"
    CUMULATIVE = "cumulative"
    DERIVED = "derived"
    CONVERSION = "conversion"
