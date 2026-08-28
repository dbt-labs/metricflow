from __future__ import annotations

from metricflow_semantic_interfaces.enum_extension import ExtendedEnum


class DataType(ExtendedEnum):
    """Logical data type of a dimension or metric value, independent of physical representation.

    This is a passthrough annotation, not used by MetricFlow's own validation or query logic. It exists so
    that specific data types (e.g. from Ossie's `datatype` field) can be preserved through conversions without
    being collapsed into `DimensionType`'s coarse CATEGORICAL/TIME buckets.
    """

    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"
    DATETIME_TZ = "datetime_tz"
    OPAQUE = "opaque"
