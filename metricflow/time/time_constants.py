from __future__ import annotations

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

# Python formatting string to use for converting datetime to ISO8601
ISO8601_PYTHON_FORMAT = "%Y-%m-%d"
ISO8601_PYTHON_TS_FORMAT = "%Y-%m-%d %H:%M:%S"

SUPPORTED_GRANULARITIES = [
    TimeGranularity.DAY,
    TimeGranularity.WEEK,
    TimeGranularity.MONTH,
    TimeGranularity.QUARTER,
    TimeGranularity.YEAR,
]
