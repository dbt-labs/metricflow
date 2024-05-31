from __future__ import annotations

import pandas as pd
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.time.pandas_adjuster import (
    adjust_to_end_of_period,
    adjust_to_start_of_period,
    is_period_end,
    is_period_start,
)


def format_with_first_or_last(time_granularity: TimeGranularity) -> bool:
    """Indicates that this can only be calculated if query results display the first or last date of the period."""
    return time_granularity in [TimeGranularity.MONTH, TimeGranularity.QUARTER, TimeGranularity.YEAR]


def match_start_or_end_of_period(
    time_granularity: TimeGranularity, date_to_match: pd.Timestamp, date_to_adjust: pd.Timestamp
) -> pd.Timestamp:
    """Adjust date_to_adjust to be start or end of period based on if date_to_match is at start or end of period."""
    if is_period_start(time_granularity, date_to_match):
        return adjust_to_start_of_period(time_granularity, date_to_adjust)
    elif is_period_end(time_granularity, date_to_match):
        return adjust_to_end_of_period(time_granularity, date_to_adjust)
    else:
        raise ValueError(
            f"Expected `date_to_match` to fall at the start or end of the granularity period. Got '{date_to_match}' for granularity {time_granularity}."
        )


def string_to_time_granularity(s: str) -> TimeGranularity:  # noqa: D103
    values = {item.value: item for item in TimeGranularity}
    return values[s]
