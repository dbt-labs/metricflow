from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.errors.error_classes import FeatureNotSupportedError


def error_if_not_standard_grain(input_granularity: str, context: Optional[str] = None) -> TimeGranularity:
    """Cast input grainularity string to TimeGranularity, otherwise error.

    TODO: Not needed once, custom grain is supported for most things.
    """
    try:
        time_grain = TimeGranularity(input_granularity)
    except ValueError:
        error_msg = f"Received a non-standard time granularity, which is not supported at the moment, received: {input_granularity}."
        if context:
            error_msg += f"\nContext: {context}"
        raise FeatureNotSupportedError(error_msg)
    return time_grain
