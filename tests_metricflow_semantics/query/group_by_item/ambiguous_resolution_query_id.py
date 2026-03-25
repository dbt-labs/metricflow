from __future__ import annotations

from enum import Enum


class AmbiguousResolutionQueryId(Enum):
    """IDs to describe the various resolution DAGs used for testing."""

    NO_METRICS = "no_metrics"
    SIMPLE_METRIC = "simple_metric"
    CUMULATIVE_METRIC = "accumulate_last_2_months_metric"
    METRICS_WITH_SAME_TIME_GRAINS = "metrics_with_same_time_grains"
    METRICS_WITH_DIFFERENT_TIME_GRAINS = "metrics_with_different_time_grains"
    DERIVED_METRIC_WITH_SAME_PARENT_TIME_GRAINS = "derived_metric_with_same_parent_time_grains"
    DERIVED_METRIC_WITH_DIFFERENT_PARENT_TIME_GRAINS = "derived_metric_with_different_parent_time_grains"
