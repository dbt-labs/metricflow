from __future__ import annotations

import logging

from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

logger = logging.getLogger(__name__)


def test_min_queryable_time_granularity_for_different_agg_time_grains(  # noqa: D103
    extended_date_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    metric_lookup = extended_date_semantic_manifest_lookup.metric_lookup

    min_queryable_grain = metric_lookup.get_min_queryable_time_granularity(
        MetricReference("monthly_bookings_to_daily_bookings")
    )

    # Since `monthly_bookings_to_daily_bookings` is based on metrics with DAY and MONTH aggregation time grains,
    # the minimum queryable grain should be MONTH.
    assert min_queryable_grain == TimeGranularity.MONTH


def test_custom_offset_window_for_metric(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Test offset window with custom grain supplied."""
    metric = simple_semantic_manifest_lookup.metric_lookup.get_metric(MetricReference("bookings_offset_alien_day"))

    assert len(metric.input_metrics) == 1
    assert metric.input_metrics[0].offset_window == PydanticMetricTimeWindow(count=1, granularity="alien_day")
