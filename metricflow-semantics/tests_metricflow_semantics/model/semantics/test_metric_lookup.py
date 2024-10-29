from __future__ import annotations

import logging

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
