from __future__ import annotations

import logging

import pytest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup

from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetricInput,
    PydanticMetricTimeWindow,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.references import MetricReference
from metricflow_semantic_interfaces.test_utils import metric_with_guaranteed_meta
from metricflow_semantic_interfaces.type_enums import MetricType, TimeGranularity

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


@pytest.mark.parametrize(
    ("numerator", "denominator", "expected_alias"),
    (
        (PydanticMetricInput(name="a"), PydanticMetricInput(name="a"), None),
        (PydanticMetricInput(name="a"), PydanticMetricInput(name="b"), None),
        (PydanticMetricInput(name="a"), PydanticMetricInput(name="b", alias="c"), "c"),
        (PydanticMetricInput(name="a", alias="x"), PydanticMetricInput(name="b", alias="x"), "x__denominator"),
        (PydanticMetricInput(name="a", alias="b"), PydanticMetricInput(name="b"), "b__denominator"),
        (
            PydanticMetricInput(name="a", alias="x"),
            PydanticMetricInput(name="a", alias="x", exclude_dimensions=("d",)),
            "x__denominator",
        ),
    ),
)
def test_ratio_denominator_alias_separates_inputs_with_the_same_output_name(  # noqa: D103
    numerator: PydanticMetricInput, denominator: PydanticMetricInput, expected_alias: str | None
) -> None:
    metric = metric_with_guaranteed_meta(
        name="ratio",
        type=MetricType.RATIO,
        type_params=PydanticMetricTypeParams(numerator=numerator, denominator=denominator),
    )

    assert metric.type_params.numerator is not None and metric.type_params.denominator is not None
    assert MetricLookup.input_metric_alias(metric, metric.type_params.denominator) == expected_alias
    assert MetricLookup.input_metric_alias(metric, metric.type_params.numerator) == numerator.alias
