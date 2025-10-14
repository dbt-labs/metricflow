from __future__ import annotations

from typing import List

import pytest
from metricflow_semantic_interfaces.errors import ModelTransformError
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import (
    PydanticMeasure,
    PydanticMeasureAggregationParameters,
)
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInput,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.transformations.convert_median import (
    ConvertMedianMetricToPercentile,
    ConvertMedianToPercentileRule,
)
from metricflow_semantic_interfaces.type_enums import AggregationType, EntityType
from metricflow_semantic_interfaces.type_enums.metric_type import MetricType

from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def build_manifest_with_single_measure(measure: PydanticMeasure) -> PydanticSemanticManifest:
    """Helper to construct a manifest with a single semantic model and one measure."""
    semantic_model = PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id", type=EntityType.PRIMARY)],
        measures=[measure],
    )
    return PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )


def build_manifest_with_metrics(metrics: List[PydanticMetric]) -> PydanticSemanticManifest:
    """Helper to construct a manifest with a single semantic model and a list of metrics."""
    semantic_model = PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[],
        measures=[],
    )
    return PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=metrics,
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )


def test_metric_median_rule_does_not_change_non_median_agg() -> None:
    """A metric with agg not MEDIAN remains unchanged."""
    original_metric = PydanticMetric(
        name="not_median_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.SUM,
            ),
            expr="revenue",
        ),
    )

    manifest = build_manifest_with_metrics([original_metric.copy(deep=True)])

    out = ConvertMedianMetricToPercentile.transform_model(manifest)
    out_metric = next(m for m in out.metrics if m.name == "not_median_metric")
    assert out_metric == original_metric


def test_metric_median_rule_does_not_change_non_simple_type() -> None:
    """If a metric is not SIMPLE, it remains unchanged."""
    base_average_metric = PydanticMetric(
        name="base_avg_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.AVERAGE,
            ),
            expr="price",
        ),
    )

    original_derived_metric = PydanticMetric(
        name="derived_metric",
        type=MetricType.DERIVED,
        type_params=PydanticMetricTypeParams(
            metrics=[PydanticMetricInput(name="base_avg_metric")],
        ),
    )

    manifest = build_manifest_with_metrics([base_average_metric, original_derived_metric.copy(deep=True)])

    out = ConvertMedianMetricToPercentile.transform_model(manifest)
    out_metric = next(m for m in out.metrics if m.name == "derived_metric")
    assert out_metric == original_derived_metric


def test_metric_median_rule_sets_percentile_params_when_missing_and_changes_agg() -> None:
    """MEDIAN metric without params gets percentile 0.5 and agg becomes PERCENTILE."""
    metric = PydanticMetric(
        name="median_no_params_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.MEDIAN,
            ),
            expr="value",
        ),
    )

    manifest = build_manifest_with_metrics([metric])

    out = ConvertMedianMetricToPercentile.transform_model(manifest)
    out_metric = next(m for m in out.metrics if m.name == "median_no_params_metric")
    assert out_metric.type_params.metric_aggregation_params is not None
    assert (
        out_metric.type_params.metric_aggregation_params.agg == AggregationType.PERCENTILE
    ), "Aggregation type should be changed to PERCENTILE for MEDIAN metric, but was not."
    assert out_metric.type_params.metric_aggregation_params.agg_params is not None
    assert (
        out_metric.type_params.metric_aggregation_params.agg_params.percentile == 0.5
    ), "Percentile should be set to 0.5 for MEDIAN metric, but was different."


def test_metric_median_rule_raises_when_percentile_not_median() -> None:
    """MEDIAN metric with percentile != 0.5 raises an error."""
    metric = PydanticMetric(
        name="median_bad_percentile_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.MEDIAN,
                agg_params=PydanticMeasureAggregationParameters(percentile=0.9),
            ),
            expr="value",
        ),
    )

    manifest = build_manifest_with_metrics([metric])

    with pytest.raises(
        ModelTransformError,
        match="uses a MEDIAN aggregation, while percentile is set to '0.9', a conflicting value",
    ):
        ConvertMedianMetricToPercentile.transform_model(manifest)


def test_metric_median_rule_raises_when_discrete_percentile_true() -> None:
    """MEDIAN metric with use_discrete_percentile set raises an error."""
    metric = PydanticMetric(
        name="median_discrete_percentile_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.MEDIAN,
                agg_params=PydanticMeasureAggregationParameters(use_discrete_percentile=True),
            ),
            expr="value",
        ),
    )

    manifest = build_manifest_with_metrics([metric])

    with pytest.raises(
        ModelTransformError,
        match="uses a MEDIAN aggregation, while use_discrete_percentile is set to true",
    ):
        ConvertMedianMetricToPercentile.transform_model(manifest)


def test_metric_median_rule_preserves_existing_median_percentile_value() -> None:
    """MEDIAN metric with percentile == 0.5 remains 0.5 after transform and agg becomes PERCENTILE."""
    metric = PydanticMetric(
        name="median_ok_percentile_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.MEDIAN,
                agg_params=PydanticMeasureAggregationParameters(percentile=0.5),
            ),
            expr="value",
        ),
    )

    manifest = build_manifest_with_metrics([metric])

    out = ConvertMedianMetricToPercentile.transform_model(manifest)
    out_metric = next(m for m in out.metrics if m.name == "median_ok_percentile_metric")
    assert out_metric.type_params.metric_aggregation_params is not None
    assert (
        out_metric.type_params.metric_aggregation_params.agg == AggregationType.PERCENTILE
    ), "Aggregation type should be changed to PERCENTILE for MEDIAN metric, but was not."
    assert out_metric.type_params.metric_aggregation_params.agg_params is not None
    assert (
        out_metric.type_params.metric_aggregation_params.agg_params.percentile == 0.5
    ), "Percentile should remain 0.5 for MEDIAN metric, but was changed."


def test_metric_median_rule_iterates_across_multiple_metrics() -> None:
    """Two MEDIAN metrics are converted; two non-MEDIAN metrics remain unchanged."""
    # MEDIAN metrics
    metric_median_no_params = PydanticMetric(
        name="median_value_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.MEDIAN,
            ),
            expr="value",
        ),
    )
    metric_median_with_params = PydanticMetric(
        name="median_latency_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.MEDIAN,
                agg_params=PydanticMeasureAggregationParameters(percentile=0.5),
            ),
            expr="latency",
        ),
    )

    # Non-MEDIAN metrics
    original_non_median_sum_metric = PydanticMetric(
        name="total_revenue_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.SUM,
            ),
            expr="revenue",
        ),
    )
    original_non_median_average_metric = PydanticMetric(
        name="average_price_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.AVERAGE,
            ),
            expr="price",
        ),
    )

    manifest = build_manifest_with_metrics(
        [
            metric_median_no_params,
            metric_median_with_params,
            original_non_median_sum_metric.copy(deep=True),
            original_non_median_average_metric.copy(deep=True),
        ]
    )

    out = ConvertMedianMetricToPercentile.transform_model(manifest)

    # MEDIAN metrics changed to PERCENTILE with percentile 0.5
    out_median_no_params = next(m for m in out.metrics if m.name == "median_value_metric")
    assert out_median_no_params.type_params.metric_aggregation_params is not None
    assert (
        out_median_no_params.type_params.metric_aggregation_params.agg == AggregationType.PERCENTILE
    ), "Aggregation type should be changed to PERCENTILE for MEDIAN metric, but was not."
    assert out_median_no_params.type_params.metric_aggregation_params.agg_params is not None
    assert (
        out_median_no_params.type_params.metric_aggregation_params.agg_params.percentile == 0.5
    ), "Percentile should be set to 0.5 for MEDIAN metric, but was different."

    out_median_with_params = next(m for m in out.metrics if m.name == "median_latency_metric")
    assert out_median_with_params.type_params.metric_aggregation_params is not None
    assert (
        out_median_with_params.type_params.metric_aggregation_params.agg == AggregationType.PERCENTILE
    ), "Aggregation type should be changed to PERCENTILE for MEDIAN metric, but was not."
    assert out_median_with_params.type_params.metric_aggregation_params.agg_params is not None
    assert (
        out_median_with_params.type_params.metric_aggregation_params.agg_params.percentile == 0.5
    ), "Percentile should remain 0.5 for MEDIAN metric, but was changed."

    # Non-MEDIAN metrics unchanged
    out_sum_metric = next(m for m in out.metrics if m.name == "total_revenue_metric")
    assert out_sum_metric == original_non_median_sum_metric

    out_average_metric = next(m for m in out.metrics if m.name == "average_price_metric")
    assert out_average_metric == original_non_median_average_metric


def test_legacy_measure_median_rule_does_not_change_non_median_agg() -> None:
    """A measure with agg not MEDIAN remains unchanged."""
    original = PydanticMeasure(name="not_median", agg=AggregationType.SUM, expr="revenue")
    manifest = build_manifest_with_single_measure(original.copy(deep=True))

    out = ConvertMedianToPercentileRule.transform_model(manifest)
    out_measure = out.semantic_models[0].measures[0]
    assert out_measure == original


def test_legacy_measure_median_rule_sets_percentile_params_when_missing_and_changes_agg() -> None:
    """MEDIAN measure without agg_params gets params with percentile 0.5 and agg becomes PERCENTILE."""
    measure = PydanticMeasure(name="median_no_params", agg=AggregationType.MEDIAN, expr="value")
    manifest = build_manifest_with_single_measure(measure)

    out = ConvertMedianToPercentileRule.transform_model(manifest)
    out_measure = out.semantic_models[0].measures[0]
    assert out_measure.agg == AggregationType.PERCENTILE
    assert out_measure.agg_params is not None
    assert out_measure.agg_params.percentile == 0.5


def test_legacy_measure_median_rule_raises_when_percentile_not_median() -> None:
    """MEDIAN measure with agg_params.percentile != 0.5 raises an error."""
    measure = PydanticMeasure(
        name="median_bad_percentile",
        agg=AggregationType.MEDIAN,
        expr="value",
        agg_params=PydanticMeasureAggregationParameters(percentile=0.9),
    )
    manifest = build_manifest_with_single_measure(measure)

    with pytest.raises(
        ModelTransformError,
        match="uses a MEDIAN aggregation, while percentile is set to '0.9', a conflicting value",
    ):
        ConvertMedianToPercentileRule.transform_model(manifest)


def test_legacy_measure_median_rule_raises_when_discrete_percentile_true() -> None:
    """MEDIAN measure with agg_params.use_discrete_percentile set raises an error."""
    measure = PydanticMeasure(
        name="median_discrete_percentile",
        agg=AggregationType.MEDIAN,
        expr="value",
        agg_params=PydanticMeasureAggregationParameters(use_discrete_percentile=True),
    )
    manifest = build_manifest_with_single_measure(measure)

    with pytest.raises(
        ModelTransformError,
        match="uses a MEDIAN aggregation, while use_discrete_percentile is set to true",
    ):
        ConvertMedianToPercentileRule.transform_model(manifest)


def test_legacy_measure_median_rule_preserves_existing_median_percentile_value() -> None:
    """MEDIAN measure with agg_params.percentile == 0.5 remains 0.5 after transform and agg becomes PERCENTILE."""
    measure = PydanticMeasure(
        name="median_ok_percentile",
        agg=AggregationType.MEDIAN,
        expr="value",
        agg_params=PydanticMeasureAggregationParameters(percentile=0.5),
    )
    manifest = build_manifest_with_single_measure(measure)

    out = ConvertMedianToPercentileRule.transform_model(manifest)
    out_measure = out.semantic_models[0].measures[0]
    assert out_measure.agg == AggregationType.PERCENTILE
    assert out_measure.agg_params is not None
    assert out_measure.agg_params.percentile == 0.5


def test_legacy_measure_median_rule_iterates_across_multiple_measures() -> None:
    """Test that we can apply (and not apply) this to multiple measures in a single model.

    The expectation is that we have two MEDIAN measures that are converted and two
    non-MEDIAN measures that remain unchanged.
    """
    original_non_median_sum = PydanticMeasure(name="total_revenue", agg=AggregationType.SUM, expr="revenue")
    original_non_median_average = PydanticMeasure(name="average_price", agg=AggregationType.AVERAGE, expr="price")

    median_without_params = PydanticMeasure(name="median_value", agg=AggregationType.MEDIAN, expr="value")
    median_with_params = PydanticMeasure(
        name="median_latency",
        agg=AggregationType.MEDIAN,
        expr="latency",
        agg_params=PydanticMeasureAggregationParameters(percentile=0.5),
    )

    semantic_model = PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id", type=EntityType.PRIMARY)],
        measures=[
            median_without_params,
            median_with_params,
            original_non_median_sum.copy(deep=True),
            original_non_median_average.copy(deep=True),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    out = ConvertMedianToPercentileRule.transform_model(manifest)
    out_model = out.semantic_models[0]

    # MEDIAN measures changed to PERCENTILE with percentile 0.5
    out_for_median_without_params = next(m for m in out_model.measures if m.name == "median_value")
    assert (
        out_for_median_without_params.agg == AggregationType.PERCENTILE
    ), "Aggregation type should be changed to PERCENTILE for MEDIAN measure, but was not."
    assert out_for_median_without_params.agg_params is not None
    assert (
        out_for_median_without_params.agg_params.percentile == 0.5
    ), "Percentile should be added for this MEDIAN measure and set to 0.5."

    out_for_median_with_params = next(m for m in out_model.measures if m.name == "median_latency")
    assert (
        out_for_median_with_params.agg == AggregationType.PERCENTILE
    ), "Aggregation type should be changed to PERCENTILE for MEDIAN measure, but was not."
    assert out_for_median_with_params.agg_params is not None
    assert (
        out_for_median_with_params.agg_params.percentile == 0.5
    ), "Percentile should be still be 0.5 for this MEDIAN measure, but was changed."

    # Non-MEDIAN measures unchanged
    out_total_revenue = next(m for m in out_model.measures if m.name == "total_revenue")
    assert out_total_revenue == original_non_median_sum

    out_average_price = next(m for m in out_model.measures if m.name == "average_price")
    assert out_average_price == original_non_median_average
