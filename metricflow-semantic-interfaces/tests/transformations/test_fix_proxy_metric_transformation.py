from __future__ import annotations

import logging
from typing import Any

from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.transformations.fix_proxy_metrics import (
    FixProxyMetricsRule,
)
from metricflow_semantic_interfaces.type_enums import AggregationType, MetricType

from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def get_empty_semantic_model(name: str = "example_model") -> PydanticSemanticModel:
    """Helper to create an empty semantic model."""
    return PydanticSemanticModel(
        name=name,
        node_relation=PydanticNodeRelation(alias=name, schema_name="example_schema"),
        entities=[],
        measures=[],
    )


def test_fixes_faulty_proxy_metric_with_measure_expr(caplog: Any) -> None:  # type: ignore[misc]
    """Test that a proxy metric with expr == metric.name gets fixed when measure has an expr."""
    metric_name = "my_metric"
    measure_name = "my_sum_measure"
    measure_expr = "revenue_amount"
    semantic_model_name = "revenue_model"

    # Create a measure with an expr
    measure = PydanticMeasure(name=measure_name, agg=AggregationType.SUM, expr=measure_expr)
    semantic_model = get_empty_semantic_model(semantic_model_name)
    semantic_model.measures = [measure]

    # Create a faulty proxy metric where expr == metric.name
    metric = PydanticMetric(
        name=metric_name,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(name=measure_name),
            expr=metric_name,  # This is the faulty expr
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model=semantic_model_name,
                agg=AggregationType.SUM,
            ),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[metric],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    # Transform the manifest
    with caplog.at_level(logging.WARNING):
        result = FixProxyMetricsRule.transform_model(manifest)

    # This warning log should be thrown
    assert "should not have an expr set" in caplog.text

    fixed_metric = result.metrics[0]

    # The expr should now be the measure's expr
    assert fixed_metric.type_params.expr == measure_expr, "Metric expr should have been fixed to use measure's expr"
    assert fixed_metric.name == metric_name, "Metric name should not have changed"
    assert fixed_metric.type == MetricType.SIMPLE, "Metric type should not have changed"


def test_does_not_change_non_faulty_proxy_metric(caplog: Any) -> None:  # type: ignore[misc]
    """Test that a proxy metric with correct expr (expr != metric.name) is not changed."""
    measure_name = "my_measure"
    measure_expr = "actual_column"
    correct_expr = "actual_column"
    semantic_model_name = "test_model"

    # Create a measure with an expr
    measure = PydanticMeasure(name=measure_name, agg=AggregationType.SUM, expr=measure_expr)
    semantic_model = get_empty_semantic_model(semantic_model_name)
    semantic_model.measures = [measure]

    # Create a metric with a correct expr (not equal to metric name)
    metric = PydanticMetric(
        name=measure_name,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(name=measure_name),
            expr=correct_expr,  # This is already correct
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model=semantic_model_name,
                agg=AggregationType.SUM,
            ),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[metric],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    # Transform the manifest
    with caplog.at_level(logging.WARNING):
        result = FixProxyMetricsRule.transform_model(manifest)
    unchanged_metric = result.metrics[0]

    # This warning log should not be thrown
    assert "should not have an expr set" not in caplog.text

    # The expr should remain unchanged
    assert unchanged_metric.type_params.expr == correct_expr, "Metric expr should not have changed"


def test_fixes_manually_defined_not_same_name_as_measure() -> None:
    """Test that a manually defined metric with metric.name != measure.name gets fixed to use the measure expr."""
    metric_name = "my_metric"  # Name doesn't match measure name
    measure_name = "my_sum_measure"
    measure_expr = "revenue_amount"
    semantic_model_name = "revenue_model"

    # Create a measure with an expr
    measure = PydanticMeasure(name=measure_name, agg=AggregationType.SUM, expr=measure_expr)
    semantic_model = get_empty_semantic_model(semantic_model_name)
    semantic_model.measures = [measure]

    # Create a faulty proxy metric where expr == metric.name
    metric = PydanticMetric(
        name=metric_name,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(name=measure_name),
            expr="asd",  # faulty expr
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model=semantic_model_name,
                agg=AggregationType.SUM,
            ),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[metric],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    # Transform the manifest
    result = FixProxyMetricsRule.transform_model(manifest)
    fixed_metric = result.metrics[0]

    # The expr should now be the measure's expr
    assert fixed_metric.type_params.expr == measure_expr, "Metric expr should have been fixed to use measure's expr"
    assert fixed_metric.type == MetricType.SIMPLE, "Metric type should not have changed"
