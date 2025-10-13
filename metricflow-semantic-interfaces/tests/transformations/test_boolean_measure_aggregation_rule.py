from __future__ import annotations

from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.transformations.boolean_aggregations import (
    BooleanAggregationRule,
)
from metricflow_semantic_interfaces.transformations.boolean_measure import (
    BooleanMeasureAggregationRule,
)
from metricflow_semantic_interfaces.type_enums import AggregationType, EntityType, MetricType

from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def test_boolean_measure_aggregation_rule_transforms_only_sum_boolean_measures() -> None:
    """Validate SUM_BOOLEAN measures correctly mutate their expr and agg types.

    (Also validate that other measures are unchanged.)
    """
    # Three measures:
    # - other_measure: not SUM_BOOLEAN -> unchanged
    # - measure_with_expr: SUM_BOOLEAN with existing expr -> wrap expr, change agg to SUM
    # - measure_without_expr: SUM_BOOLEAN with no expr -> use measure name, change agg to SUM
    semantic_model = PydanticSemanticModel(
        name="this_semantic_model",
        node_relation=PydanticNodeRelation(alias="this_semantic_model", schema_name="schema"),
        entities=[PydanticEntity(name="e1", type=EntityType.PRIMARY)],
        measures=[
            PydanticMeasure(name="other_measure", agg=AggregationType.COUNT, expr="1"),
            PydanticMeasure(name="measure_with_expr", agg=AggregationType.SUM_BOOLEAN, expr="is_active"),
            PydanticMeasure(name="measure_without_expr", agg=AggregationType.SUM_BOOLEAN),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    out = BooleanMeasureAggregationRule.transform_model(manifest)
    out_sm = out.semantic_models[0]

    # Sanity: still three measures
    assert len(out_sm.measures) == 3

    # Unchanged non-boolean measure
    other_measure = next(m for m in out_sm.measures if m.name == "other_measure")
    assert other_measure.agg == AggregationType.COUNT
    assert other_measure.expr == "1"

    # SUM_BOOLEAN with existing expr -> wrapped and agg becomes SUM
    measure_with_expr = next(m for m in out_sm.measures if m.name == "measure_with_expr")
    assert measure_with_expr.agg == AggregationType.SUM
    assert measure_with_expr.expr == "CASE WHEN is_active THEN 1 ELSE 0 END"

    # SUM_BOOLEAN with no expr -> use measure name and agg becomes SUM
    measure_without_expr = next(m for m in out_sm.measures if m.name == "measure_without_expr")
    assert measure_without_expr.agg == AggregationType.SUM
    assert measure_without_expr.expr == "CASE WHEN measure_without_expr THEN 1 ELSE 0 END"


def test_boolean_aggregation_rule_transforms_only_sum_boolean_metrics() -> None:
    """Validate SUM_BOOLEAN metrics get wrapped expr and SUM agg; others unchanged."""
    # Three metrics (SIMPLE with metric_aggregation_params):
    # - other_metric: not SUM_BOOLEAN -> unchanged
    # - metric_with_expr: SUM_BOOLEAN with existing expr -> wrap expr, change agg to SUM
    # - metric_without_expr: SUM_BOOLEAN with no expr -> use metric name, change agg to SUM
    semantic_model = PydanticSemanticModel(
        name="this_semantic_model",
        node_relation=PydanticNodeRelation(alias="this_semantic_model", schema_name="schema"),
        entities=[PydanticEntity(name="e1", type=EntityType.PRIMARY)],
        measures=[],
    )

    other_metric = PydanticMetric(
        name="other_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="this_semantic_model",
                agg=AggregationType.COUNT,
            ),
            expr="1",
        ),
    )

    metric_with_expr = PydanticMetric(
        name="metric_with_expr",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="this_semantic_model",
                agg=AggregationType.SUM_BOOLEAN,
            ),
            expr="is_active",
        ),
    )

    metric_without_expr = PydanticMetric(
        name="metric_without_expr",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="this_semantic_model",
                agg=AggregationType.SUM_BOOLEAN,
            ),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[other_metric, metric_with_expr, metric_without_expr],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    out = BooleanAggregationRule.transform_model(manifest)

    # Sanity: still three metrics
    assert len(out.metrics) == 3

    # Unchanged non-boolean metric
    out_other = next(m for m in out.metrics if m.name == "other_metric")
    assert out_other.type == MetricType.SIMPLE
    assert out_other.type_params.metric_aggregation_params is not None
    assert out_other.type_params.metric_aggregation_params.agg == AggregationType.COUNT
    assert out_other.type_params.expr == "1"

    # SUM_BOOLEAN with existing expr -> wrapped and agg becomes SUM
    out_with_expr = next(m for m in out.metrics if m.name == "metric_with_expr")
    assert out_with_expr.type_params.metric_aggregation_params is not None
    assert out_with_expr.type_params.metric_aggregation_params.agg == AggregationType.SUM
    assert out_with_expr.type_params.expr == "CASE WHEN is_active THEN 1 ELSE 0 END"

    # SUM_BOOLEAN with no expr -> use metric name and agg becomes SUM
    out_without_expr = next(m for m in out.metrics if m.name == "metric_without_expr")
    assert out_without_expr.type_params.metric_aggregation_params is not None
    assert out_without_expr.type_params.metric_aggregation_params.agg == AggregationType.SUM
    assert out_without_expr.type_params.expr == "CASE WHEN metric_without_expr THEN 1 ELSE 0 END"
