from __future__ import annotations

from typing import List, Optional

import pytest
from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import (
    PydanticMeasure,
    PydanticMeasureAggregationParameters,
)
from metricflow_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from metricflow_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.transformations.flatten_simple_metrics_with_measure_inputs import (
    FlattenSimpleMetricsWithMeasureInputsRule,
)
from metricflow_semantic_interfaces.transformations.semantic_manifest_transformer import (
    PydanticSemanticManifestTransformer,
)
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)


def _project_config() -> PydanticProjectConfiguration:
    # Minimal project configuration for constructing a manifest directly in tests
    return PydanticProjectConfiguration()


DEFAULT_MEASURE_NAME = "orders"


def _make_semantic_model_with_measure(
    *,
    name: str,
    time_dimension: str,
    measures: List[PydanticMeasure],
) -> PydanticSemanticModel:
    """Helper to build a semantic model with a single time dimension and provided measures."""
    return PydanticSemanticModel(
        name=name,
        node_relation=PydanticNodeRelation(alias=name, schema_name="schema"),
        entities=[PydanticEntity(name="e", type=EntityType.PRIMARY)],
        dimensions=[
            PydanticDimension(
                name=time_dimension,
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            )
        ],
        measures=measures,
    )


def test_metric_with_measure_and_metric_agg_params_is_unchanged() -> None:
    """If a simple metric has both measure and metric_aggregation_params, it should not be altered."""
    # Build a minimal semantic model with one measure
    sm = PydanticSemanticModel(
        name="sm1",
        node_relation=PydanticNodeRelation(alias="sm1", schema_name="schema"),
        entities=[PydanticEntity(name="e1", type=EntityType.PRIMARY)],
        dimensions=[
            PydanticDimension(
                name="ds",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            )
        ],
        measures=[
            PydanticMeasure(
                name="m1",
                agg=AggregationType.SUM,
                agg_time_dimension="ds",
                expr="value",
            )
        ],
    )

    # Create a metric that already has both a measure and metric_aggregation_params
    metric = PydanticMetric(
        name="m1",
        description="pre-flatten",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(
                name="m1",
                fill_nulls_with=5,
                join_to_timespine=True,
            ),
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="sm1",
                agg=AggregationType.SUM,
                agg_params=None,
                agg_time_dimension="ds",
                non_additive_dimension=None,
            ),
            expr="value",
            join_to_timespine=False,
            fill_nulls_with=None,
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[metric],
        project_configuration=_project_config(),
    )

    transformed = PydanticSemanticManifestTransformer.transform(manifest)

    # Verify nothing has changed in the metric
    assert len(transformed.metrics) == 1
    out = transformed.metrics[0]
    assert out.type == MetricType.SIMPLE
    assert out.type_params.measure == PydanticMetricInputMeasure(
        name="m1",
        fill_nulls_with=5,
        join_to_timespine=True,
    )
    assert out.type_params.metric_aggregation_params is not None
    assert out.type_params.metric_aggregation_params.semantic_model == "sm1"
    assert out.type_params.metric_aggregation_params.agg == AggregationType.SUM
    assert out.type_params.metric_aggregation_params.agg_time_dimension == "ds"
    assert out.type_params.expr == "value"
    # These are not changed
    assert out.type_params.join_to_timespine is False
    assert out.type_params.fill_nulls_with is None


def test_metric_with_measure_only_gets_populated_and_referenced_metric_uses_values() -> None:
    """Populates fields for simple metric with only measure; referencing metric remains intact across models."""
    # Two semantic models, the measure lives in sm2
    sm1 = _make_semantic_model_with_measure(name="sm1", time_dimension="ds", measures=[])

    sm2 = _make_semantic_model_with_measure(
        name="sm2",
        time_dimension="event_time",
        measures=[
            PydanticMeasure(
                name=DEFAULT_MEASURE_NAME,
                agg=AggregationType.PERCENTILE,
                agg_time_dimension="event_time",
                expr="amount",
                agg_params=PydanticMeasureAggregationParameters(percentile=50.0, use_discrete_percentile=True),
            )
        ],
    )

    # Metric that should get populated by the rule (has a measure, no metric_aggregation_params)
    metric_to_flatten = PydanticMetric(
        name=DEFAULT_MEASURE_NAME,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(
                name=DEFAULT_MEASURE_NAME,
                join_to_timespine=True,
                fill_nulls_with=7,
            ),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm1, sm2],
        metrics=[metric_to_flatten],
        project_configuration=_project_config(),
    )

    transformed = FlattenSimpleMetricsWithMeasureInputsRule.transform_model(manifest)

    # Ensure both metrics are present
    assert len(transformed.metrics) == 1

    # Find updated simple metric
    updated_simple = next(m for m in transformed.metrics if m.name == "orders")
    assert updated_simple.type == MetricType.SIMPLE
    # The rule should populate aggregation params from the measure in sm2
    assert updated_simple.type_params.metric_aggregation_params is not None
    agg_params = updated_simple.type_params.metric_aggregation_params
    assert agg_params.semantic_model == "sm2"
    assert agg_params.agg == AggregationType.PERCENTILE
    assert agg_params.agg_time_dimension == "event_time"
    assert updated_simple.type_params.expr == "amount"
    # join_to_timespine should be set to True and fill_nulls_with to 7 by
    # pulling from the measure
    assert updated_simple.type_params.join_to_timespine is True
    assert updated_simple.type_params.fill_nulls_with == 7


@pytest.mark.parametrize(
    "join_to_timespine,fill_nulls_with",
    [
        (True, None),  # join_to_timespine only
        (False, 12),  # fill_nulls_with only
        (True, 45),  # both set
        (False, None),  # neither set
    ],
)
def test_measure_input_non_filter_fields_applied_with_measure_filter(
    join_to_timespine: bool,
    fill_nulls_with: Optional[int],
) -> None:
    """Verifies join_to_timespine and fill_nulls_with are copied from measure input; measure filter merges."""
    # Build semantic models; measure lives in sm2
    sm1 = PydanticSemanticModel(
        name="sm1",
        node_relation=PydanticNodeRelation(alias="sm1", schema_name="schema"),
        entities=[PydanticEntity(name="e1", type=EntityType.PRIMARY)],
        dimensions=[
            PydanticDimension(
                name="ds",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            )
        ],
        measures=[],
    )

    sm2 = PydanticSemanticModel(
        name="sm2",
        node_relation=PydanticNodeRelation(alias="sm2", schema_name="schema"),
        entities=[PydanticEntity(name="e2", type=EntityType.PRIMARY)],
        dimensions=[
            PydanticDimension(
                name="event_time",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            )
        ],
        measures=[
            PydanticMeasure(
                name="orders",
                agg=AggregationType.PERCENTILE,
                agg_params=PydanticMeasureAggregationParameters(percentile=50.0, use_discrete_percentile=True),
                agg_time_dimension="event_time",
                expr="amount",
            )
        ],
    )

    # Measure input has a filter, metric has none
    measure_filter = PydanticWhereFilterIntersection(
        where_filters=[PydanticWhereFilter(where_sql_template="ds >= '2020-01-01'")]
    )

    metric_to_flatten = PydanticMetric(
        name="orders",
        description="needs flatten",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(
                name="orders",
                fill_nulls_with=fill_nulls_with,
                join_to_timespine=join_to_timespine,
                filter=measure_filter,
            ),
        ),
        # no metric-level filter
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm1, sm2], metrics=[metric_to_flatten], project_configuration=_project_config()
    )

    transformed = FlattenSimpleMetricsWithMeasureInputsRule.transform_model(manifest)

    assert len(transformed.metrics) == 1, "Expected exactly one metric after transformation"
    updated_simple = transformed.metrics[0]

    # Aggregation params and expr are copied from the measure
    assert updated_simple.type_params.metric_aggregation_params is not None
    assert updated_simple.type_params.metric_aggregation_params.semantic_model == "sm2"
    assert updated_simple.type_params.metric_aggregation_params.agg_time_dimension == "event_time"
    assert updated_simple.type_params.expr == "amount"

    # Non-filter measure input fields applied appropriately
    if fill_nulls_with is not None:
        assert updated_simple.type_params.fill_nulls_with == fill_nulls_with
    else:
        assert updated_simple.type_params.fill_nulls_with is None

    if join_to_timespine:
        assert updated_simple.type_params.join_to_timespine is True
    else:
        assert updated_simple.type_params.join_to_timespine is False

    # Measure filter should be surfaced on the metric
    assert updated_simple.filter is not None
    assert sorted([wf.where_sql_template for wf in updated_simple.filter.where_filters]) == sorted(
        ["ds >= '2020-01-01'"]
    )


@pytest.mark.parametrize(
    "has_measure_filter,has_metric_filter",
    [
        (True, False),  # only measure filter
        (False, True),  # only metric filter
        (True, True),  # both
        (False, False),  # neither
    ],
)
def test_filter_merging_between_metric_and_measure_input(has_measure_filter: bool, has_metric_filter: bool) -> None:
    """Verifies filters from measure input and metric are merged as expected."""
    sm = _make_semantic_model_with_measure(
        name="sm",
        time_dimension="ds",
        measures=[
            PydanticMeasure(
                name=DEFAULT_MEASURE_NAME,
                agg=AggregationType.SUM,
                agg_time_dimension="ds",
            )
        ],
    )

    measure_filter = (
        PydanticWhereFilterIntersection(where_filters=[PydanticWhereFilter(where_sql_template="amount > 0")])
        if has_measure_filter
        else None
    )
    metric_filter = (
        PydanticWhereFilterIntersection(where_filters=[PydanticWhereFilter(where_sql_template="e = 'x'")])
        if has_metric_filter
        else None
    )

    metric = PydanticMetric(
        name=DEFAULT_MEASURE_NAME,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(
                name=DEFAULT_MEASURE_NAME,
                filter=measure_filter,
            ),
        ),
        filter=metric_filter,
    )

    manifest = PydanticSemanticManifest(semantic_models=[sm], metrics=[metric], project_configuration=_project_config())

    transformed = FlattenSimpleMetricsWithMeasureInputsRule.transform_model(manifest)
    assert len(transformed.metrics) == 1, "Expected exactly one metric after transformation"
    out = transformed.metrics[0]

    if has_measure_filter or has_metric_filter:
        assert out.filter is not None
        expected_order = []
        if has_metric_filter:
            expected_order.append("e = 'x'")
        if has_measure_filter:
            expected_order.append("amount > 0")
        assert sorted([wf.where_sql_template for wf in out.filter.where_filters]) == sorted(expected_order)
    else:
        assert out.filter is None


@pytest.mark.parametrize(
    "measure_expr,expected_expr",
    [
        ("amount", "amount"),  # measure has expr
        (None, "orders"),  # measure has no expr, falls back to name
    ],
)
def test_expr_population_from_measure_or_name(measure_expr: Optional[str], expected_expr: str) -> None:
    """Verifies expr is taken from measure.expr if available; otherwise from measure.name."""
    sm = _make_semantic_model_with_measure(
        name="sm",
        time_dimension="ds",
        measures=[
            PydanticMeasure(
                name=DEFAULT_MEASURE_NAME,
                agg=AggregationType.SUM,
                agg_time_dimension="ds",
                expr=measure_expr,
            )
        ],
    )

    measure_filter = PydanticWhereFilterIntersection(
        where_filters=[PydanticWhereFilter(where_sql_template="ds >= '2020-01-01'")]
    )

    metric = PydanticMetric(
        name=DEFAULT_MEASURE_NAME,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(
                name=DEFAULT_MEASURE_NAME,
                filter=measure_filter,
            ),
        ),
    )

    manifest = PydanticSemanticManifest(semantic_models=[sm], metrics=[metric], project_configuration=_project_config())

    transformed = FlattenSimpleMetricsWithMeasureInputsRule.transform_model(manifest)
    assert len(transformed.metrics) == 1, "Expected exactly one metric after transformation"
    out = transformed.metrics[0]

    assert out.type_params.expr == expected_expr
