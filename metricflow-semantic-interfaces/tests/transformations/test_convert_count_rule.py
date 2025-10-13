from __future__ import annotations

import pytest
from metricflow_semantic_interfaces.errors import ModelTransformError
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
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
from metricflow_semantic_interfaces.transformations.convert_count import (
    ConvertCountMetricToSumRule,
    ConvertCountToSumRule,
)
from metricflow_semantic_interfaces.type_enums import AggregationType, EntityType
from metricflow_semantic_interfaces.type_enums.metric_type import MetricType

from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def get_empty_semantic_model() -> PydanticSemanticModel:  # noqa: D103
    return PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[],
        measures=[],
    )


def test_metric_convert_count_rule_does_not_change_non_count() -> None:
    """If a metric does not have agg type COUNT, it remains unchanged."""
    semantic_model = get_empty_semantic_model()

    original = PydanticMetric(
        name="non_count_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.AVERAGE,
            ),
            expr="revenue",
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[original.copy(deep=True)],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountMetricToSumRule.transform_model(manifest)
    out = next(m for m in result.metrics if m.name == "non_count_metric")
    assert out == original, "Metric should not have been changed, but was."


def test_metric_convert_count_rule_does_not_change_non_simple_type() -> None:
    """If a metric does not have type SIMPLE, it remains unchanged."""
    semantic_model = get_empty_semantic_model()

    base_average_metric = PydanticMetric(
        name="base_avg_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.COUNT,
            ),
            expr="price",
        ),
    )

    original = PydanticMetric(
        name="derived_metric",
        type=MetricType.DERIVED,
        type_params=PydanticMetricTypeParams(
            metrics=[PydanticMetricInput(name="base_avg_metric")],
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[base_average_metric, original.copy(deep=True)],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountMetricToSumRule.transform_model(manifest)
    out = next(m for m in result.metrics if m.name == "derived_metric")
    assert out == original, "Metric should not have been changed because it's not a SIMPLE metric, but was."


def test_metric_convert_count_rule_raises_when_count_without_expr() -> None:
    """Metric with agg type COUNT without expr should raise the expected error."""
    semantic_model = get_empty_semantic_model()

    metric = PydanticMetric(
        name="missing_expr_count_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.COUNT,
            ),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[metric],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    with pytest.raises(
        ModelTransformError,
        match="uses a COUNT aggregation, which requires an expr to be provided",
    ):
        ConvertCountMetricToSumRule.transform_model(manifest)


def test_metric_convert_count_rule_leaves_expr_one_but_sets_sum() -> None:
    """Metric with agg type COUNT and expr '1' should keep expr as '1' but set agg to SUM."""
    semantic_model = get_empty_semantic_model()

    metric = PydanticMetric(
        name="count_all_rows_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.COUNT,
            ),
            expr="1",
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[metric],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountMetricToSumRule.transform_model(manifest)
    out = next(m for m in result.metrics if m.name == "count_all_rows_metric")
    assert out.type_params.metric_aggregation_params is not None
    assert (
        out.type_params.metric_aggregation_params.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert out.type_params.expr == "1", "Expression should NOT have been changed, but was."


def test_metric_convert_count_rule_transforms_count_with_expr_to_case_expression() -> None:
    """Metric with agg type COUNT and expr other than '1' should wrap expr and set agg to SUM."""
    semantic_model = get_empty_semantic_model()

    metric = PydanticMetric(
        name="count_valid_values_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.COUNT,
            ),
            expr="is_valid",
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[metric],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountMetricToSumRule.transform_model(manifest)
    out = next(m for m in result.metrics if m.name == "count_valid_values_metric")
    assert out.type_params.metric_aggregation_params is not None
    assert (
        out.type_params.metric_aggregation_params.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert (
        out.type_params.expr == "CASE WHEN is_valid IS NOT NULL THEN 1 ELSE 0 END"
    ), "Expression should have been changed, but was not (or was changed incorrectly)."


def test_metric_convert_count_rule_transforms_across_multiple_metrics() -> None:
    """Transform across multiple simple metrics, changing COUNT-with-condition and leaving others unchanged.

    This is meant as a larger smoke test, and verifies that the behaviors work
    across multiple metrics since other tests are focused on one metric at a time.

    When debugging, if other tests in this file are failing, start on the other tests first.
    """
    semantic_model = get_empty_semantic_model()

    metric_count_condition = PydanticMetric(
        name="count_with_condition_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.COUNT,
            ),
            expr="status",
        ),
    )
    metric_count_all = PydanticMetric(
        name="count_all_rows_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.COUNT,
            ),
            expr="1",
        ),
    )
    metric_non_count = PydanticMetric(
        name="non_count_metric",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="example_model",
                agg=AggregationType.AVERAGE,
            ),
            expr="price",
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[metric_count_condition, metric_count_all, metric_non_count],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountMetricToSumRule.transform_model(manifest)

    out_condition = next(m for m in result.metrics if m.name == "count_with_condition_metric")
    assert out_condition.type_params.metric_aggregation_params is not None
    assert (
        out_condition.type_params.metric_aggregation_params.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert (
        out_condition.type_params.expr == "CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END"
    ), "Expression should have been changed, but was not (or was changed incorrectly)."

    out_all = next(m for m in result.metrics if m.name == "count_all_rows_metric")
    assert out_all.type_params.metric_aggregation_params is not None
    assert (
        out_all.type_params.metric_aggregation_params.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert out_all.type_params.expr == "1", "Expression should NOT have been changed, but was."

    out_non_count = next(m for m in result.metrics if m.name == "non_count_metric")
    assert out_non_count.type_params.metric_aggregation_params is not None
    assert (
        out_non_count.type_params.metric_aggregation_params.agg == AggregationType.AVERAGE
    ), "Aggregation type should not have been changed."
    assert out_non_count.type_params.expr == "price", "Expression should not have been changed."


# ==================================================================================
# Legacy measure convert count rule
# ==================================================================================


def test_legacy_measure_convert_count_rule_does_not_change_non_count_measures() -> None:
    """If a measure does not have agg type COUNT, it remains unchanged."""
    original_measure = PydanticMeasure(name="non_count_measure", agg=AggregationType.AVERAGE, expr="revenue")
    semantic_model = get_empty_semantic_model()
    semantic_model.measures = [original_measure.copy(deep=True)]

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountToSumRule.transform_model(manifest)
    out_model = result.semantic_models[0]
    non_count_measure = next(m for m in out_model.measures if m.name == "non_count_measure")

    assert non_count_measure == original_measure, "Measure should not have been changed, but was."


def test_legacy_measure_convert_count_rule_raises_when_count_without_expr() -> None:
    """Measure with agg type COUNT without expr should raise the expected error."""
    semantic_model = get_empty_semantic_model()
    semantic_model.measures = [PydanticMeasure(name="missing_expr_count", agg=AggregationType.COUNT)]

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    with pytest.raises(
        ModelTransformError,
        match="uses a COUNT aggregation, which requires an expr to be provided",
    ):
        ConvertCountToSumRule.transform_model(manifest)


def test_legacy_measure_convert_count_rule_leaves_expr_one_but_sets_sum() -> None:
    """Measure with agg type COUNT and expr '1' should keep expr as '1' but set agg to SUM."""
    semantic_model = get_empty_semantic_model()
    semantic_model.measures = [PydanticMeasure(name="count_all_rows", agg=AggregationType.COUNT, expr="1")]

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountToSumRule.transform_model(manifest)
    out_model = result.semantic_models[0]
    count_all_rows = next(m for m in out_model.measures if m.name == "count_all_rows")

    assert count_all_rows.agg == AggregationType.SUM, "Aggregation type should have been changed to SUM, but was not."
    assert count_all_rows.expr == "1", "Expression should NOT have been changed, but was."


def test_legacy_measure_convert_count_rule_transforms_count_with_expr_to_case_expression() -> None:
    """Measure with agg type COUNT and expr other than '1' should wrap expr and set agg to SUM."""
    semantic_model = get_empty_semantic_model()
    semantic_model.measures = [PydanticMeasure(name="count_valid_values", agg=AggregationType.COUNT, expr="is_valid")]

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountToSumRule.transform_model(manifest)
    out_model = result.semantic_models[0]
    count_valid_values = next(m for m in out_model.measures if m.name == "count_valid_values")

    assert (
        count_valid_values.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert (
        count_valid_values.expr == "CASE WHEN is_valid IS NOT NULL THEN 1 ELSE 0 END"
    ), "Expression should have been changed, but was not (or was changed incorrectly)."


def test_legacy_measure_convert_count_rule_transforms_across_multiple_models() -> None:
    """Transform across multiple models, changing one COUNT-with-condition per model and leaving others unchanged.

    This is meant as a larger smoke test, and verifies that the behaviors work
    across multiple models and measures since other tests are focused on one measure at a time.

    When debugging, if other tests in this file are failing, start on the other tests first.
    """
    original_unchanged_metric_one = PydanticMeasure(
        name="non_count_model_one", agg=AggregationType.AVERAGE, expr="revenue"
    )
    semantic_model_one = PydanticSemanticModel(
        name="model_one",
        node_relation=PydanticNodeRelation(alias="model_one", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id_one", type=EntityType.PRIMARY)],
        measures=[
            # Will be transformed
            PydanticMeasure(name="count_with_condition_model_one", agg=AggregationType.COUNT, expr="status"),
            # Will remain the same expression but agg will become SUM
            PydanticMeasure(name="count_all_rows_model_one", agg=AggregationType.COUNT, expr="1"),
            # Will be entirely unchanged
            original_unchanged_metric_one.copy(deep=True),
        ],
    )

    original_unchanged_metric_two = PydanticMeasure(
        name="non_count_model_two", agg=AggregationType.MEDIAN, expr="price"
    )
    semantic_model_two = PydanticSemanticModel(
        name="model_two",
        node_relation=PydanticNodeRelation(alias="model_two", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id_two", type=EntityType.PRIMARY)],
        measures=[
            # Will be transformed
            PydanticMeasure(name="count_with_condition_model_two", agg=AggregationType.COUNT, expr="category"),
            # Will remain the same expression but agg will become SUM
            PydanticMeasure(name="count_all_rows_model_two", agg=AggregationType.COUNT, expr="1"),
            # Will be entirely unchanged
            original_unchanged_metric_two.copy(deep=True),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model_one, semantic_model_two],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountToSumRule.transform_model(manifest)

    # Model one checks
    out_model_one = next(sm for sm in result.semantic_models if sm.name == "model_one")

    transformed_measure_one = next(m for m in out_model_one.measures if m.name == "count_with_condition_model_one")
    assert transformed_measure_one.agg == AggregationType.SUM, "Aggregation type should have been changed, but was not."
    assert (
        transformed_measure_one.expr == "CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END"
    ), "Expression should have been changed, but was not (or was changed incorrectly)."

    unchanged_count_one = next(m for m in out_model_one.measures if m.name == "count_all_rows_model_one")
    assert (
        unchanged_count_one.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert unchanged_count_one.expr == "1", "Expression should not have been changed, but was."

    unchanged_non_count_one = next(m for m in out_model_one.measures if m.name == "non_count_model_one")
    assert unchanged_non_count_one == original_unchanged_metric_one, "Metric should not have been changed, but was."

    # Model two checks
    out_model_two = next(sm for sm in result.semantic_models if sm.name == "model_two")

    transformed_measure_two = next(m for m in out_model_two.measures if m.name == "count_with_condition_model_two")
    assert (
        transformed_measure_two.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert (
        transformed_measure_two.expr == "CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END"
    ), "Expression should have been changed, but was not (or was changed incorrectly)."

    unchanged_count_two = next(m for m in out_model_two.measures if m.name == "count_all_rows_model_two")
    assert (
        unchanged_count_two.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert unchanged_count_two.expr == "1", "Expression should not have been changed, but was."

    unchanged_non_count_two = next(m for m in out_model_two.measures if m.name == "non_count_model_two")
    assert unchanged_non_count_two == original_unchanged_metric_two, "Metric should not have been changed, but was."
