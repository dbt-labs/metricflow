from __future__ import annotations

from typing import List, Optional

import pytest
from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticConversionTypeParams,
    PydanticCumulativeTypeParams,
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInput,
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
from metricflow_semantic_interfaces.transformations.replace_input_measures_with_simple_metrics_transformation import (
    ReplaceInputMeasuresWithSimpleMetricsTransformationRule,
)
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)


def _project_config() -> PydanticProjectConfiguration:
    return PydanticProjectConfiguration()


def _build_semantic_model_with_measure(
    sm_name: str,
    measure_name: str,
    time_dim_name: str = "ds",
) -> PydanticSemanticModel:
    return PydanticSemanticModel(
        name=sm_name,
        node_relation=PydanticNodeRelation(alias=sm_name, schema_name="schema"),
        entities=[PydanticEntity(name="e1", type=EntityType.PRIMARY)],
        dimensions=[
            PydanticDimension(
                name=time_dim_name,
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            )
        ],
        measures=[
            PydanticMeasure(
                name=measure_name,
                agg=AggregationType.SUM,
                agg_time_dimension=time_dim_name,
                expr="value",
            )
        ],
    )


def _build_simple_metric(
    name: str,
    sm_name: str,
    time_dim_name: str,
    fill_nulls_with: Optional[int],
    join_to_timespine: bool,
) -> PydanticMetric:
    return PydanticMetric(
        name=name,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model=sm_name,
                agg=AggregationType.SUM,
                agg_time_dimension=time_dim_name,
            ),
            expr="value",
            join_to_timespine=join_to_timespine,
            fill_nulls_with=fill_nulls_with,
        ),
        description=None,
        label=None,
        config=None,
    )


def test_cumulative_no_measure_with_metric_input_is_unchanged() -> None:
    """If only a metric input is provided on cumulative, no changes are made."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    cumulative_metric = PydanticMetric(
        name="cumulative_metric",
        type=MetricType.CUMULATIVE,
        type_params=PydanticMetricTypeParams(
            cumulative_type_params=PydanticCumulativeTypeParams(metric=PydanticMetricInput(name="preexisting_simple"))
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm], metrics=[cumulative_metric], project_configuration=_project_config()
    )
    original_metrics = manifest.copy(deep=True).metrics
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    assert out.metrics == original_metrics


def test_cumulative_with_measure_and_metric_input_is_unchanged() -> None:
    """If both measure and metric inputs are provided, no changes are made."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    cumulative_metric = PydanticMetric(
        name="cumulative_metric",
        type=MetricType.CUMULATIVE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(name="m1", fill_nulls_with=5, join_to_timespine=True),
            cumulative_type_params=PydanticCumulativeTypeParams(metric=PydanticMetricInput(name="preexisting_simple")),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[cumulative_metric],
        project_configuration=_project_config(),
    )

    original_metrics = manifest.copy(deep=True).metrics
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    assert out.metrics == original_metrics


def test_cumulative_with_measure_reuses_existing_simple_metric() -> None:
    """With a preexisting matching simple metric, reuse it without creating a new one."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    cumulative_metric = PydanticMetric(
        name="cumulative_metric",
        type=MetricType.CUMULATIVE,
        type_params=PydanticMetricTypeParams(
            cumulative_type_params=PydanticCumulativeTypeParams(),
            measure=PydanticMetricInputMeasure(name="m1", fill_nulls_with=5, join_to_timespine=True),
        ),
    )

    existing_simple_metric = _build_simple_metric(
        name="existing_simple_for_m1",
        sm_name="sm",
        time_dim_name="ds",
        fill_nulls_with=5,
        join_to_timespine=True,
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[cumulative_metric, existing_simple_metric],
        project_configuration=_project_config(),
    )

    initial_simple_metric_count = len([m for m in manifest.metrics if m.type == MetricType.SIMPLE])
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    post_simple_metric_count = len([m for m in out.metrics if m.type == MetricType.SIMPLE])

    assert post_simple_metric_count == initial_simple_metric_count

    out_metric = next(m for m in out.metrics if m.type == MetricType.CUMULATIVE)
    assert (
        out_metric.type_params.cumulative_type_params is not None
        and out_metric.type_params.cumulative_type_params.metric is not None
    ), "cumulative_type_params should be set as part of the test setup here."
    assert out_metric.type_params.cumulative_type_params.metric.name == "existing_simple_for_m1"


def test_cumulative_with_measure_creates_one_for_multiple_metrics() -> None:
    """With two cumulative metrics, create a single shared simple metric when none exists."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    metrics: List[PydanticMetric] = []
    for i in range(2):
        metrics.append(
            PydanticMetric(
                name=f"cumulative_metric_{i}",
                type=MetricType.CUMULATIVE,
                type_params=PydanticMetricTypeParams(
                    cumulative_type_params=PydanticCumulativeTypeParams(),
                    measure=PydanticMetricInputMeasure(name="m1", fill_nulls_with=5, join_to_timespine=True),
                ),
            )
        )

    manifest = PydanticSemanticManifest(semantic_models=[sm], metrics=metrics, project_configuration=_project_config())

    initial_simple_metric_count = len([m for m in manifest.metrics if m.type == MetricType.SIMPLE])
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    post_simple_metric_count = len([m for m in out.metrics if m.type == MetricType.SIMPLE])

    assert post_simple_metric_count == initial_simple_metric_count + 1
    private_metric_count = len([m for m in out.metrics if m.type == MetricType.SIMPLE and m.type_params.is_private])
    assert private_metric_count == 1

    names = []
    for m in out.metrics:
        if m.type != MetricType.CUMULATIVE:
            continue
        # ughhh ugly type assertion here.
        assert (
            m.type_params.cumulative_type_params is not None and m.type_params.cumulative_type_params.metric is not None
        ), "cumulative_type_params should be set as part of the test setup here."
        names.append(m.type_params.cumulative_type_params.metric.name)
    assert len(set(names)) == 1


def test_repeated_cumulative_measure_inputs_do_not_create_dulplicates_metrics() -> None:
    """Start with one simple metric; grouped cumulative measure inputs create four new simple metrics.

    Groups:
    - Group 1: two cumulative metrics with no fill/join settings
    - Group 2: one with fill_nulls_with only
    - Group 3: one with join_to_timespine only
    - Group 4: two with both fill_nulls_with and join_to_timespine

    Validate:
    - Exactly four new simple metrics are added
    - Group 1 metrics share the same created metric input
    - Group 4 metrics share the same created metric input
    """
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    # Existing simple metric (for a different configuration) to ensure we add 4 new ones
    existing_simple_metric = PydanticMetric(
        name="existing_unrelated_simple",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="sm",
                agg=AggregationType.SUM,
                agg_params=None,
                agg_time_dimension="ds",
                non_additive_dimension=None,
            ),
            # Different expr than the measure in sm to avoid accidental equivalence
            expr="other_value",
            join_to_timespine=False,
            fill_nulls_with=None,
        ),
    )

    metrics: List[PydanticMetric] = [existing_simple_metric]

    # Group 1: two cumulative metrics with default measure settings
    g1_input = PydanticMetricInputMeasure(name="m1", fill_nulls_with=None, join_to_timespine=False)
    for i in range(2):
        metrics.append(
            PydanticMetric(
                name=f"cumulative_g1_{i}",
                type=MetricType.CUMULATIVE,
                type_params=PydanticMetricTypeParams(
                    cumulative_type_params=PydanticCumulativeTypeParams(),
                    measure=g1_input,
                ),
            )
        )

    # Group 2: one cumulative metric with fill only
    g2_input = PydanticMetricInputMeasure(name="m1", fill_nulls_with=3, join_to_timespine=False)
    metrics.append(
        PydanticMetric(
            name="cumulative_g2",
            type=MetricType.CUMULATIVE,
            type_params=PydanticMetricTypeParams(
                cumulative_type_params=PydanticCumulativeTypeParams(),
                measure=g2_input,
            ),
        )
    )

    # Group 3: one cumulative metric with join only
    g3_input = PydanticMetricInputMeasure(name="m1", fill_nulls_with=None, join_to_timespine=True)
    metrics.append(
        PydanticMetric(
            name="cumulative_g3",
            type=MetricType.CUMULATIVE,
            type_params=PydanticMetricTypeParams(
                cumulative_type_params=PydanticCumulativeTypeParams(),
                measure=g3_input,
            ),
        )
    )

    # Group 4: two cumulative metrics with both fill and join
    g4_input = PydanticMetricInputMeasure(name="m1", fill_nulls_with=9, join_to_timespine=True)
    for i in range(2):
        metrics.append(
            PydanticMetric(
                name=f"cumulative_g4_{i}",
                type=MetricType.CUMULATIVE,
                type_params=PydanticMetricTypeParams(
                    cumulative_type_params=PydanticCumulativeTypeParams(),
                    measure=g4_input,
                ),
            )
        )

    manifest = PydanticSemanticManifest(semantic_models=[sm], metrics=metrics, project_configuration=_project_config())

    initial_simple_metric_count = len([m for m in manifest.metrics if m.type == MetricType.SIMPLE])
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    post_simple_metric_count = len([m for m in out.metrics if m.type == MetricType.SIMPLE])

    # Expect 4 new simple metrics (one per distinct measure config)
    assert post_simple_metric_count == initial_simple_metric_count + 4

    # Collect created metric input names for each group
    def _get_metric_input_name_from_output_name(metric_name: str) -> str:
        m = next(x for x in out.metrics if x.name == metric_name)
        assert (
            m.type_params.cumulative_type_params is not None and m.type_params.cumulative_type_params.metric is not None
        ), "cumulative_type_params should be set as part of the test setup here."
        return m.type_params.cumulative_type_params.metric.name

    g1_names = [
        _get_metric_input_name_from_output_name("cumulative_g1_0"),
        _get_metric_input_name_from_output_name("cumulative_g1_1"),
    ]
    assert len(set(g1_names)) == 1, "Group 1 metrics were not successfully deduplicated."
    assert g1_names[0] == "m1"
    g2_names = [_get_metric_input_name_from_output_name("cumulative_g2")]
    assert len(set(g2_names)) == 1, "Group 2 metrics were not successfully deduplicated."
    assert g2_names[0] == "m1_fill_nulls_with_3"
    g3_names = [_get_metric_input_name_from_output_name("cumulative_g3")]
    assert len(set(g3_names)) == 1, "Group 3 metrics were not successfully deduplicated."
    assert g3_names[0] == "m1_join_to_timespine"
    g4_names = [
        _get_metric_input_name_from_output_name("cumulative_g4_0"),
        _get_metric_input_name_from_output_name("cumulative_g4_1"),
    ]
    assert len(set(g4_names)) == 1, "Group 4 metrics were not successfully deduplicated."
    assert g4_names[0] == "m1_fill_nulls_with_9_join_to_timespine"


@pytest.mark.parametrize(
    "join_to_timespine,fill_nulls_with,expected_metric_name",
    [
        (True, None, "m1_join_to_timespine"),
        (False, 12, "m1_fill_nulls_with_12"),
        (True, -10, "m1_fill_nulls_with_neg_10_join_to_timespine"),
        (False, None, "m1"),
    ],
)
def test_cumulative_measure_input_features_reflected_in_created_simple_metric(
    join_to_timespine: bool, fill_nulls_with: Optional[int], expected_metric_name: str
) -> None:
    """Created simple metric reflects measure input features (join_to_timespine, fill_nulls_with)."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    cumulative_metric = PydanticMetric(
        name="cumulative_metric",
        type=MetricType.CUMULATIVE,
        type_params=PydanticMetricTypeParams(
            cumulative_type_params=PydanticCumulativeTypeParams(),
            measure=PydanticMetricInputMeasure(
                name="m1",
                fill_nulls_with=fill_nulls_with,
                join_to_timespine=join_to_timespine,
            ),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm], metrics=[cumulative_metric], project_configuration=_project_config()
    )
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)

    # Find created simple metric by expected name
    created = next(m for m in out.metrics if m.type == MetricType.SIMPLE and m.name == expected_metric_name)

    # Non-filter measure input fields applied appropriately
    if fill_nulls_with is not None:
        assert created.type_params.fill_nulls_with == fill_nulls_with
    else:
        assert created.type_params.fill_nulls_with is None
    if join_to_timespine:
        assert created.type_params.join_to_timespine is True
    else:
        assert created.type_params.join_to_timespine is False

    # Aggregation params and expr copied from measure
    assert created.type_params.metric_aggregation_params is not None
    assert created.type_params.metric_aggregation_params.semantic_model == "sm"
    assert created.type_params.metric_aggregation_params.agg == AggregationType.SUM
    assert created.type_params.metric_aggregation_params.agg_time_dimension == "ds"
    assert created.type_params.expr == "value"

    # Cumulative metric should point to the created metric
    out_cumulative = next(m for m in out.metrics if m.name == "cumulative_metric")
    assert out_cumulative.type_params.cumulative_type_params is not None
    assert out_cumulative.type_params.cumulative_type_params.metric is not None
    assert out_cumulative.type_params.cumulative_type_params.metric.name == expected_metric_name


@pytest.mark.parametrize(
    "has_measure_filter,has_metric_filter",
    [
        (True, False),
        (False, True),
        (True, True),
        (False, False),
    ],
)
def test_cumulative_input_filters_merge_onto_metric_input_and_created_simple_metric(
    has_measure_filter: bool,
    has_metric_filter: bool,
) -> None:
    """Filters from cumulative and measure input should be merged onto created metric and preserved in input."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

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

    cumulative_metric = PydanticMetric(
        name="cumulative_metric",
        type=MetricType.CUMULATIVE,
        filter=metric_filter,
        type_params=PydanticMetricTypeParams(
            cumulative_type_params=PydanticCumulativeTypeParams(),
            measure=PydanticMetricInputMeasure(name="m1", filter=measure_filter),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm], metrics=[cumulative_metric], project_configuration=_project_config()
    )
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)

    out_cumulative = next(m for m in out.metrics if m.name == "cumulative_metric")
    assert out_cumulative.type_params.cumulative_type_params is not None
    assert out_cumulative.type_params.cumulative_type_params.metric is not None
    metric_input = out_cumulative.type_params.cumulative_type_params.metric

    # Created simple metric
    created = next(m for m in out.metrics if m.type == MetricType.SIMPLE and m.name == "m1")

    # The created metric should have no filters on it.
    assert created.filter is None

    # The cumulative metric's input metric should have the filter that used to be on the measure input.
    if has_measure_filter:
        metric_input = out_cumulative.type_params.cumulative_type_params.metric
        error_message = "Expected to retain filter from measure input on the new metric input field."
        assert metric_input.filter is not None, error_message
        assert (
            measure_filter is not None
        ), "Something went wrong with the test setup; this object should never be None here."
        assert metric_input.filter.where_filters == measure_filter.where_filters, error_message
    else:
        assert (
            metric_input.filter is None
        ), "Filters should not be added to the metric input if the existing measure input field had none."

    assert (
        out_cumulative.filter == metric_filter
    ), "The cumulative metric's filter should be unchanged as it is irrelevant to the transformation."

    # The metric input on cumulative points to created metric name
    assert metric_input.name == "m1"


@pytest.mark.parametrize(
    "measure_expr,expected_expr",
    [
        ("value", "value"),
        (None, "m1"),
    ],
)
def test_cumulative_expr_in_created_simple_metric(measure_expr: Optional[str], expected_expr: str) -> None:
    """Expr on created simple metric comes from measure.expr or falls back to measure name."""
    # Build model overriding measure expr if provided
    sm = PydanticSemanticModel(
        name="sm",
        node_relation=PydanticNodeRelation(alias="sm", schema_name="schema"),
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
                expr=measure_expr,
            )
        ],
    )

    cumulative_metric = PydanticMetric(
        name="cumulative_metric",
        type=MetricType.CUMULATIVE,
        type_params=PydanticMetricTypeParams(
            cumulative_type_params=PydanticCumulativeTypeParams(),
            measure=PydanticMetricInputMeasure(name="m1"),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm], metrics=[cumulative_metric], project_configuration=_project_config()
    )
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)

    # Created simple metric
    created = next(m for m in out.metrics if m.type == MetricType.SIMPLE and m.name == "m1")
    assert created.type_params.expr == expected_expr


def test_cumulative_metric_name_collision_creates_unique_metric() -> None:
    """Test that a new simple metric is created with a unique name when a name collision occurs but features differ."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    # Pre-existing simple metric with name "m1" and default features (no fill_nulls_with, no join_to_timespine)
    preexisting_simple_metric = PydanticMetric(
        name="m1",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="sm",
                agg=AggregationType.COUNT,
                agg_time_dimension="ds",
            ),
            expr="1",
            join_to_timespine=False,
            fill_nulls_with=None,
        ),
    )

    # Cumulative metric that references a measure with the same name, but with fill_nulls_with set
    cumulative_metric = PydanticMetric(
        name="cumulative_with_fill",
        type=MetricType.CUMULATIVE,
        type_params=PydanticMetricTypeParams(
            cumulative_type_params=PydanticCumulativeTypeParams(),
            measure=PydanticMetricInputMeasure(name="m1"),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[preexisting_simple_metric, cumulative_metric],
        project_configuration=_project_config(),
    )

    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)

    # There should be two simple metrics: the original and a new one with a unique name
    simple_metrics = [m for m in out.metrics if m.type == MetricType.SIMPLE]
    simple_metric_names = {m.name for m in simple_metrics}
    assert "m1" in simple_metric_names
    # The new metric should have an incremented unique name
    expected_new_metric_name = "m1_1"
    assert expected_new_metric_name in simple_metric_names
    assert next(
        m for m in simple_metrics if m.name == expected_new_metric_name
    ).type_params.is_private, "Newly created metrics should be private."

    # The cumulative metric should reference the new metric by name
    cum_metric = next(m for m in out.metrics if m.name == "cumulative_with_fill")
    assert (
        cum_metric.type_params.cumulative_type_params is not None
        and cum_metric.type_params.cumulative_type_params.metric is not None
    )
    assert cum_metric.type_params.cumulative_type_params.metric.name == expected_new_metric_name


@pytest.mark.parametrize("side", ["base", "conversion"])
def test_conversion_no_measure_with_metric_input_is_unchanged(side: str) -> None:
    """If only a metric input is provided on conversion, no changes are made."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    conversion_metric = PydanticMetric(
        name=f"conversion_{side}",
        type=MetricType.CONVERSION,
        type_params=PydanticMetricTypeParams(
            conversion_type_params=(
                PydanticConversionTypeParams(
                    entity="e1",
                    base_metric=PydanticMetricInput(name="preexisting_simple"),
                )
                if side == "base"
                else PydanticConversionTypeParams(
                    entity="e1",
                    conversion_metric=PydanticMetricInput(name="preexisting_simple"),
                )
            ),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[conversion_metric],
        project_configuration=_project_config(),
    )

    original_metrics = manifest.copy(deep=True).metrics
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    assert out.metrics == original_metrics


@pytest.mark.parametrize("side", ["base", "conversion"])
def test_conversion_with_measure_and_metric_input_is_unchanged(side: str) -> None:
    """If both measure and metric inputs are provided, no changes are made."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    conversion_metric = PydanticMetric(
        name=f"conversion_{side}",
        type=MetricType.CONVERSION,
        type_params=PydanticMetricTypeParams(
            conversion_type_params=(
                PydanticConversionTypeParams(
                    entity="e1",
                    base_measure=PydanticMetricInputMeasure(name="m1", fill_nulls_with=7, join_to_timespine=True),
                    base_metric=PydanticMetricInput(name="preexisting_simple"),
                )
                if side == "base"
                else PydanticConversionTypeParams(
                    entity="e1",
                    conversion_measure=PydanticMetricInputMeasure(name="m1", fill_nulls_with=7, join_to_timespine=True),
                    conversion_metric=PydanticMetricInput(name="preexisting_simple"),
                )
            ),
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[conversion_metric],
        project_configuration=_project_config(),
    )

    original_metrics = manifest.copy(deep=True).metrics
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    assert out.metrics == original_metrics


@pytest.mark.parametrize("side", ["base", "conversion"])
def test_conversion_with_measure_reuses_existing_simple_metric(side: str) -> None:
    """With a preexisting matching simple metric, reuse it without creating a new one."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    conversion_metric = PydanticMetric(
        name=f"conversion_{side}",
        type=MetricType.CONVERSION,
        type_params=PydanticMetricTypeParams(
            conversion_type_params=(
                PydanticConversionTypeParams(
                    entity="e1",
                    base_measure=PydanticMetricInputMeasure(name="m1", fill_nulls_with=7, join_to_timespine=True),
                )
                if side == "base"
                else PydanticConversionTypeParams(
                    entity="e1",
                    conversion_measure=PydanticMetricInputMeasure(name="m1", fill_nulls_with=7, join_to_timespine=True),
                )
            ),
        ),
    )

    existing_simple_metric = _build_simple_metric(
        name="existing_simple_for_m1",
        sm_name="sm",
        time_dim_name="ds",
        fill_nulls_with=7,
        join_to_timespine=True,
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[conversion_metric, existing_simple_metric],
        project_configuration=_project_config(),
    )

    initial_simple_metrics = [m.copy(deep=True) for m in manifest.metrics if m.type == MetricType.SIMPLE]
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    # post_simple_metric_count = len([m for m in out.metrics if m.type == MetricType.SIMPLE])
    # assert post_simple_metric_count == initial_simple_metric_count
    out_simple_metrics = [m.copy(deep=True) for m in out.metrics if m.type == MetricType.SIMPLE]
    assert initial_simple_metrics == out_simple_metrics

    out_params = next(m for m in out.metrics if m.type == MetricType.CONVERSION).type_params.conversion_type_params
    assert out_params is not None, "no conversion metric found; this is a fundamental problem."
    if side == "base":
        assert out_params.base_metric is not None
        chosen_name = out_params.base_metric.name
    else:
        assert out_params.conversion_metric is not None
        chosen_name = out_params.conversion_metric.name
    assert chosen_name == "existing_simple_for_m1"


@pytest.mark.parametrize("side", ["base", "conversion"])
def test_conversion_with_measure_creates_one_for_multiple_metrics(side: str) -> None:
    """With two conversion metrics, create a single shared simple metric when none exists."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    metrics: List[PydanticMetric] = []
    for i in range(2):
        metrics.append(
            PydanticMetric(
                name=f"conversion_{side}_{i}",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=(
                        PydanticConversionTypeParams(
                            entity="e1",
                            base_measure=PydanticMetricInputMeasure(
                                name="m1", fill_nulls_with=7, join_to_timespine=True
                            ),
                        )
                        if side == "base"
                        else PydanticConversionTypeParams(
                            entity="e1",
                            conversion_measure=PydanticMetricInputMeasure(
                                name="m1", fill_nulls_with=7, join_to_timespine=True
                            ),
                        )
                    ),
                ),
            )
        )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=metrics,
        project_configuration=_project_config(),
    )

    initial_simple_metrics = [m for m in manifest.metrics if m.type == MetricType.SIMPLE]
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    post_simple_metrics = [m for m in out.metrics if m.type == MetricType.SIMPLE]

    assert len(post_simple_metrics) == len(initial_simple_metrics) + 1
    assert next(
        m for m in post_simple_metrics if m.name not in initial_simple_metrics
    ).type_params.is_private, "Newly created metrics should be private."

    names = []
    for m in out.metrics:
        if m.type != MetricType.CONVERSION:
            continue
        assert m.type_params.conversion_type_params is not None, "Conversion metric lacked conversion type params."
        params = m.type_params.conversion_type_params
        if side == "base":
            assert params.base_metric is not None
            names.append(params.base_metric.name)
        else:
            assert params.conversion_metric is not None
            names.append(params.conversion_metric.name)
    assert len(set(names)) == 1
