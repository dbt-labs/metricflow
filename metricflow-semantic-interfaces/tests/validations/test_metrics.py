from __future__ import annotations

from typing import Optional

import pytest
from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import (
    PydanticMeasure,
    PydanticMeasureAggregationParameters,
    PydanticNonAdditiveDimensionParameters,
)
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticCumulativeTypeParams,
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricTimeWindow,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    TimeDimensionReference,
)
from metricflow_semantic_interfaces.test_utils import (
    metric_with_guaranteed_meta,
    semantic_model_with_guaranteed_meta,
)
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    PeriodAggregation,
    TimeGranularity,
)
from metricflow_semantic_interfaces.validations.metrics import (
    CumulativeMetricRule,
    DerivedMetricRule,
    MetricAggregationParamsInForSimpleMetricsRule,
    MetricsCountAggregationExprRule,
    MetricsNonAdditiveDimensionsRule,
    MetricsPercentileAggregationRule,
    MetricTimeGranularityRule,
)
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)

from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION
from tests.validations.validation_test_utils import check_error_in_issues


@pytest.mark.parametrize(
    "metric, error_substring",
    [
        (
            metric_with_guaranteed_meta(
                name="metric_with_an_invalid_non_additive_dimension_name",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        agg=AggregationType.SUM,
                        semantic_model="sum_measure2",
                        non_additive_dimension=PydanticNonAdditiveDimensionParameters(
                            name="this_name_does_not_exist",
                            window_choice=AggregationType.MIN,
                            window_groupings=["ds"],
                        ),
                        agg_time_dimension="time_dim",
                    ),
                ),
            ),
            "that is not defined as a dimension in semantic model 'sum_measure2'.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_invalid_window_groupings",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        agg=AggregationType.SUM,
                        semantic_model="sum_measure2",
                        non_additive_dimension=PydanticNonAdditiveDimensionParameters(
                            name="country",
                            window_choice=AggregationType.MIN,
                            window_groupings=["bloopbloopiamnotrealohhhhnooooooooo"],
                        ),
                        agg_time_dimension="time_dim",
                    ),
                ),
            ),
            "has a non_additive_dimension with an invalid 'window_groupings'",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_a_categorical_non_additive_dimension_name",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        agg=AggregationType.SUM,
                        semantic_model="sum_measure2",
                        non_additive_dimension=PydanticNonAdditiveDimensionParameters(
                            name="country",
                            window_choice=AggregationType.MIN,
                        ),
                        agg_time_dimension="time_dim",
                    ),
                ),
            ),
            "that is defined as a categorical dimension which is not supported.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_a_non_additive_dimension_name_with_mismatched_time_granularity",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        agg=AggregationType.SUM,
                        semantic_model="sum_measure2",
                        non_additive_dimension=PydanticNonAdditiveDimensionParameters(
                            name="weekly_dim",
                            window_choice=AggregationType.MIN,
                        ),
                        agg_time_dimension="time_dim",
                    ),
                ),
            ),
            "base time granularity (WEEK) that is not equal to the metric's "
            "agg_time_dimension time_dim with a base granularity of (DAY)",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_a_bogus_semantic_model",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        agg=AggregationType.SUM,
                        semantic_model="i_am_missing",
                        non_additive_dimension=PydanticNonAdditiveDimensionParameters(
                            name="weekly_dim",
                            window_choice=AggregationType.MIN,
                        ),
                        agg_time_dimension="time_dim",
                    ),
                ),
            ),
            "Metric 'metric_with_a_bogus_semantic_model' references semantic model "
            "'i_am_missing', but that semantic model could not be found.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_valid_non_additive_dimension",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        agg=AggregationType.SUM,
                        semantic_model="sum_measure2",
                        non_additive_dimension=PydanticNonAdditiveDimensionParameters(
                            name="second_time_dim",
                            window_choice=AggregationType.MIN,
                        ),
                        agg_time_dimension="time_dim",
                    ),
                ),
            ),
            None,
        ),
    ],
)
def test_simple_metrics_non_additive_dimension(  # noqa: D103
    metric: PydanticMetric,
    error_substring: Optional[str],
) -> None:
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([MetricsNonAdditiveDimensionsRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure2",
                    measures=[
                        PydanticMeasure(
                            name="this_measure_name",
                            agg=AggregationType.SUM,
                            agg_time_dimension="ename",
                        )
                    ],
                    dimensions=[
                        PydanticDimension(name="country", type=DimensionType.CATEGORICAL),
                        PydanticDimension(
                            name="time_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                        PydanticDimension(
                            name="weekly_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.WEEK,
                            ),
                        ),
                        PydanticDimension(
                            name="second_time_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                    ],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
            ],
            metrics=[metric],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    if error_substring is not None:
        check_error_in_issues(error_substrings=[error_substring], issues=validation_results.all_issues)
    else:
        assert len(validation_results.all_issues) == 0, "Expected no issues, but found validation issues: " + str(
            validation_results.all_issues
        )


@pytest.mark.parametrize(
    "metric, error_substring",
    [
        (
            metric_with_guaranteed_meta(
                name="metric_with_no_expr",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.COUNT,
                    ),
                ),
            ),
            "uses a COUNT aggregation, which requires an expr to be provided. Provide "
            "'expr: 1' if a count of all rows is desired.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_distinct_expr",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.COUNT,
                    ),
                    expr="distinct 1",
                ),
            ),
            "uses a 'count' aggregation with a DISTINCT expr: 'distinct 1'. This is not supported as it "
            "effectively converts an additive metric into a non-additive one, and this could cause certain "
            "queries to return incorrect results. Please use the count_distinct aggregation type",
        ),
    ],
)
def test_simple_metrics_expr_for_count_aggregation(  # noqa: D103
    metric: PydanticMetric,
    error_substring: str,
) -> None:
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([MetricsCountAggregationExprRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure2",
                    measures=[
                        PydanticMeasure(
                            name="this_measure_name",
                            agg=AggregationType.SUM,
                            agg_time_dimension="ename",
                        )
                    ],
                    dimensions=[
                        PydanticDimension(name="country", type=DimensionType.CATEGORICAL),
                        PydanticDimension(
                            name="time_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                        PydanticDimension(
                            name="weekly_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.WEEK,
                            ),
                        ),
                    ],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
            ],
            metrics=[metric],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    check_error_in_issues(error_substrings=[error_substring], issues=validation_results.all_issues)


@pytest.mark.parametrize(
    "metric, error_substring",
    [
        (
            metric_with_guaranteed_meta(
                name="metric_with_no_percentile",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.PERCENTILE,
                    ),
                ),
            ),
            "uses a PERCENTILE aggregation, which requires agg_params.percentile to be provided.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_percentile_too_large",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.PERCENTILE,
                        agg_params=PydanticMeasureAggregationParameters(
                            percentile=1.1,
                        ),
                    ),
                ),
            ),
            "Percentile aggregation parameter for metric 'metric_with_percentile_too_large' is '1.1', but must "
            "be between 0 and 1 (non-inclusive). For example, to indicate the 65th percentile value, set 'percentile: "
            "0.65'. For percentile values of 0, please use MIN, for percentile values of 1, please use MAX.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_percentile_too_small",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.PERCENTILE,
                        agg_params=PydanticMeasureAggregationParameters(
                            percentile=-0.1,
                        ),
                    ),
                ),
            ),
            "Percentile aggregation parameter for metric 'metric_with_percentile_too_small' is '-0.1', but must "
            "be between 0 and 1 (non-inclusive). For example, to indicate the 65th percentile value, set 'percentile: "
            "0.65'. For percentile values of 0, please use MIN, for percentile values of 1, please use MAX.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_percentile_0_is_not_included",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.PERCENTILE,
                        agg_params=PydanticMeasureAggregationParameters(
                            percentile=0,
                        ),
                    ),
                ),
            ),
            "Percentile aggregation parameter for metric 'metric_with_percentile_0_is_not_included' is '0.0', but must "
            "be between 0 and 1 (non-inclusive). For example, to indicate the 65th percentile value, set 'percentile: "
            "0.65'. For percentile values of 0, please use MIN, for percentile values of 1, please use MAX.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_percentile_1_is_not_included",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.PERCENTILE,
                        agg_params=PydanticMeasureAggregationParameters(
                            percentile=1,
                        ),
                    ),
                ),
            ),
            "Percentile aggregation parameter for metric 'metric_with_percentile_1_is_not_included' is '1.0', but must "
            "be between 0 and 1 (non-inclusive). For example, to indicate the 65th percentile value, set 'percentile: "
            "0.65'. For percentile values of 0, please use MIN, for percentile values of 1, please use MAX.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_missing_percentile_parameter",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.PERCENTILE,
                        agg_params=PydanticMeasureAggregationParameters(),
                    ),
                ),
            ),
            "Metric 'metric_with_missing_percentile_parameter' uses a PERCENTILE aggregation, which "
            "requires agg_params.percentile to be provided.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_no_agg_params",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.PERCENTILE,
                    ),
                ),
            ),
            "Metric 'metric_with_no_agg_params' uses a PERCENTILE aggregation, which "
            "requires agg_params.percentile to be provided.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_percentile_value_but_wrong_agg_type",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.SUM,
                        agg_params=PydanticMeasureAggregationParameters(
                            percentile=0.5,
                        ),
                    ),
                ),
            ),
            "Metric 'metric_with_percentile_value_but_wrong_agg_type' with aggregation 'sum' "
            "uses agg_params (percentile) only relevant to Percentile metrics.",
        ),
        # TODO SL-4116: Figure out if 'agg' actually should be optional or if it is
        # required, and then either update the structs or add tests, as appropriate.
    ],
)
def test_simple_metrics_percentile_aggregation(  # noqa: D103
    metric: PydanticMetric,
    error_substring: str,
) -> None:
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([MetricsPercentileAggregationRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure2",
                    measures=[
                        PydanticMeasure(
                            name="this_measure_name",
                            agg=AggregationType.SUM,
                            agg_time_dimension="ename",
                        )
                    ],
                    dimensions=[
                        PydanticDimension(name="country", type=DimensionType.CATEGORICAL),
                        PydanticDimension(
                            name="time_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                        PydanticDimension(
                            name="weekly_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.WEEK,
                            ),
                        ),
                    ],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
            ],
            metrics=[metric],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    check_error_in_issues(error_substrings=[error_substring], issues=validation_results.all_issues)


@pytest.mark.parametrize(
    "metric, error_substring_if_error",
    [
        (
            metric_with_guaranteed_meta(
                name="cumulative_metric_with_agg_params_are_not_allowed",
                type=MetricType.CUMULATIVE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.SUM,
                    ),
                ),
            ),
            "Metric 'cumulative_metric_with_agg_params_are_not_allowed' is not a Simple "
            "metric, so it cannot have values for 'agg', 'agg_time_dimension', "
            "'non_additive_dimension', 'percentile', or 'expr'.",
        ),
        (
            metric_with_guaranteed_meta(
                name="conversion_metric_with_agg_params_are_not_allowed",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.MAX,
                    ),
                ),
            ),
            "Metric 'conversion_metric_with_agg_params_are_not_allowed' is not a Simple "
            "metric, so it cannot have values for 'agg', 'agg_time_dimension', "
            "'non_additive_dimension', 'percentile', or 'expr'.",
        ),
        (
            metric_with_guaranteed_meta(
                name="simple_metric_can_have_agg_params",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.MAX,
                    ),
                ),
            ),
            None,  # No error; this should pass
        ),
    ],
)
def test_simple_metrics_are_the_only_metrics_allowed_to_have_agg_params(  # noqa: D103
    metric: PydanticMetric,
    error_substring_if_error: Optional[str],
) -> None:
    model_validator = SemanticManifestValidator[PydanticSemanticManifest](
        [MetricAggregationParamsInForSimpleMetricsRule()]
    )
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[
                        PydanticMeasure(
                            name="this_measure_name",
                            agg=AggregationType.SUM,
                            agg_time_dimension="ename",
                        )
                    ],
                    dimensions=[
                        PydanticDimension(name="country", type=DimensionType.CATEGORICAL),
                        PydanticDimension(
                            name="time_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                        PydanticDimension(
                            name="weekly_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.WEEK,
                            ),
                        ),
                    ],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
            ],
            metrics=[metric],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    if error_substring_if_error:
        check_error_in_issues(error_substrings=[error_substring_if_error], issues=validation_results.all_issues)
    else:
        assert len(validation_results.all_issues) == 0, "expected this metric to pass validation, but it did not"


@pytest.mark.parametrize(
    "metric, error_substring_if_error, warning_substring_if_warning",
    [
        (
            metric_with_guaranteed_meta(
                name="metric_with_measure_only",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    measure=PydanticMetricInputMeasure(name="this_measure_name"),
                ),
            ),
            None,  # No error; this should pass
            None,  # No warnings
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_agg_params_only",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.SUM,
                    ),
                ),
            ),
            None,  # No error; this should pass
            None,  # No warnings
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_both_measure_and_agg_params",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    measure=PydanticMetricInputMeasure(name="this_measure_name"),
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.SUM,
                    ),
                ),
            ),
            None,  # No Errors
            "Metric 'metric_with_both_measure_and_agg_params' should not have both "
            "metric_aggregation_params and a measure. The measure will be ignored; please "
            "remove it to avoid confusion.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_neither_measure_nor_agg_params",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(),
            ),
            "Metric 'metric_with_neither_measure_nor_agg_params' is a Simple metric, so it must have either "
            "metric_aggregation_params or a measure.",
            None,  # No warnings
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_measure_and_fill_nulls_with",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    measure=PydanticMetricInputMeasure(name="this_measure_name"),
                    fill_nulls_with=1,
                ),
            ),
            None,  # No warnings
            "Simple Metric 'metric_with_measure_and_fill_nulls_with' should not have a measure input as well as a "
            "value for fill_nulls_with.  The metric's fill_nulls_with "
            "will be ignored until the measure is removed.",
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_with_measure_and_join_to_timespine",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    measure=PydanticMetricInputMeasure(name="this_measure_name"),
                    join_to_timespine=True,
                ),
            ),
            None,  # No warnings
            "Simple Metric 'metric_with_measure_and_join_to_timespine' should not have a measure input as well as a "
            "value for join_to_timespine.  The metric's join_to_timespine "
            "will be ignored until the measure is removed.",
        ),
    ],
)
def test_simple_metrics_have_measures_xor_agg_params(  # noqa: D103
    metric: PydanticMetric,
    error_substring_if_error: Optional[str],
    warning_substring_if_warning: Optional[str],
) -> None:
    model_validator = SemanticManifestValidator[PydanticSemanticManifest](
        [MetricAggregationParamsInForSimpleMetricsRule()]
    )
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[
                        PydanticMeasure(
                            name="this_measure_name",
                            agg=AggregationType.SUM,
                            agg_time_dimension="ename",
                        )
                    ],
                    dimensions=[
                        PydanticDimension(name="country", type=DimensionType.CATEGORICAL),
                        PydanticDimension(
                            name="time_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                        PydanticDimension(
                            name="weekly_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.WEEK,
                            ),
                        ),
                    ],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
            ],
            metrics=[metric],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    if error_substring_if_error:
        check_error_in_issues(error_substrings=[error_substring_if_error], issues=validation_results.errors)
    if warning_substring_if_warning:
        check_error_in_issues(error_substrings=[warning_substring_if_warning], issues=validation_results.warnings)
    if not error_substring_if_error and not warning_substring_if_warning:
        assert len(validation_results.all_issues) == 0, "expected this metric to pass validation, but it did not"


@pytest.mark.parametrize(
    "metric, error_substring_if_error",
    [
        (
            metric_with_guaranteed_meta(
                name="metric_with_measure_only",
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="sum_measure",
                        agg=AggregationType.SUM,
                    ),
                    fill_nulls_with=1,
                    join_to_timespine=True,
                ),
            ),
            None,  # No error; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="non_simple_metric_with_fill_nulls_with",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    fill_nulls_with=1,
                ),
            ),
            # There will be other errors because this metric is missing fields,
            # but we don't care about them.
            "Metric 'non_simple_metric_with_fill_nulls_with' is not a Simple metric, so it "
            "cannot have a value for for the following fields: 'fill_nulls_with'.",
        ),
        (
            metric_with_guaranteed_meta(
                name="non_simple_metric_with_join_to_timespine",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    join_to_timespine=True,
                ),
            ),
            # There will be other errors because this metric is missing fields,
            # but we don't care about them.
            "Metric 'non_simple_metric_with_join_to_timespine' is not a Simple metric, so it "
            "cannot have a value for for the following fields: 'join_to_timespine'.",
        ),
        (
            metric_with_guaranteed_meta(
                name="non_simple_metric_both_filter_nulls_with_and_join_to_timespine",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    join_to_timespine=True,
                    fill_nulls_with=1,
                ),
            ),
            # There will be other errors because this metric is missing fields,
            # but we don't care about them.
            "Metric 'non_simple_metric_both_filter_nulls_with_and_join_to_timespine' is not a Simple metric, so it "
            "cannot have a value for for the following fields: 'fill_nulls_with', 'join_to_timespine'.",
        ),
    ],
)
def test_non_simple_metrics_cannot_have_input_fields(
    metric: PydanticMetric,
    error_substring_if_error: Optional[str],
) -> None:
    """Validate that things like fill_nulls_with and join_to_timespine are not allowed on non-simple metrics."""
    model_validator = SemanticManifestValidator[PydanticSemanticManifest](
        [MetricAggregationParamsInForSimpleMetricsRule()]
    )
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[
                        PydanticMeasure(
                            name="this_measure_name",
                            agg=AggregationType.SUM,
                            agg_time_dimension="ename",
                        )
                    ],
                    dimensions=[
                        PydanticDimension(name="country", type=DimensionType.CATEGORICAL),
                        PydanticDimension(
                            name="time_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                        PydanticDimension(
                            name="weekly_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.WEEK,
                            ),
                        ),
                    ],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
            ],
            metrics=[metric],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    if error_substring_if_error:
        check_error_in_issues(error_substrings=[error_substring_if_error], issues=validation_results.all_issues)
    else:
        assert len(validation_results.all_issues) == 0, "expected this metric to pass validation, but it did not"


def test_metric_no_time_dim_dim_only_source() -> None:  # noqa: D103
    dim_name = "country"
    dim2_name = "ename"
    measure_name = "foo"
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]()
    model_validator.checked_validations(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[],
                    dimensions=[PydanticDimension(name=dim_name, type=DimensionType.CATEGORICAL)],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
                semantic_model_with_guaranteed_meta(
                    name="sum_measure2",
                    measures=[
                        PydanticMeasure(
                            name=measure_name,
                            agg=AggregationType.SUM,
                            agg_time_dimension=dim2_name,
                        )
                    ],
                    dimensions=[
                        PydanticDimension(name=f"{dim_name}_dup", type=DimensionType.CATEGORICAL),
                        PydanticDimension(
                            name=dim2_name,
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                    ],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
            ],
            metrics=[
                metric_with_guaranteed_meta(
                    name="metric_with_no_time_dim",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                )
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )


def test_metric_no_time_dim() -> None:  # noqa: D103
    with pytest.raises(SemanticManifestValidationException):
        dim_name = "country"
        measure_name = "foo"
        model_validator = SemanticManifestValidator[PydanticSemanticManifest]()
        model_validator.checked_validations(
            PydanticSemanticManifest(
                semantic_models=[
                    semantic_model_with_guaranteed_meta(
                        name="sum_measure",
                        measures=[PydanticMeasure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            PydanticDimension(
                                name=dim_name,
                                type=DimensionType.CATEGORICAL,
                            )
                        ],
                    )
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name="metric_with_no_time_dim",
                        type=MetricType.SIMPLE,
                        type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                    )
                ],
                project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
            )
        )


def test_generated_metrics_only() -> None:  # noqa: D103
    dim_reference = DimensionReference(element_name="dim")

    dim2_reference = TimeDimensionReference(element_name="ds")
    measure_name = "measure"
    entity_reference = EntityReference(element_name="primary")
    semantic_model = semantic_model_with_guaranteed_meta(
        name="dim1",
        measures=[
            PydanticMeasure(name=measure_name, agg=AggregationType.SUM, agg_time_dimension=dim2_reference.element_name)
        ],
        dimensions=[
            PydanticDimension(name=dim_reference.element_name, type=DimensionType.CATEGORICAL),
            PydanticDimension(
                name=dim2_reference.element_name,
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(
                    time_granularity=TimeGranularity.DAY,
                ),
            ),
        ],
        entities=[
            PydanticEntity(name=entity_reference.element_name, type=EntityType.PRIMARY),
        ],
    )
    semantic_model.measures[0].create_metric = True

    SemanticManifestValidator[PydanticSemanticManifest]().checked_validations(
        PydanticSemanticManifest(
            semantic_models=[semantic_model],
            metrics=[],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )


def test_derived_metric() -> None:  # noqa: D103
    measure_name = "foo"
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([DerivedMetricRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[
                        PydanticMeasure(
                            name=measure_name,
                            agg=AggregationType.SUM,
                            agg_time_dimension="ds",
                        )
                    ],
                    dimensions=[
                        PydanticDimension(
                            name="ds",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                    ],
                ),
            ],
            metrics=[
                metric_with_guaranteed_meta(
                    name="random_metric",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                ),
                metric_with_guaranteed_meta(
                    name="random_metric2",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                ),
                metric_with_guaranteed_meta(
                    name="alias_collision",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric2 * 2",
                        metrics=[
                            PydanticMetricInput(name="random_metric", alias="random_metric2"),
                            PydanticMetricInput(name="random_metric2"),
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="doesntexist",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="notexist * 2", metrics=[PydanticMetricInput(name="notexist")]
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="no_expr",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(metrics=[PydanticMetricInput(name="random_metric")]),
                ),
                metric_with_guaranteed_meta(
                    name="input_metric_not_in_expr",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(expr="x", metrics=[PydanticMetricInput(name="random_metric")]),
                ),
                metric_with_guaranteed_meta(
                    name="no_input_metrics",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(expr="x"),
                ),
                metric_with_guaranteed_meta(
                    name="has_valid_time_window_params",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric / random_metric3",
                        metrics=[
                            PydanticMetricInput(
                                name="random_metric",
                                offset_window=PydanticMetricTimeWindow.parse("3 weekies"),
                            ),
                            PydanticMetricInput(
                                name="random_metric",
                                offset_to_grain=TimeGranularity.MONTH.value,
                                alias="random_metric3",
                            ),
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="has_both_time_offset_params_on_same_input_metric",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric * 2",
                        metrics=[
                            PydanticMetricInput(
                                name="random_metric",
                                offset_window=PydanticMetricTimeWindow.parse("3 weeks"),
                                offset_to_grain=TimeGranularity.MONTH.value,
                            )
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="has_custom_grain_offset_window",  # this is valid
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric * 2",
                        metrics=[
                            PydanticMetricInput(
                                name="random_metric",
                                offset_window=PydanticMetricTimeWindow.parse("3 martian_weeks"),
                            )
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="has_custom_offset_to_grain",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric * 2",
                        metrics=[
                            PydanticMetricInput(
                                name="random_metric",
                                offset_to_grain="martian_week",
                            )
                        ],
                    ),
                ),
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    build_issues = validation_results.all_issues
    assert len(build_issues) == 8
    expected_substrings = [
        "is already being used. Please choose another alias",
        "does not exist as a configured metric in the model",
        "Both offset_window and offset_to_grain set",
        "is not used in `expr`",
        "No input metrics found for derived metric",
        "No `expr` set for derived metric",
        "Invalid time granularity 'weekies' in window: '3 weekies'",
        "Custom granularities are not supported",
        "Invalid time granularity found in `offset_to_grain`: 'martian_week'",
    ]
    check_error_in_issues(error_substrings=expected_substrings, issues=build_issues)


def test_cumulative_metrics() -> None:  # noqa: D103
    measure_name = "foo"
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([CumulativeMetricRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[
                        PydanticMeasure(
                            name=measure_name,
                            agg=AggregationType.SUM,
                            agg_time_dimension="ds",
                        )
                    ],
                    dimensions=[
                        PydanticDimension(
                            name="ds",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                    ],
                ),
            ],
            metrics=[
                # Metrics with old type params structure - should get warning
                metric_with_guaranteed_meta(
                    name="metric1",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.WEEK.value),
                        cumulative_type_params=PydanticCumulativeTypeParams(period_agg=PeriodAggregation.LAST),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="metric2",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        grain_to_date=TimeGranularity.MONTH,
                    ),
                ),
                # Metrics with new type params structure - should have no issues
                metric_with_guaranteed_meta(
                    name="big_mama",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.WEEK.value),
                            period_agg=PeriodAggregation.AVERAGE,
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="lil_baby",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(grain_to_date=TimeGranularity.MONTH.value),
                    ),
                ),
                # Metric with both window & grain across both type_params - should get warning
                metric_with_guaranteed_meta(
                    name="woooooo",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        grain_to_date=TimeGranularity.MONTH,
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.WEEK.value),
                            period_agg=PeriodAggregation.FIRST,
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="dis_bad",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        window=PydanticMetricTimeWindow(count=2, granularity=TimeGranularity.QUARTER.value),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.QUARTER.value),
                        ),
                    ),
                ),
                # Metric without window or grain_to_date - should have no issues
                metric_with_guaranteed_meta(
                    name="dis_good",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(period_agg=PeriodAggregation.FIRST),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="bad_window",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow.parse(window="3 moons"),
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_grain_window",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow.parse(window="3 martian_week"),
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_grain_window_plural",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow.parse(window="3 martian_weeks"),
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_grain_to_date",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            grain_to_date="3 martian_weeks",
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_window_old",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        window=PydanticMetricTimeWindow.parse(window="5 martian_week"),
                    ),
                ),
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )

    build_issues = validation_results.all_issues
    assert len(build_issues) == 7
    expected_substrings = [
        "Invalid time granularity",
        "Both window and grain_to_date set for cumulative metric. Please set one or the other.",
        "Got differing values for `window`",
        "Invalid time granularity 'martian_week' in window: '3 martian_weeks'",
        "Invalid time granularity 'martian_week' in window: '3 martian_week'",
        "Invalid time granularity 'martian_week' in window: '5 martian_week'",
    ]
    check_error_in_issues(error_substrings=expected_substrings, issues=build_issues)


@pytest.mark.parametrize(
    "metric, error_substring_if_error, error_substring_if_warning",
    [
        (
            metric_with_guaranteed_meta(
                name="good_cuz_it_has_only_measure",
                type=MetricType.CUMULATIVE,
                type_params=PydanticMetricTypeParams(
                    measure=PydanticMetricInputMeasure(name="sum_measure"),
                    cumulative_type_params=PydanticCumulativeTypeParams(period_agg=PeriodAggregation.FIRST),
                ),
            ),
            None,  # No error; this should pass
            None,
        ),
        (
            metric_with_guaranteed_meta(
                name="good_cuz_it_has_only_metric",
                type=MetricType.CUMULATIVE,
                type_params=PydanticMetricTypeParams(
                    cumulative_type_params=PydanticCumulativeTypeParams(
                        period_agg=PeriodAggregation.FIRST,
                        metric=PydanticMetricInput(name="sum_metric"),
                    ),
                ),
            ),
            None,  # No error; this should pass
            None,
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_metric_has_both_measure_and_metric_as_inputs",
                type=MetricType.CUMULATIVE,
                type_params=PydanticMetricTypeParams(
                    measure=PydanticMetricInputMeasure(name="sum_measure"),
                    cumulative_type_params=PydanticCumulativeTypeParams(
                        period_agg=PeriodAggregation.FIRST,
                        metric=PydanticMetricInput(name="sum_metric"),
                    ),
                ),
            ),
            None,
            "Cumulative metric 'bad_metric_has_both_measure_and_metric_as_inputs' should not have both a measure "
            "and a metric as inputs. The measure will be ignored; please remove "
            "it to avoid confusion.",
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_metric_has_neither_measure_nor_metric_as_inputs",
                type=MetricType.CUMULATIVE,
                type_params=PydanticMetricTypeParams(
                    cumulative_type_params=PydanticCumulativeTypeParams(period_agg=PeriodAggregation.FIRST),
                ),
            ),
            None,
            "Cumulative metric 'bad_metric_has_neither_measure_nor_metric_as_inputs' must have either a measure "
            "or a metric as inputs. Please add one of them.",
        ),
    ],
)
def test_cumulative_metrics_have_metric_xor_measure(
    metric: PydanticMetric,
    error_substring_if_error: Optional[str],
    error_substring_if_warning: Optional[str],
) -> None:
    """Validate that things like fill_nulls_with and join_to_timespine are not allowed on non-simple metrics."""
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([CumulativeMetricRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[
                        PydanticMeasure(
                            name="this_measure_name",
                            agg=AggregationType.SUM,
                            agg_time_dimension="ename",
                        )
                    ],
                    dimensions=[
                        PydanticDimension(name="country", type=DimensionType.CATEGORICAL),
                        PydanticDimension(
                            name="time_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                        PydanticDimension(
                            name="weekly_dim",
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.WEEK,
                            ),
                        ),
                    ],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
            ],
            metrics=[metric],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    if error_substring_if_error:
        check_error_in_issues(error_substrings=[error_substring_if_error], issues=validation_results.errors)
    if error_substring_if_warning:
        check_error_in_issues(error_substrings=[error_substring_if_warning], issues=validation_results.warnings)
    if not error_substring_if_error and not error_substring_if_warning:
        assert len(validation_results.all_issues) == 0, "expected this metric to pass validation, but it did not"


def test_time_granularity() -> None:
    """Test that default grain is validated appropriately."""
    week_measure_name = "foo"
    month_measure_name = "boo"
    week_time_dim_name = "ds__week"
    month_time_dim_name = "ds__month"
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([MetricTimeGranularityRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="semantic_model",
                    measures=[
                        PydanticMeasure(
                            name=month_measure_name, agg=AggregationType.SUM, agg_time_dimension=month_time_dim_name
                        ),
                        PydanticMeasure(
                            name=week_measure_name, agg=AggregationType.SUM, agg_time_dimension=week_time_dim_name
                        ),
                    ],
                    dimensions=[
                        PydanticDimension(
                            name=month_time_dim_name,
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.MONTH),
                        ),
                        PydanticDimension(
                            name=week_time_dim_name,
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.WEEK),
                        ),
                    ],
                ),
            ],
            metrics=[
                # Simple metrics
                metric_with_guaranteed_meta(
                    name="month_metric_with_no_time_granularity_set",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=month_measure_name),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="week_metric_with_valid_time_granularity",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=week_measure_name),
                    ),
                    time_granularity=TimeGranularity.MONTH.value,
                ),
                metric_with_guaranteed_meta(
                    name="month_metric_with_invalid_time_granularity",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=month_measure_name),
                    ),
                    time_granularity=TimeGranularity.WEEK.value,
                ),
                # Derived metrics
                metric_with_guaranteed_meta(
                    name="derived_metric_with_no_time_granularity_set",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        metrics=[
                            PydanticMetricInput(name="week_metric_with_valid_time_granularity"),
                        ],
                        expr="week_metric_with_valid_time_granularity + 1",
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="derived_metric_with_valid_time_granularity",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        metrics=[
                            PydanticMetricInput(name="week_metric_with_valid_time_granularity"),
                            PydanticMetricInput(name="month_metric_with_no_time_granularity_set"),
                        ],
                        expr=("week_metric_with_valid_time_granularity + month_metric_with_no_time_granularity_set"),
                    ),
                    time_granularity=TimeGranularity.YEAR.value,
                ),
                metric_with_guaranteed_meta(
                    name="derived_metric_with_invalid_time_granularity",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        metrics=[
                            PydanticMetricInput(name="week_metric_with_valid_time_granularity"),
                            PydanticMetricInput(name="month_metric_with_no_time_granularity_set"),
                        ],
                        expr=("week_metric_with_valid_time_granularity + month_metric_with_no_time_granularity_set"),
                    ),
                    time_granularity=TimeGranularity.DAY.value,
                ),
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )

    build_issues = validation_results.all_issues
    assert len(build_issues) == 2
    expected_substrings = [
        "`time_granularity` for metric 'month_metric_with_invalid_time_granularity' must be >= MONTH.",
        "`time_granularity` for metric 'derived_metric_with_invalid_time_granularity' must be >= MONTH.",
    ]
    check_error_in_issues(error_substrings=expected_substrings, issues=build_issues)
