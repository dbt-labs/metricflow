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
    PydanticConstantPropertyInput,
    PydanticConversionTypeParams,
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
from metricflow_semantic_interfaces.test_utils import (
    metric_with_guaranteed_meta,
    semantic_model_with_guaranteed_meta,
)
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)
from metricflow_semantic_interfaces.validations.metrics import ConversionMetricRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION
from tests.validations.validation_test_utils import check_error_in_issues

BASE_MEASURE_NAME = "base_measure"
CONVERSION_MEASURE_NAME = "conversion_measure"
ENTITY_NAME = "entity"
INVALID_ENTITY_NAME = "bad_entity"
INVALID_MEASURE_NAME = "invalid_measure"
DEFAULT_WINDOW = PydanticMetricTimeWindow.parse("7 days")

INPUT_CONVERSION_METRIC_NAME = "conversion_input_metric"
INPUT_CONVERSION_METRIC = metric_with_guaranteed_meta(
    name=INPUT_CONVERSION_METRIC_NAME,
    type=MetricType.SIMPLE,
    type_params=PydanticMetricTypeParams(
        metric_aggregation_params=PydanticMetricAggregationParams(
            agg=AggregationType.COUNT,
            semantic_model="conversion",
        ),
        expr="1",
    ),
)

INPUT_BASE_METRIC_NAME = "base_input_metric"
INPUT_BASE_METRIC = metric_with_guaranteed_meta(
    name=INPUT_BASE_METRIC_NAME,
    type=MetricType.SIMPLE,
    type_params=PydanticMetricTypeParams(
        metric_aggregation_params=PydanticMetricAggregationParams(
            agg=AggregationType.COUNT,
            semantic_model="base",
        ),
        expr="1",
    ),
)

METRIC_WITH_NON_EXISTENT_MODEL_NAME = "metric_with_nonexistent_model"
METRIC_WITH_NON_EXISTENT_MODEL = metric_with_guaranteed_meta(
    name=METRIC_WITH_NON_EXISTENT_MODEL_NAME,
    type=MetricType.SIMPLE,
    type_params=PydanticMetricTypeParams(
        metric_aggregation_params=PydanticMetricAggregationParams(
            agg=AggregationType.COUNT,
            semantic_model="this_model_does_not_exist",
        ),
        expr="1",
    ),
)

BASE_SUM_METRIC_NAME = "sum_metric"
BASE_SUM_METRIC = metric_with_guaranteed_meta(
    name=BASE_SUM_METRIC_NAME,
    type=MetricType.SIMPLE,
    type_params=PydanticMetricTypeParams(
        metric_aggregation_params=PydanticMetricAggregationParams(
            agg=AggregationType.SUM,
            semantic_model="base",
        ),
    ),
)

NON_SIMPLE_METRIC_NAME = "non_simple_metric"
NON_SIMPLE_METRIC = metric_with_guaranteed_meta(
    name=NON_SIMPLE_METRIC_NAME,
    type=MetricType.CUMULATIVE,
    type_params=PydanticMetricTypeParams(
        measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
        grain_to_date=TimeGranularity.MONTH,
    ),
)

VALIDATOR = SemanticManifestValidator[PydanticSemanticManifest]([ConversionMetricRule()])
SEMANTIC_MODELS = [
    semantic_model_with_guaranteed_meta(
        name="base",
        measures=[
            PydanticMeasure(name=BASE_MEASURE_NAME, agg=AggregationType.COUNT, agg_time_dimension="ds", expr="1"),
            PydanticMeasure(name=INVALID_MEASURE_NAME, agg=AggregationType.MAX, agg_time_dimension="ds"),
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
        entities=[
            PydanticEntity(name=ENTITY_NAME, type=EntityType.PRIMARY),
        ],
    ),
    semantic_model_with_guaranteed_meta(
        name="conversion",
        measures=[
            PydanticMeasure(name=CONVERSION_MEASURE_NAME, agg=AggregationType.COUNT, agg_time_dimension="ds", expr="1")
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
        entities=[
            PydanticEntity(name=ENTITY_NAME, type=EntityType.PRIMARY),
        ],
    ),
]


@pytest.mark.parametrize(
    "metric, error_substrings_if_errors, warning_substrings_if_warnings",
    [
        # ======================= Basic Happy Example Tests =======================
        (
            metric_with_guaranteed_meta(
                name="proper_metric_with_measure_base_and_measure_conversion",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            None,  # No error; this should pass
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="proper_metric_with_metric_base_and_measure_conversion",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_metric=PydanticMetricInput(name=INPUT_BASE_METRIC_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            None,  # No error; this should pass
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="proper_metric_with_measure_base_and_metric_conversion",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_metric=PydanticMetricInput(name=INPUT_CONVERSION_METRIC_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            None,  # No error; this should pass
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="proper_metric_with_metric_base_and_metric_conversion",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_metric=PydanticMetricInput(name=INPUT_BASE_METRIC_NAME),
                        conversion_metric=PydanticMetricInput(name=INPUT_CONVERSION_METRIC_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            None,  # No error; this should pass
            None,  # No warning; this should pass
        ),
        # =============== Basic Missing Params Validation(s) =======================
        (
            metric_with_guaranteed_meta(
                name="missing_conversion_type_params",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(),
            ),
            ["Conversion metric 'missing_conversion_type_params' must have conversion_type_params."],
            None,  # No warning; this should pass
        ),
        # =============== Entity, Constant, Window, Grain - General Tests ===============
        (
            metric_with_guaranteed_meta(
                name="entity_doesnt_exist",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=INVALID_ENTITY_NAME,
                    )
                ),
            ),
            [
                f"Entity: {INVALID_ENTITY_NAME} not found in base semantic model: base",
            ],
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="constant_property_doesnt_exist",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                        constant_properties=[
                            PydanticConstantPropertyInput(base_property="bad_dim", conversion_property="bad_dim2")
                        ],
                    )
                ),
            ),
            [
                "The provided constant property: bad_dim, cannot be found in semantic model base",
                "The provided constant property: bad_dim2, cannot be found in semantic model conversion",
            ],
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_window",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                        ),
                        window=PydanticMetricTimeWindow.parse("7 moons"),
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "Invalid time granularity 'moons' in window: '7 moons'",
            ],
            None,
        ),
        (
            metric_with_guaranteed_meta(
                name="custom_grain_window",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                        ),
                        window=PydanticMetricTimeWindow.parse("7 martian_week"),
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "Invalid time granularity 'martian_week' in window: '7 martian_week' "
                "Custom granularities are not supported for this field yet.",
            ],
            None,
        ),
        (
            metric_with_guaranteed_meta(
                name="custom_grain_window_plural_grain_name",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                        ),
                        window=PydanticMetricTimeWindow.parse("7 martian_weeks"),
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "Invalid time granularity 'martian_week' in window: '7 martian_weeks' "
                "Custom granularities are not supported for this field yet."
            ],
            None,
        ),
        # =============== Input Measures / Metrics Property Validations ====================
        (
            metric_with_guaranteed_meta(
                name="bad_measure_agg_type_metric",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=INVALID_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "For conversion metrics, the input measure must be COUNT/SUM(1)/COUNT_DISTINCT. "
                f"Measure '{INVALID_MEASURE_NAME}' is agg type: AggregationType.MAX",
            ],
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_metric_agg_type_metric",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_metric=PydanticMetricInput(name=BASE_SUM_METRIC_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "For conversion metrics, the input metric must be COUNT/SUM(1)/COUNT_DISTINCT. "
                f"Metric '{BASE_SUM_METRIC_NAME}' is agg type: AggregationType.SUM",
            ],
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="filter_on_conversion_metric_is_not_supported_warning",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_metric=PydanticMetricInput(
                            name=INPUT_CONVERSION_METRIC_NAME,
                            filter=PydanticWhereFilterIntersection(
                                where_filters=[
                                    PydanticWhereFilter(where_sql_template="""{{ dimension('some_bool') }}""")
                                ]
                            ),
                        ),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            None,  # This only fires a warning, not an error.
            [
                f"Metric input '{INPUT_CONVERSION_METRIC_NAME}' has a filter. For conversion metrics, "
                "filtering on the conversion input is not fully supported yet.",
            ],
        ),
        (
            metric_with_guaranteed_meta(
                name="filter_on_conversion_measure_is_not_supported_warning",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                            filter=PydanticWhereFilterIntersection(
                                where_filters=[
                                    PydanticWhereFilter(where_sql_template="""{{ dimension('some_bool') }}""")
                                ]
                            ),
                        ),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            None,  # This only fires a warning, not an error.
            [
                f"Measure input '{CONVERSION_MEASURE_NAME}' has a filter. For conversion metrics, "
                "filtering on the conversion input is not fully supported yet.",
            ],
        ),
        (
            metric_with_guaranteed_meta(
                name="metric_has_bogus_filter",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                            filter=PydanticWhereFilterIntersection(
                                where_filters=[
                                    PydanticWhereFilter(where_sql_template="""{{ dimension('some_bool') }}""")
                                ]
                            ),
                        ),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            None,  # This only fires a warning, not an error.
            [
                f"Measure input '{CONVERSION_MEASURE_NAME}' has a filter. For conversion metrics, "
                "filtering on the conversion input is not fully supported yet.",
            ],
        ),
        (
            metric_with_guaranteed_meta(
                name="input_metric_has_nonexistent_model",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_metric=PydanticMetricInput(name=METRIC_WITH_NON_EXISTENT_MODEL_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                        ),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                f"Input metric '{METRIC_WITH_NON_EXISTENT_MODEL_NAME}' for conversion metric "
                "'input_metric_has_nonexistent_model' is linked to a semantic model "
                "that does not exist in your manifest.",
            ],
            None,
        ),
        (
            metric_with_guaranteed_meta(
                name="input_measure_is_not_real",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name="i_am_not_real",
                        ),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "Input measure 'i_am_not_real' for conversion metric "
                "'input_measure_is_not_real' does not exist in your manifest.",
            ],
            None,
        ),
        # =============== Correct Inputs Provided Validations ====================
        (
            metric_with_guaranteed_meta(
                name="bad_metric_has_both_base_measure_and_base_metric",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name="base_measure"),
                        base_metric=PydanticMetricInput(name="base_metric"),
                        conversion_measure=PydanticMetricInputMeasure(
                            name="conversion_measure",
                        ),
                        entity="primary_entity",
                    ),
                ),
            ),
            None,  # No errors
            [
                "Conversion metric 'bad_metric_has_both_base_measure_and_base_metric' "
                "should not have both a base measure "
                "and a base metric as inputs. The base measure will be ignored; "
                "please remove it to avoid confusion.",
            ],
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_metric_has_neither_base_measure_nor_base_metric",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        conversion_measure=PydanticMetricInputMeasure(
                            name="conversion_measure",
                        ),
                        entity="primary_entity",
                    ),
                ),
            ),
            [
                "Conversion metric 'bad_metric_has_neither_base_measure_nor_base_metric' must "
                "have either a base measure "
                "or a base metric as an input. Please add one of them.",
            ],
            None,  # No warnings
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_metric_has_both_conversion_measure_and_conversion_metric",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_metric=PydanticMetricInput(name=INPUT_BASE_METRIC_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name="measure_1"),
                        conversion_metric=PydanticMetricInput(name="conversion_metric"),
                        entity="primary_entity",
                    ),
                ),
            ),
            None,  # No errors
            [
                "Conversion metric 'bad_metric_has_both_conversion_measure_and_conversion_metric' "
                "should not have both a conversion measure and a conversion metric "
                "as inputs. The conversion measure will be ignored; please remove "
                "it to avoid confusion.",
            ],
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_metric_has_neither_conversion_measure_nor_conversion_metric",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        entity="primary_entity",
                        base_metric=PydanticMetricInput(name=INPUT_BASE_METRIC_NAME),
                    ),
                ),
            ),
            [
                "Conversion metric 'bad_metric_has_neither_conversion_measure_nor_conversion_metric' "
                "must have either a "
                "conversion measure or a conversion metric as an input. Please add one of them.",
            ],
            None,  # No warnings
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_metric_has_non_simple_metric_as_base_metric",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_metric=PydanticMetricInput(name=NON_SIMPLE_METRIC_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        entity="primary_entity",
                    ),
                ),
            ),
            [
                f"Metric '{NON_SIMPLE_METRIC_NAME}' is not a Simple metric, so it cannot be "
                "used as an input for Conversion metric 'bad_metric_has_non_simple_metric_as_base_metric'.",
            ],
            None,  # No warnings
        ),
    ],
)
def test_conversion_metrics(  # noqa: D103
    metric: PydanticMetric,
    error_substrings_if_errors: Optional[List[str]],
    warning_substrings_if_warnings: Optional[List[str]],
) -> None:
    validation_results = VALIDATOR.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=SEMANTIC_MODELS,
            metrics=[
                metric,
                INPUT_CONVERSION_METRIC,
                INPUT_BASE_METRIC,
                BASE_SUM_METRIC,
                NON_SIMPLE_METRIC,
                METRIC_WITH_NON_EXISTENT_MODEL,
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    if error_substrings_if_errors:
        check_error_in_issues(error_substrings=error_substrings_if_errors, issues=validation_results.errors)
    if warning_substrings_if_warnings:
        check_error_in_issues(error_substrings=warning_substrings_if_warnings, issues=validation_results.warnings)
    if not error_substrings_if_errors and not warning_substrings_if_warnings:
        assert len(validation_results.all_issues) == 0, "expected this metric to pass validation, but it did not"
