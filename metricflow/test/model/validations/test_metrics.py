import pytest

from metricflow.aggregation_properties import AggregationType
from dbt.semantic.validations.model_validator import ModelValidator
from dbt.contracts.graph.entities import Mutability, MutabilityType
from dbt.contracts.graph.dimensions import Dimension, DimensionType, DimensionTypeParams
from dbt.contracts.graph.identifiers import Identifier, IdentifierType
from dbt.contracts.graph.measures import Measure
from dbt.contracts.graph.metrics import MetricInput, MetricType, MetricTypeParams
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.validations.metrics import DerivedMetricRule
from metricflow.model.validations.validator_helpers import ModelValidationException
from dbt.semantic.references import DimensionReference, IdentifierReference, TimeDimensionReference
from metricflow.test.fixtures.table_fixtures import DEFAULT_DS
from metricflow.test.model.validations.helpers import entity_with_guaranteed_meta, metric_with_guaranteed_meta
from metricflow.time.time_granularity import TimeGranularity


def test_metric_no_time_dim_dim_only_source() -> None:  # noqa:D
    dim_name = "country"
    dim2_name = "ename"
    measure_name = "foo"
    model_validator = ModelValidator()
    model_validator.checked_validations(
        UserConfiguredModel(
            entities=[
                entity_with_guaranteed_meta(
                    name="sum_measure",
                    sql_query="SELECT foo, country FROM bar",
                    measures=[],
                    dimensions=[Dimension(name=dim_name, type=DimensionType.CATEGORICAL)],
                    mutability=Mutability(type=MutabilityType.IMMUTABLE),
                ),
                entity_with_guaranteed_meta(
                    name="sum_measure2",
                    sql_query="SELECT foo, country FROM bar",
                    measures=[
                        Measure(
                            name=measure_name,
                            agg=AggregationType.SUM,
                            agg_time_dimension=dim2_name,
                        )
                    ],
                    dimensions=[
                        Dimension(name=dim_name, type=DimensionType.CATEGORICAL),
                        Dimension(
                            name=dim2_name,
                            type=DimensionType.TIME,
                            type_params=DimensionTypeParams(
                                is_primary=True,
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                    ],
                    mutability=Mutability(type=MutabilityType.IMMUTABLE),
                ),
            ],
            metrics=[
                metric_with_guaranteed_meta(
                    name="metric_with_no_time_dim",
                    type=MetricType.MEASURE_PROXY,
                    type_params=MetricTypeParams(measures=[measure_name]),
                )
            ],
        )
    )


def test_metric_no_time_dim() -> None:  # noqa:D
    with pytest.raises(ModelValidationException):
        dim_name = "country"
        measure_name = "foo"
        model_validator = ModelValidator()
        model_validator.checked_validations(
            UserConfiguredModel(
                entities=[
                    entity_with_guaranteed_meta(
                        name="sum_measure",
                        sql_query="SELECT foo, country FROM bar",
                        measures=[Measure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.CATEGORICAL,
                            )
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    )
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name="metric_with_no_time_dim",
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
            )
        )


def test_metric_multiple_primary_time_dims() -> None:  # noqa:D
    with pytest.raises(ModelValidationException):
        dim_name = "date_created"
        dim2_name = "date_deleted"
        measure_name = "foo"
        model_validator = ModelValidator()
        model_validator.checked_validations(
            UserConfiguredModel(
                entities=[
                    entity_with_guaranteed_meta(
                        name="sum_measure",
                        sql_query="SELECT foo, date_created, date_deleted FROM bar",
                        measures=[Measure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            ),
                            Dimension(
                                name=dim2_name,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            ),
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    )
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name="foo",
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
            )
        )


def test_generated_metrics_only() -> None:  # noqa:D
    dim_reference = DimensionReference(element_name="dim")

    dim2_reference = TimeDimensionReference(element_name=DEFAULT_DS)
    measure_name = "measure"
    identifier_reference = IdentifierReference(element_name="primary")
    entity = entity_with_guaranteed_meta(
        name="dim1",
        sql_query=f"SELECT {dim_reference.element_name}, {measure_name} FROM bar",
        measures=[Measure(name=measure_name, agg=AggregationType.SUM, agg_time_dimension=dim2_reference.element_name)],
        dimensions=[
            Dimension(name=dim_reference.element_name, type=DimensionType.CATEGORICAL),
            Dimension(
                name=dim2_reference.element_name,
                type=DimensionType.TIME,
                type_params=DimensionTypeParams(
                    is_primary=True,
                    time_granularity=TimeGranularity.DAY,
                ),
            ),
        ],
        mutability=Mutability(type=MutabilityType.IMMUTABLE),
        identifiers=[
            Identifier(name=identifier_reference.element_name, type=IdentifierType.PRIMARY),
        ],
    )
    entity.measures[0].create_metric = True

    ModelValidator().checked_validations(
        UserConfiguredModel(
            entities=[entity],
            metrics=[],
        )
    )


def test_derived_metric() -> None:  # noqa: D
    measure_name = "foo"
    model_validator = ModelValidator([DerivedMetricRule()])
    result = model_validator.validate_model(
        UserConfiguredModel(
            entities=[
                entity_with_guaranteed_meta(
                    name="sum_measure",
                    sql_query="SELECT foo, ds FROM bar",
                    measures=[
                        Measure(
                            name=measure_name,
                            agg=AggregationType.SUM,
                            agg_time_dimension="ds",
                        )
                    ],
                    dimensions=[
                        Dimension(
                            name="ds",
                            type=DimensionType.TIME,
                            type_params=DimensionTypeParams(
                                is_primary=True,
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                    ],
                    mutability=Mutability(type=MutabilityType.IMMUTABLE),
                ),
            ],
            metrics=[
                metric_with_guaranteed_meta(
                    name="random_metric",
                    type=MetricType.MEASURE_PROXY,
                    type_params=MetricTypeParams(measures=[measure_name]),
                ),
                metric_with_guaranteed_meta(
                    name="random_metric2",
                    type=MetricType.MEASURE_PROXY,
                    type_params=MetricTypeParams(measures=[measure_name]),
                ),
                metric_with_guaranteed_meta(
                    name="alias_collision",
                    type=MetricType.DERIVED,
                    type_params=MetricTypeParams(
                        expr="random_metric2 * 2",
                        metrics=[
                            MetricInput(name="random_metric", alias="random_metric2"),
                            MetricInput(name="random_metric2"),
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="doesntexist",
                    type=MetricType.DERIVED,
                    type_params=MetricTypeParams(expr="notexist * 2", metrics=[MetricInput(name="notexist")]),
                ),
                metric_with_guaranteed_meta(
                    name="has_valid_time_window_params",
                    type=MetricType.DERIVED,
                    type_params=MetricTypeParams(
                        expr="random_metric / random_metric3",
                        metrics=[
                            MetricInput(name="random_metric", offset_window="3 weeks"),
                            MetricInput(name="random_metric", offset_to_grain="month", alias="random_metric3"),
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="has_both_time_offset_params_on_same_input_metric",
                    type=MetricType.DERIVED,
                    type_params=MetricTypeParams(
                        expr="random_metric * 2",
                        metrics=[MetricInput(name="random_metric", offset_window="3 weeks", offset_to_grain="month")],
                    ),
                ),
            ],
        )
    )
    build_issues = result.issues.errors
    assert len(build_issues) == 3
    expected_substr1 = "is already being used. Please choose another alias"
    expected_substr2 = "does not exist as a configured metric in the model"
    expected_substr3 = "Both offset_window and offset_to_grain set"
    missing_error_strings = set()
    for expected_str in [expected_substr1, expected_substr2, expected_substr3]:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build_issues):
            missing_error_strings.add(expected_str)
    assert (
        len(missing_error_strings) == 0
    ), f"Failed to match one or more expected errors: {missing_error_strings} in {set([x.as_readable_str() for x in build_issues])}"
