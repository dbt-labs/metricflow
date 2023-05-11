import pytest
import textwrap

from dbt_semantic_interfaces.objects.aggregation_type import AggregationType
from metricflow.model.model_validator import ModelValidator
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_validation_ready_model
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from dbt_semantic_interfaces.objects.elements.entity import Entity, EntityType
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.metric import MetricInput, MetricType, MetricTypeParams
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import DimensionReference, EntityReference, TimeDimensionReference
from metricflow.model.validations.metrics import DerivedMetricRule, MetricConstraintAliasesRule
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.test.fixtures.table_fixtures import DEFAULT_DS
from metricflow.test.model.validations.helpers import semantic_model_with_guaranteed_meta, metric_with_guaranteed_meta
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity


def test_metric_no_time_dim_dim_only_source() -> None:  # noqa:D
    dim_name = "country"
    dim2_name = "ename"
    measure_name = "foo"
    model_validator = ModelValidator()
    model_validator.checked_validations(
        UserConfiguredModel(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[],
                    dimensions=[Dimension(name=dim_name, type=DimensionType.CATEGORICAL)],
                ),
                semantic_model_with_guaranteed_meta(
                    name="sum_measure2",
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
                ),
            ],
            metrics=[
                metric_with_guaranteed_meta(
                    name="metric_with_no_time_dim",
                    type=MetricType.MEASURE_PROXY,
                    type_params=MetricTypeParams(measure=measure_name),
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
                semantic_models=[
                    semantic_model_with_guaranteed_meta(
                        name="sum_measure",
                        measures=[Measure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.CATEGORICAL,
                            )
                        ],
                    )
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name="metric_with_no_time_dim",
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measure=measure_name),
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
                semantic_models=[
                    semantic_model_with_guaranteed_meta(
                        name="sum_measure",
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
                    )
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name="foo",
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measure=measure_name),
                    )
                ],
            )
        )


def test_generated_metrics_only() -> None:  # noqa:D
    dim_reference = DimensionReference(element_name="dim")

    dim2_reference = TimeDimensionReference(element_name=DEFAULT_DS)
    measure_name = "measure"
    entity_reference = EntityReference(element_name="primary")
    semantic_model = semantic_model_with_guaranteed_meta(
        name="dim1",
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
        entities=[
            Entity(name=entity_reference.element_name, type=EntityType.PRIMARY),
        ],
    )
    semantic_model.measures[0].create_metric = True

    ModelValidator().checked_validations(
        UserConfiguredModel(
            semantic_models=[semantic_model],
            metrics=[],
        )
    )


def test_derived_metric() -> None:  # noqa: D
    measure_name = "foo"
    model_validator = ModelValidator([DerivedMetricRule()])
    model_issues = model_validator.validate_model(
        UserConfiguredModel(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
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
                ),
            ],
            metrics=[
                metric_with_guaranteed_meta(
                    name="random_metric",
                    type=MetricType.MEASURE_PROXY,
                    type_params=MetricTypeParams(measure=measure_name),
                ),
                metric_with_guaranteed_meta(
                    name="random_metric2",
                    type=MetricType.MEASURE_PROXY,
                    type_params=MetricTypeParams(measure=measure_name),
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
    build_issues = model_issues.errors
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


def test_metric_alias_is_set_when_required() -> None:
    """Tests to ensure that an appropriate error appears when a required alias is missing"""
    metric_name = "num_sample_rows"
    yaml_contents = textwrap.dedent(
        f"""\
        semantic_model:
          name: sample_semantic_model
          node_relation:
            schema_name: some_schema
            alias: source_table
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: {metric_name}
              agg: sum
              expr: 1
              create_metric: true
          dimensions:
            - name: is_instant
              type: categorical
            - name: ds
              type: time
              type_params:
                time_granularity: day
                is_primary: true
        ---
        metric:
          name: "metric1"
          type: derived
          type_params:
            expr: {metric_name} + {metric_name}
            metrics:
              - name: {metric_name}
              - name: {metric_name}
                constraint: is_instant
        """
    )
    missing_alias_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_model([missing_alias_file])

    model_issues = ModelValidator([MetricConstraintAliasesRule()]).validate_model(model.model)

    assert len(model_issues.errors) == 1
    expected_error_substring = f"depends on multiple different constrained versions of metric {metric_name}"
    actual_error = model_issues.errors[0].as_readable_str()
    assert (
        actual_error.find(expected_error_substring) != -1
    ), f"Expected error {expected_error_substring} not found in error string! Instead got {actual_error}"