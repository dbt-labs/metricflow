import pytest

from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.data_source import Mutability, MutabilityType, DataSource
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.objects.elements.measure import Measure, AggregationType
from metricflow.model.objects.metric import MetricType, MetricTypeParams, Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.specs import DimensionReference, MeasureReference, TimeDimensionReference
from metricflow.test.model.validations.helpers import data_source_with_guaranteed_meta, metric_with_guaranteed_meta
from metricflow.time.time_granularity import TimeGranularity


def test_incompatible_dimension_type() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"type conflict for dimension"):
        dim_name = "dim"
        measure_name = "measure"
        model_validator = ModelValidator()
        model_validator.checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_name}, {measure_name} FROM bar",
                        measures=[Measure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                    data_source_with_guaranteed_meta(
                        name="categoricaldim",
                        sql_query="SELECT foo FROM bar",
                        dimensions=[Dimension(name=dim_name, type=DimensionType.CATEGORICAL)],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
                materializations=[],
            )
        )


def test_incompatible_dimension_is_partition() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"conflicting is_partition attribute for dimension"):
        dim_name = "dim1"
        measure_name = "measure"
        model_validator = ModelValidator()
        model_validator.checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_name}, {measure_name} FROM bar",
                        measures=[Measure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                is_partition=True,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                    data_source_with_guaranteed_meta(
                        name="dim2",
                        sql_query="SELECT foo1 FROM bar",
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                is_partition=False,
                                type_params=DimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
                materializations=[],
            )
        )


def test_multiple_primary_time_dimensions() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"one of many defined as primary"):
        dimension_reference = TimeDimensionReference(element_name="ds")
        dimension_reference2 = DimensionReference(element_name="not_ds")
        measure_reference = MeasureReference(element_name="measure")
        model_validator = ModelValidator()
        model_validator.checked_validations(
            model=UserConfiguredModel(
                data_sources=[
                    DataSource(
                        name="dim1",
                        sql_query=f"SELECT ds, {measure_reference.element_name} FROM bar",
                        measures=[
                            Measure(
                                name=measure_reference.element_name,
                                agg=AggregationType.SUM,
                                agg_time_dimension=dimension_reference.element_name,
                            )
                        ],
                        dimensions=[
                            Dimension(
                                name=dimension_reference.element_name,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            ),
                            Dimension(
                                name=dimension_reference2.element_name,
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
                    Metric(
                        name=measure_reference.element_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_reference.element_name]),
                    )
                ],
                materializations=[],
            )
        )
