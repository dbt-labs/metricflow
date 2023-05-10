import pytest

from dbt_semantic_interfaces.objects.aggregation_type import AggregationType
from metricflow.model.model_validator import ModelValidator
from dbt_semantic_interfaces.objects.data_source import DataSource, NodeRelation
from dbt_semantic_interfaces.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.metric import MetricType, MetricTypeParams, Metric
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import DimensionReference, MeasureReference, TimeDimensionReference
from metricflow.model.validations.data_sources import DataSourceTimeDimensionWarningsRule
from metricflow.model.validations.dimension_const import DimensionConsistencyRule
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.test.model.validations.helpers import data_source_with_guaranteed_meta, metric_with_guaranteed_meta
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity


def test_incompatible_dimension_type() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"type conflict for dimension"):
        dim_name = "dim"
        measure_name = "measure"
        model_validator = ModelValidator([DimensionConsistencyRule()])
        model_validator.checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
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
                    ),
                    data_source_with_guaranteed_meta(
                        name="categoricaldim",
                        dimensions=[Dimension(name=dim_name, type=DimensionType.CATEGORICAL)],
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
            )
        )


def test_incompatible_dimension_is_partition() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"conflicting is_partition attribute for dimension"):
        dim_name = "dim1"
        measure_name = "measure"
        model_validator = ModelValidator([DimensionConsistencyRule()])
        model_validator.checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
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
                    ),
                    data_source_with_guaranteed_meta(
                        name="dim2",
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
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
            )
        )


def test_multiple_primary_time_dimensions() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"one of many defined as primary"):
        dimension_reference = TimeDimensionReference(element_name="ds")
        dimension_reference2 = DimensionReference(element_name="not_ds")
        measure_reference = MeasureReference(element_name="measure")
        model_validator = ModelValidator([DataSourceTimeDimensionWarningsRule()])
        model_validator.checked_validations(
            model=UserConfiguredModel(
                data_sources=[
                    DataSource(
                        name="dim1",
                        node_relation=NodeRelation(
                            alias="table",
                            schema_name="schema",
                        ),
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
                    ),
                ],
                metrics=[
                    Metric(
                        name=measure_reference.element_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_reference.element_name]),
                    )
                ],
            )
        )
