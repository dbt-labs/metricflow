import pytest

from metricflow.aggregation_properties import AggregationType
from dbt.semantic.validations.model_validator import ModelValidator
from dbt.contracts.graph.entities import Mutability, MutabilityType
from dbt.contracts.graph.dimensions import Dimension, DimensionType, DimensionTypeParams
from dbt.contracts.graph.measures import Measure
from dbt.contracts.graph.metrics import MetricType, MetricTypeParams
from dbt.contracts.graph.nodes import Metric, Entity
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.validations.entities import EntityTimeDimensionWarningsRule
from metricflow.model.validations.dimension_const import DimensionConsistencyRule
from metricflow.model.validations.validator_helpers import ModelValidationException
from dbt.semantic.references import DimensionReference, MeasureReference, TimeDimensionReference
from metricflow.test.model.validations.helpers import entity_with_guaranteed_meta, metric_with_guaranteed_meta
from dbt.semantic.time import TimeGranularity


def test_incompatible_dimension_type() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"type conflict for dimension"):
        dim_name = "dim"
        measure_name = "measure"
        model_validator = ModelValidator([DimensionConsistencyRule()])
        model_validator.checked_validations(
            UserConfiguredModel(
                entities=[
                    entity_with_guaranteed_meta(
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
                    entity_with_guaranteed_meta(
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
            )
        )


def test_incompatible_dimension_is_partition() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"conflicting is_partition attribute for dimension"):
        dim_name = "dim1"
        measure_name = "measure"
        model_validator = ModelValidator([DimensionConsistencyRule()])
        model_validator.checked_validations(
            UserConfiguredModel(
                entities=[
                    entity_with_guaranteed_meta(
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
                    entity_with_guaranteed_meta(
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
            )
        )


def test_multiple_primary_time_dimensions() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"one of many defined as primary"):
        dimension_reference = TimeDimensionReference(element_name="ds")
        dimension_reference2 = DimensionReference(element_name="not_ds")
        measure_reference = MeasureReference(element_name="measure")
        model_validator = ModelValidator([EntityTimeDimensionWarningsRule()])
        model_validator.checked_validations(
            model=UserConfiguredModel(
                entities=[
                    Entity(
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
            )
        )
