import pytest

from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.data_source import DataSource, Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.objects.elements.measure import Measure, AggregationType
from metricflow.model.objects.metric import Metric, MetricType, MetricTypeParams
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.specs import MeasureReference, DimensionReference
from metricflow.time.time_granularity import TimeGranularity
from metricflow.model.validations.validator_helpers import ModelValidationException


@pytest.mark.skip("TODO: Will convert to validation rule")
def test_inconsistent_elements() -> None:  # noqa:D
    dim_reference = DimensionReference(element_name="ename")
    measure_reference = MeasureReference(element_name="ename")
    with pytest.raises(ModelValidationException):
        ModelValidator.checked_validations(
            UserConfiguredModel(
                data_sources=[
                    DataSource(
                        name="s1",
                        sql_query="SELECT foo FROM bar",
                        dimensions=[
                            Dimension(
                                name=dim_reference,
                                type_=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    time_format="YYYY-MM-DD",
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                    DataSource(
                        name="s2",
                        sql_query="SELECT foo FROM bar",
                        measures=[Measure(name=measure_reference, agg=AggregationType.SUM)],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    Metric(
                        name=measure_reference.element_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_reference]),
                    )
                ],
                materializations=[],
            )
        )
