import pytest

from metricflow.model.objects.data_source import DataSource, MutabilityType, Mutability
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.specs import DimensionReference
from metricflow.time.time_granularity import TimeGranularity


@pytest.mark.skip("TODO: Will convert to validation rule")
def test_data_source_invalid_sql() -> None:  # noqa:D
    dimension_reference = DimensionReference(element_name="ds")
    with pytest.raises(ModelValidationException, match=r"Invalid SQL"):
        DataSource(
            name="invalid_sql_source",
            sql_query="SELECT foo FROM bar;",
            dimensions=[
                Dimension(
                    name=dimension_reference,
                    type=DimensionType.TIME,
                    type_params=DimensionTypeParams(
                        time_format="YYYY-MM-DD",
                        time_granularity=TimeGranularity.DAY,
                    ),
                )
            ],
            mutability=Mutability(type=MutabilityType.IMMUTABLE),
        )
