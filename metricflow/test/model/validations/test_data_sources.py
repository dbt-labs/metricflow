import pytest

from dbt.contracts.graph.entities import MutabilityType, Mutability
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.test.model.validations.helpers import entity_with_guaranteed_meta
from metricflow.time.time_granularity import TimeGranularity


@pytest.mark.skip("TODO: Will convert to validation rule")
def test_entity_invalid_sql() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"Invalid SQL"):
        entity_with_guaranteed_meta(
            name="invalid_sql_source",
            sql_query="SELECT foo FROM bar;",
            dimensions=[
                Dimension(
                    name="ds",
                    type=DimensionType.TIME,
                    type_params=DimensionTypeParams(
                        time_granularity=TimeGranularity.DAY,
                    ),
                )
            ],
            mutability=Mutability(type=MutabilityType.IMMUTABLE),
        )
