import pytest

from metricflow.dataclass_serialization import DataclassSerializer, DataClassDeserializer
from metricflow.sql.sql_bind_parameters import SqlBindParameters, SqlBindParameter, SqlBindParameterValue


@pytest.fixture
def serializer() -> DataclassSerializer:  # noqa: D
    return DataclassSerializer()


@pytest.fixture
def deserializer() -> DataClassDeserializer:  # noqa: D
    return DataClassDeserializer()


def test_serialization(  # noqa: D
    serializer: DataclassSerializer,
    deserializer: DataClassDeserializer,
) -> None:
    bind_parameters = SqlBindParameters(
        param_items=(
            SqlBindParameter("key0", SqlBindParameterValue.create_from_sql_column_type("value0")),
            SqlBindParameter("key1", SqlBindParameterValue.create_from_sql_column_type("value1")),
        )
    )
    serialized_obj = serializer.pydantic_serialize(bind_parameters)
    deserialized_obj = deserializer.pydantic_deserialize(
        dataclass_type=SqlBindParameters, serialized_obj=serialized_obj
    )
    assert bind_parameters == deserialized_obj
