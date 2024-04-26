from __future__ import annotations

import pytest
from dbt_semantic_interfaces.dataclass_serialization import DataClassDeserializer, DataclassSerializer
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameter, SqlBindParameters, SqlBindParameterValue


@pytest.fixture
def serializer() -> DataclassSerializer:  # noqa: D103
    return DataclassSerializer()


@pytest.fixture
def deserializer() -> DataClassDeserializer:  # noqa: D103
    return DataClassDeserializer()


def test_serialization(  # noqa: D103
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
