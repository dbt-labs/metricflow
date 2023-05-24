from __future__ import annotations

import pytest
from dbt_semantic_interfaces.dataclass_serialization import DataClassDeserializer, DataclassSerializer

from metricflow.instances import InstanceSet
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository


@pytest.fixture
def serializer() -> DataclassSerializer:  # noqa: D
    return DataclassSerializer()


@pytest.fixture
def deserializer() -> DataClassDeserializer:  # noqa: D
    return DataClassDeserializer()


def test_serialization(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    serializer: DataclassSerializer,
    deserializer: DataClassDeserializer,
) -> None:
    for _, data_set in consistent_id_object_repository.simple_model_data_sets.items():
        serialized_obj = serializer.pydantic_serialize(data_set.instance_set)
        deserialized_obj = deserializer.pydantic_deserialize(dataclass_type=InstanceSet, serialized_obj=serialized_obj)
        assert data_set.instance_set == deserialized_obj
