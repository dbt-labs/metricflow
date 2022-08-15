import logging
from dataclasses import dataclass
from typing import Optional, Tuple, Protocol

import pytest

from metricflow.dataclass_serialization import SerializableDataclass, DataClassDeserializer, DataclassSerializer

logger = logging.getLogger(__name__)


@pytest.fixture
def dataclass_serializer() -> DataclassSerializer:  # noqa: D
    return DataclassSerializer()


@pytest.fixture
def dataclass_deserializer() -> DataClassDeserializer:  # noqa: D
    return DataClassDeserializer()


@dataclass(frozen=True)
class SimpleDataclass(SerializableDataclass):  # noqa: D
    field0: int


@dataclass(frozen=True)
class NestedDataclass(SerializableDataclass):  # noqa: D
    field1: SimpleDataclass


@dataclass(frozen=True)
class DeeplyNestedDataclass(SerializableDataclass):  # noqa: D
    field2: NestedDataclass


@dataclass(frozen=True)
class DataclassWithOptional(SerializableDataclass):  # noqa: D
    field3: Optional[SimpleDataclass] = None
    field4: Optional[SimpleDataclass] = None


@dataclass(frozen=True)
class DataclassWithTuple(SerializableDataclass):  # noqa: D
    field5: Tuple[SimpleDataclass, ...]


class SimpleProtocol(Protocol):  # noqa: D
    field6: int


@dataclass(frozen=True)
class SimpleClassWithProtocol(SimpleProtocol, SerializableDataclass):  # noqa: D
    field6: int


@dataclass(frozen=True)
class NestedDataclassWithProtocol(SerializableDataclass):  # noqa: D
    field7: SimpleClassWithProtocol


def test_simple_dataclass(  # noqa: D
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = SimpleDataclass(field0=1)
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(SimpleDataclass, serialized_obj=serialized_object)
    assert obj == deserialized_object


def test_nested_dataclass(  # noqa: D
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = NestedDataclass(field1=SimpleDataclass(field0=1))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)
    deserialized_object = dataclass_deserializer.pydantic_deserialize(NestedDataclass, serialized_obj=serialized_object)
    assert obj == deserialized_object


def test_deeply_nested_dataclass(  # noqa: D
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = DeeplyNestedDataclass(field2=NestedDataclass(field1=SimpleDataclass(field0=1)))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DeeplyNestedDataclass, serialized_obj=serialized_object
    )
    assert obj == deserialized_object


def test_dataclass_with_optional(  # noqa: D
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    # DataclassWithOptional has 2 optional fields, so this tests the None and not-None cases.
    obj = DataclassWithOptional(field4=SimpleDataclass(field0=1))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DataclassWithOptional, serialized_obj=serialized_object
    )
    assert obj == deserialized_object


def test_dataclass_with_tuple(  # noqa: D
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = DataclassWithTuple(field5=(SimpleDataclass(field0=1),))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DataclassWithTuple, serialized_obj=serialized_object
    )
    assert obj == deserialized_object


def test_dataclass_with_protocol(  # noqa: D
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = SimpleClassWithProtocol(field6=1)
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        SimpleClassWithProtocol, serialized_obj=serialized_object
    )
    assert obj == deserialized_object


def test_nested_dataclass_with_protocol(  # noqa: D
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = NestedDataclassWithProtocol(field7=SimpleClassWithProtocol(field6=1))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        NestedDataclassWithProtocol, serialized_obj=serialized_object
    )
    assert obj == deserialized_object
