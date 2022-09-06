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
    field0: int = -1


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


@dataclass(frozen=True)
class DataclassWithDefaultTuple(SerializableDataclass):  # noqa: D
    field8: Tuple[DataclassWithOptional, ...] = tuple()


@dataclass(frozen=True)
class DataclassWithDataclassDefault(SerializableDataclass):  # noqa: D
    field9: SimpleDataclass = SimpleDataclass(field0=-10)


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


def test_dataclass_deserialization_with_optional_default(dataclass_deserializer: DataClassDeserializer) -> None:
    """Tests application of default value when deserializing a sparse optional

    This is necessary to support cases where new optional fields are added and defaulted None, but serialized
    values - which should be compatible - exist without the field in place
    """
    serialized_object = r'{"field4": {"field0": 5}}'

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DataclassWithOptional, serialized_obj=serialized_object
    )
    assert deserialized_object == DataclassWithOptional(field4=SimpleDataclass(field0=5))
    # Verify default
    assert deserialized_object.field3 is None


def test_dataclass_with_tuple(  # noqa: D
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = DataclassWithTuple(field5=(SimpleDataclass(field0=1),))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DataclassWithTuple, serialized_obj=serialized_object
    )
    assert obj == deserialized_object


def test_dataclass_deserialization_with_tuple_default(dataclass_deserializer: DataClassDeserializer) -> None:
    """Tests application of default tuple value when deserializing a sparse tuple dataclass container

    As with Optional, this is necessary to support cases where a default is set for a field added after
    some instances were already serialized, but with an empty tuple in place as a default.
    """
    serialized_object = r"{}"

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DataclassWithDefaultTuple, serialized_obj=serialized_object
    )
    assert deserialized_object == DataclassWithDefaultTuple()
    # Verify default
    assert deserialized_object.field8 == tuple()


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


def test_nested_dataclass_deserialization_with_primitive_default(dataclass_deserializer: DataClassDeserializer) -> None:
    """Tests application of default value when deserializing a sparse primitive type dataclass container

    This ensures a default set on a primitive, with nesting, still deserializes correctly even if the value is
    not present.
    """
    serialized_object = r'{"field1": {}}'

    deserialized_object = dataclass_deserializer.pydantic_deserialize(NestedDataclass, serialized_obj=serialized_object)
    assert deserialized_object == NestedDataclass(field1=SimpleDataclass())
    # Verify default
    assert deserialized_object.field1.field0 == -1


def test_dataclass_deserialization_with_dataclass_default(dataclass_deserializer: DataClassDeserializer) -> None:
    """Tests application of default value when deserializing a dataclass with a default dataclass value set"""
    serialized_object = r"{}"

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DataclassWithDataclassDefault, serialized_obj=serialized_object
    )
    assert deserialized_object == DataclassWithDataclassDefault(field9=SimpleDataclass(field0=-10))
    # Verify default
    assert deserialized_object.field9.field0 == -10
