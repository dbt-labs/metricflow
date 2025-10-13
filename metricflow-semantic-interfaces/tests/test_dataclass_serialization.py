from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Protocol, Tuple

import pytest
from metricflow_semantic_interfaces.dataclass_serialization import (
    DataClassDeserializer,
    DataclassSerializer,
    SerializableDataclass,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def dataclass_serializer() -> DataclassSerializer:  # noqa: D103
    return DataclassSerializer()


@pytest.fixture
def dataclass_deserializer() -> DataClassDeserializer:  # noqa: D103
    return DataClassDeserializer()


@dataclass(frozen=True)
class SimpleDataclass(SerializableDataclass):  # noqa: D101
    field0: int = -1


@dataclass(frozen=True)
class NestedDataclass(SerializableDataclass):  # noqa: D101
    field1: SimpleDataclass


@dataclass(frozen=True)
class DeeplyNestedDataclass(SerializableDataclass):  # noqa: D101
    field2: NestedDataclass


@dataclass(frozen=True)
class DataclassWithOptional(SerializableDataclass):  # noqa: D101
    field3: Optional[SimpleDataclass] = None
    field4: Optional[SimpleDataclass] = None


@dataclass(frozen=True)
class DataclassWithTuple(SerializableDataclass):  # noqa: D101
    field5: Tuple[SimpleDataclass, ...]


class SimpleProtocol(Protocol):  # noqa: D101
    field6: int


@dataclass(frozen=True)
class SimpleClassWithProtocol(SimpleProtocol, SerializableDataclass):  # noqa: D101
    field6: int


@dataclass(frozen=True)
class NestedDataclassWithProtocol(SerializableDataclass):  # noqa: D101
    field7: SimpleClassWithProtocol


@dataclass(frozen=True)
class DataclassWithDefaultTuple(SerializableDataclass):  # noqa: D101
    field8: Tuple[DataclassWithOptional, ...] = tuple()


@dataclass(frozen=True)
class DataclassWithDataclassDefault(SerializableDataclass):  # noqa: D101
    field9: SimpleDataclass = SimpleDataclass(field0=-10)


@dataclass(frozen=True)
class DataclassWithPrimitiveTypes(SerializableDataclass):  # noqa: D101
    field0: int
    field1: float
    field2: bool
    field3: str


def test_simple_dataclass(  # noqa: D103
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = SimpleDataclass(field0=1)
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(SimpleDataclass, serialized_obj=serialized_object)
    assert obj == deserialized_object


def test_nested_dataclass(  # noqa: D103
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = NestedDataclass(field1=SimpleDataclass(field0=1))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)
    deserialized_object = dataclass_deserializer.pydantic_deserialize(NestedDataclass, serialized_obj=serialized_object)
    assert obj == deserialized_object


def test_deeply_nested_dataclass(  # noqa: D103
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = DeeplyNestedDataclass(field2=NestedDataclass(field1=SimpleDataclass(field0=1)))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DeeplyNestedDataclass, serialized_obj=serialized_object
    )
    assert obj == deserialized_object


def test_dataclass_with_optional(  # noqa: D103
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
    """Tests application of default value when deserializing a sparse optional.

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


def test_dataclass_with_tuple(  # noqa: D103
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = DataclassWithTuple(field5=(SimpleDataclass(field0=1),))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DataclassWithTuple, serialized_obj=serialized_object
    )
    assert obj == deserialized_object


def test_dataclass_deserialization_with_tuple_default(dataclass_deserializer: DataClassDeserializer) -> None:
    """Tests application of default tuple value when deserializing a sparse tuple dataclass container.

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


def test_dataclass_with_protocol(  # noqa: D103
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = SimpleClassWithProtocol(field6=1)
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        SimpleClassWithProtocol, serialized_obj=serialized_object
    )
    assert obj == deserialized_object


def test_nested_dataclass_with_protocol(  # noqa: D103
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    obj = NestedDataclassWithProtocol(field7=SimpleClassWithProtocol(field6=1))
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        NestedDataclassWithProtocol, serialized_obj=serialized_object
    )
    assert obj == deserialized_object


def test_nested_dataclass_deserialization_with_primitive_default(dataclass_deserializer: DataClassDeserializer) -> None:
    """Tests application of default value when deserializing a sparse primitive type dataclass container.

    This ensures a default set on a primitive, with nesting, still deserializes correctly even if the value is
    not present.
    """
    serialized_object = r'{"field1": {}}'

    deserialized_object = dataclass_deserializer.pydantic_deserialize(NestedDataclass, serialized_obj=serialized_object)
    assert deserialized_object == NestedDataclass(field1=SimpleDataclass())
    # Verify default
    assert deserialized_object.field1.field0 == -1


def test_dataclass_deserialization_with_dataclass_default(dataclass_deserializer: DataClassDeserializer) -> None:
    """Tests application of default value when deserializing a dataclass with a default dataclass value set."""
    serialized_object = r"{}"

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DataclassWithDataclassDefault, serialized_obj=serialized_object
    )
    assert deserialized_object == DataclassWithDataclassDefault(field9=SimpleDataclass(field0=-10))
    # Verify default
    assert deserialized_object.field9.field0 == -10


def test_all_primitive_types(
    dataclass_serializer: DataclassSerializer, dataclass_deserializer: DataClassDeserializer
) -> None:
    """Tests a dataclass with all supported primitive types."""
    obj = DataclassWithPrimitiveTypes(
        field0=1,
        field1=2.0,
        field2=True,
        field3="foo",
    )
    logger.error(f"dataclass_serializer is {dataclass_serializer}")
    serialized_object = dataclass_serializer.pydantic_serialize(obj)

    deserialized_object = dataclass_deserializer.pydantic_deserialize(
        DataclassWithPrimitiveTypes, serialized_obj=serialized_object
    )
    assert obj == deserialized_object
