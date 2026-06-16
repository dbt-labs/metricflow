from __future__ import annotations

from typing import Iterable, Sequence, Type

from metricflow_semantic_interfaces.dataclass_serialization import (
    DataClassDeserializer,
    DataclassSerializer,
    SerializableDataclass,
)


def assert_includes_all_serializable_dataclass_types(
    instances: Sequence[SerializableDataclass], excluded_classes: Iterable[Type[SerializableDataclass]]
) -> None:
    """Verify that the given instances include at least one instance of the known subclasses."""
    instance_types = {type(instance) for instance in instances}
    missing_instance_types = (
        set(SerializableDataclass.concrete_subclasses_for_testing())
        .difference(instance_types)
        .difference(excluded_classes)
    )
    missing_type_names = sorted(instance_type.__name__ for instance_type in missing_instance_types)
    assert (
        len(missing_type_names) == 0
    ), f"Missing instances of the following classes: {missing_type_names}. Please add them."


def assert_serializable(instances: Sequence[SerializableDataclass]) -> None:
    """Verify that the given instances are actually serializable."""
    serializer = DataclassSerializer()
    deserializer = DataClassDeserializer()

    for instance in instances:
        try:
            serialized_output = serializer.pydantic_serialize(instance)
            deserialized_instance = deserializer.pydantic_deserialize(type(instance), serialized_output)
        except Exception as e:
            raise AssertionError(f"Error serializing {instance=}") from e

        assert instance == deserialized_instance
