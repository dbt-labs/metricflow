from __future__ import annotations

import dataclasses
import datetime
import logging
from builtins import NameError
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Type, TypeVar, Tuple, Union, get_args, get_origin, get_type_hints
from typing_extensions import TypeAlias

import pydantic
from pydantic import BaseModel

from metricflow.object_utils import pformat_big_objects

logger = logging.getLogger(__name__)


# Any Pydantic object
PydanticT = TypeVar("PydanticT", bound=Type[BaseModel])
# Any value
AnyValueType: TypeAlias = Any  # type: ignore[misc]


class UnknownClassError(Exception):
    """Raised when there's an issue getting type information for a SerializableDataclass"""

    pass


class DataclassDeserializationError(Exception):
    """Raised when there is any error deserializing a dataclass."""

    pass


def _get_dataclass_field_definitions(dataclass_type: Type) -> Dict[str, FieldDefinition]:
    """Returns the types of fields in a dataclass. Returns a dict from the name of the field to the type."""
    assert dataclasses.is_dataclass(dataclass_type)
    try:
        type_hints = get_type_hints(dataclass_type, localns={})
        fields = dataclasses.fields(dataclass_type)
        return {
            field.name: FieldDefinition(field_type=type_hints[field.name], default_value=field.default)
            for field in fields
        }

    except NameError as e:
        raise UnknownClassError(
            f"Error getting type hints for dataclass {dataclass_type}. Please see the nested exception as a required "
            f"class may not be imported properly."
        ) from e


def _is_optional_type(type_to_check: Type) -> bool:
    """Returns true if the given type is an optional type. Python represents optional as Union[SomeType, NoneType]."""
    if get_origin(type_to_check) is Union:
        args = get_args(type_to_check)
        if len(args) == 2 and issubclass(args[1], type(None)):
            return True
    return False


def _get_type_parameter_for_optional(type_to_check: Type) -> Type:
    """Given Union[SomeType, NoneType], return SomeType."""
    assert _is_optional_type(type_to_check)
    return get_args(type_to_check)[0]


def _is_supported_field_type_in_serializable_dataclass(field_type: Type) -> bool:
    """Returns if a type of a given field in a SerializableDatclass is supported.

    For container classes, this does not check the type of the type parameter for the container.
    """
    return (
        (
            _is_sequence_like_tuple_type(field_type)
            and _is_supported_field_type_in_serializable_dataclass(
                _get_type_parameter_for_sequence_like_tuple_type(field_type)
            )
        )
        or (
            _is_optional_type(field_type)
            and _is_supported_field_type_in_serializable_dataclass(_get_type_parameter_for_optional(field_type))
        )
        or issubclass(field_type, Enum)
        or issubclass(field_type, SerializableDataclass)
        or issubclass(field_type, int)
        or issubclass(field_type, float)
        or issubclass(field_type, str)
        or issubclass(field_type, datetime.datetime)
        or issubclass(field_type, datetime.timedelta)
        or issubclass(field_type, BaseModel)
    )


def _is_sequence_like_tuple_type(field_type: Type) -> bool:
    """Returns true for tuple types that are like sequences.

    In a dataclass definition:

        foo: Tuple[SomeType, ...]

    would return true, but

       foo: Tuple[SomeType, SomeOtherType]

    would not.
    """
    if get_origin(field_type) is tuple:
        args = get_args(field_type)
        return len(args) == 2 and args[1] is Ellipsis
    return False


def _get_type_parameter_for_sequence_like_tuple_type(field_type: Type) -> Type:
    """Return the type parameter for a sequence like tuple type in a daclass definition.

    e.g.

        foo: Tuple[SomeType, ...]

    should return SomeType.
    """
    assert _is_sequence_like_tuple_type(field_type)
    args = get_args(field_type)
    return args[0]


class SerializableDataclass:
    """Describes a dataclass that can be serialized using DataclassSerializer.

    Previously, Pydnatic has been used for defining objects as it provides built in support for serialization and
    deserialization. However, Pydantic object is slow compared to dataclass initialization, with tests showing 10x-100x
    slower performance. This is an issue if many objects are created, which can happen in during plan generation. Using
    the BaseModel.construct() is still not as fast as dataclass initiaization and it also makes for an awkward developer
    interface. Because of this, MF implements a simple custom serializer / deserializer to work with the built-in
    Python dataclass.

    The dataclass must have concrete types for all fields and not all types are supported. Please see implementation
    details in DataclassSerializer. Not adding post_init checks as there have been previous issues with slow object
    initialization.

    This is a concrete object as MyPy currently throws a type error if a Python dataclass is defined with an abstract
    parent class.
    """

    pass


SerializableDataclassT = TypeVar("SerializableDataclassT", bound=SerializableDataclass)


class DataclassSerializer:
    """Serializer that serializes SerializableDataclasses.

    Pydantic is useful for serialization, but it has issues serializing dataclasses. We've seen issues with forward
    references and recursion errors, but it's helpful for other data types. To serialize dataclasses, this class uses
    the type annotation defined in the dataclass to create a Pydantic model, and then uses Pydantic to serialize to a
    JSON string.
    """

    def __init__(self) -> None:  # noqa: D
        self._to_pydantic_type_converter = DataClassTypeToPydanticTypeConverter()

    def _convert_dataclass_instance_to_pydantic_model(
        self, object_type: Type, obj: Optional[AnyValueType] = None
    ) -> AnyValueType:
        if not _is_supported_field_type_in_serializable_dataclass(object_type):
            raise RuntimeError(f"Unsupported field type: {object_type}")
        elif _is_optional_type(object_type):
            if obj is None:
                return None
            optional_field_type_parameter = _get_type_parameter_for_optional(object_type)
            return self._convert_dataclass_instance_to_pydantic_model(
                object_type=optional_field_type_parameter, obj=obj
            )
        elif _is_sequence_like_tuple_type(object_type):
            if obj is None:
                return None

            tuple_field_type_parameter = _get_type_parameter_for_sequence_like_tuple_type(object_type)
            return tuple(
                self._convert_dataclass_instance_to_pydantic_model(
                    object_type=tuple_field_type_parameter,
                    obj=x,
                )
                for x in obj
            )
        elif issubclass(object_type, SerializableDataclass):
            if not dataclasses.is_dataclass(object_type):
                raise RuntimeError(f"{object_type} is not a dataclass")
            if not isinstance(obj, SerializableDataclass):
                raise RuntimeError(f"{obj} is not a SerializableDataclass")
            PydanticModel = self._to_pydantic_type_converter.to_pydantic_type(object_type)

            field_dict = _get_dataclass_field_definitions(object_type)
            field_values: Dict[str, AnyValueType] = {}
            for field_name, field_definition in field_dict.items():
                field_values[field_name] = self._convert_dataclass_instance_to_pydantic_model(
                    object_type=field_definition.annotated_field_type, obj=getattr(obj, field_name)
                )
            return PydanticModel(**field_values)

        return obj

    def pydantic_serialize(self, obj: SerializableDataclassT) -> str:  # noqa: D
        assert dataclasses.is_dataclass(obj)

        return self._convert_dataclass_instance_to_pydantic_model(
            # .__class__ seems to be the approach for new classes and there are differences with type(obj)
            object_type=obj.__class__,
            obj=obj,
        ).json()


class DataClassDeserializer:
    """Corresponding deserializer for datclasses that were serialized by DataClassSerializer."""

    def __init__(self) -> None:  # noqa: D
        self._to_pydantic_type_converter = DataClassTypeToPydanticTypeConverter()

    def _convert_field_in_pydantic_object_to_actual_object(
        self, field_type: Type, obj: Optional[AnyValueType] = None
    ) -> AnyValueType:
        if not _is_supported_field_type_in_serializable_dataclass(field_type):
            raise RuntimeError(f"Unsupported type: {field_type}")
        elif _is_optional_type(field_type):
            optional_field_type_parameter = _get_type_parameter_for_optional(field_type)
            if obj is None:
                return None

            return self._convert_field_in_pydantic_object_to_actual_object(
                field_type=optional_field_type_parameter,
                obj=obj,
            )
        elif _is_sequence_like_tuple_type(field_type):
            assert isinstance(obj, tuple)
            tuple_type_parameter = _get_type_parameter_for_sequence_like_tuple_type(field_type)
            return tuple(
                self._convert_field_in_pydantic_object_to_actual_object(
                    field_type=tuple_type_parameter,
                    obj=x,
                )
                for x in obj
            )
        elif issubclass(field_type, SerializableDataclass):
            logger.debug(f"Handling field_type={field_type} object={repr(obj)}")
            return self._construct_dataclass_from_pydantic_object(
                dataclass_type=field_type,
                obj=obj,
            )
        else:
            return obj

    def _construct_dataclass_from_pydantic_object(
        self, dataclass_type: Type[SerializableDataclassT], obj: BaseModel
    ) -> SerializableDataclassT:
        logger.debug(f"Constructing dataclass of type {dataclass_type} from {repr(obj)}")
        object_args = {}
        field_dict = _get_dataclass_field_definitions(dataclass_type)
        for field_name, field_definition in field_dict.items():
            object_args[field_name] = self._convert_field_in_pydantic_object_to_actual_object(
                field_type=field_definition.annotated_field_type,
                obj=getattr(obj, field_name),
            )

        return dataclass_type(**object_args)

    def pydantic_deserialize(  # noqa: D
        self, dataclass_type: Type[SerializableDataclassT], serialized_obj: str
    ) -> SerializableDataclassT:

        try:
            ClassAsPydantic = self._to_pydantic_type_converter.to_pydantic_type(dataclass_type)
            logger.debug(f"Serialized object for creation of {ClassAsPydantic} is {serialized_obj}")
            pydantic_object = ClassAsPydantic.parse_raw(serialized_obj)
            return self._construct_dataclass_from_pydantic_object(
                dataclass_type=dataclass_type,
                obj=pydantic_object,
            )

        except Exception as e:
            raise DataclassDeserializationError from e


class DataClassTypeToPydanticTypeConverter:  # noqa: D
    """Class that converts a SerializableDataclass into an equivalent Pydantic object.

    Includes caching to make it efficient.
    """

    def __init__(self) -> None:  # noqa: D
        self._dataclass_type_to_pydantic_type: Dict[Type, Type[BaseModel]] = {}

    def to_pydantic_type(self, dataclass_type: Type[SerializableDataclass]) -> Type[BaseModel]:  # noqa: D
        if dataclass_type not in self._dataclass_type_to_pydantic_type:
            self._dataclass_type_to_pydantic_type[
                dataclass_type
            ] = DataClassTypeToPydanticTypeConverter._convert_dataclass_type_to_pydantic_type(dataclass_type)
        return self._dataclass_type_to_pydantic_type[dataclass_type]

    @staticmethod
    def _convert_dataclass_type_to_pydantic_type(dataclass_type: Type) -> Type[BaseModel]:  # noqa: D
        logger.debug(f"Converting {dataclass_type.__name__} to a pydantic class")
        assert issubclass(dataclass_type, SerializableDataclass)
        assert dataclasses.is_dataclass(dataclass_type)

        field_dict = _get_dataclass_field_definitions(dataclass_type)

        # Maps the name of the field to (type of field, default value)
        fields_for_pydantic_model: Dict[str, Tuple[Type, AnyValueType]] = {}
        logger.debug(f"Need to add: {pformat_big_objects(field_dict.keys())}")
        for field_name, field_definition in field_dict.items():
            field_definition = DataClassTypeToPydanticTypeConverter._convert_nested_fields(field_definition)
            fields_for_pydantic_model[field_name] = field_definition.as_pydantic_field_tuple()
            logger.debug(f"Adding {field_name} with type {field_definition.annotated_field_type}")

        class_name = dataclass_type.__name__ + "AsPydantic"
        logger.debug(
            f"Creating Pydantic model {class_name} with fields:\n{pformat_big_objects(fields_for_pydantic_model)}"
        )
        pydantic_model = pydantic.create_model(class_name, **fields_for_pydantic_model)
        logger.debug(f"Finished creating Pydantic model {class_name}")
        logger.debug(f"Finished converting {dataclass_type.__name__} to a pydantic class")
        return pydantic_model

    @staticmethod
    def _convert_nested_fields(field_definition: FieldDefinition) -> FieldDefinition:
        """Recursively converts a given field definition into a fully serializable type specification

        The initial set of FieldDefinitions sourced from the dataclass might contain arbitrarily
        nested SerializableDataclass objects, which would need further parsing in order to be fully
        serializable via our Pydantic conversion. This method does that traversal and returns the complete
        set of Pydantic-compatible nested types.
        """
        if not _is_supported_field_type_in_serializable_dataclass(field_definition.annotated_field_type):
            raise RuntimeError(f"Unsupported type: {field_definition.annotated_field_type}")
        elif _is_optional_type(field_definition.annotated_field_type):
            optional_field_type_parameter = _get_type_parameter_for_optional(field_definition.annotated_field_type)
            converted_field_definition = DataClassTypeToPydanticTypeConverter._convert_nested_fields(
                FieldDefinition(field_type=optional_field_type_parameter)
            )
            return FieldDefinition(  # type: ignore[arg-type]
                Union[converted_field_definition.field_type, type(None)], field_definition.default_value
            )
        elif _is_sequence_like_tuple_type(field_definition.annotated_field_type):
            tuple_field_type_parameter = _get_type_parameter_for_sequence_like_tuple_type(
                field_definition.annotated_field_type
            )
            converted_field_definition = DataClassTypeToPydanticTypeConverter._convert_nested_fields(
                FieldDefinition(field_type=tuple_field_type_parameter)
            )
            return FieldDefinition(Tuple[converted_field_definition.field_type, ...], field_definition.default_value)
        elif issubclass(field_definition.annotated_field_type, SerializableDataclass):
            return FieldDefinition(
                field_type=DataClassTypeToPydanticTypeConverter._convert_dataclass_type_to_pydantic_type(
                    field_definition.annotated_field_type
                ),
                default_value=field_definition.default_value,
            )
        else:
            return field_definition


@dataclass(frozen=True)
class FieldDefinition:
    """Describes the field definition in a dataclass as described by the annotation.

    Note the default_value follows Pydantic semantics. A default value of ... indicates that there is no default, and
    as such a value must be provided for this field in the Pydantic BaseModel or Dataclass representation. We cannot
    use None for this, as None is a valid default for optional types.
    """

    field_type: AnyValueType
    default_value: Optional[AnyValueType] = ...

    def __post_init__(self) -> None:
        """Validate the combination of default_value and field_type"""
        if self.default_value is None:
            assert _is_optional_type(self.field_type), (
                f"Invalid default of None provided for field definition - type {self.field_type} will not allow it! "
                f"Use ... or dataclasses.MISSING instead"
            )

    def as_pydantic_field_tuple(self) -> Tuple[Type, AnyValueType]:
        """Pydantic fields can be initialized with a Tuple[Type, Any], where the second parameter is the default value"""
        default = self.default_value if self.default_value != dataclasses.MISSING else ...
        return (self.annotated_field_type, default)

    @property
    def annotated_field_type(self) -> Type:  # noqa: D
        return self.field_type
